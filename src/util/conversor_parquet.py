import os
import logging
import re
import chardet
from pathlib import Path
from typing import Optional, List, Dict, Any
import polars as pl
from tqdm import tqdm
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing

# Importar funções de padronização de colunas
from .utilitarios import padronizar_colunas_dataframe
from .filtro_cnae import FiltroCNAE

# Configuração de logging
logger = logging.getLogger("conversor_parquet")

class ConversorParquet:
    def __init__(self, chunk_size: Optional[int] = None, max_workers: Optional[int] = None, campos_especificos: Optional[List[str]] = None, limpar_arquivos_descompactados: bool = False, filtrar_empregos_verdes: bool = False, arquivo_filtro_cnae: Optional[str] = None, nome_filtro_cnae: str = "CNAE", situacao_filtro_cnae: int = 1):
        """
        Inicializa o conversor de arquivos TXT para Parquet.
        
        Args:
            chunk_size: Número de linhas por chunk para processamento.
                      Se None, será calculado automaticamente para cada arquivo.
            max_workers: Número máximo de workers para processamento paralelo.
                       Se None, será detectado automaticamente.
            campos_especificos: Lista de campos específicos a serem incluídos na conversão.
                              Se None, todos os campos serão incluídos.
            limpar_arquivos_descompactados: Se True, apaga os arquivos TXT descompactados após a conversão.
            filtrar_empregos_verdes: Se True, filtra apenas empregos classificados como verdes.
            arquivo_filtro_cnae: Caminho para arquivo CSV com classificação CNAE personalizada.
            nome_filtro_cnae: Nome descritivo do filtro CNAE personalizado.
            situacao_filtro_cnae: Valor da coluna SITUACAO para filtrar no arquivo personalizado.
        """
        # Detectar número de workers automaticamente se não especificado
        if max_workers is None:
            self.max_workers = self._detectar_workers_otimos()
        else:
            self.max_workers = max_workers
        
        # Chunk size será calculado dinamicamente se não especificado
        self.chunk_size_fixo = chunk_size
        
        # Campos específicos para filtrar durante a conversão
        self.campos_especificos = [campo.upper() for campo in campos_especificos] if campos_especificos else None
        
        # Flag para controlar limpeza de arquivos descompactados
        self.limpar_arquivos_descompactados = limpar_arquivos_descompactados
        
        # Flag para controlar filtro de empregos verdes
        self.filtrar_empregos_verdes = filtrar_empregos_verdes
        
        # Parâmetros para filtro CNAE personalizado
        self.arquivo_filtro_cnae = arquivo_filtro_cnae
        self.nome_filtro_cnae = nome_filtro_cnae
        self.situacao_filtro_cnae = situacao_filtro_cnae
        
        # Inicializar filtros
        self.filtro_verdes = None
        self.filtro_cnae_personalizado = None
        
        # Inicializar filtro CNAE personalizado se especificado
        if self.arquivo_filtro_cnae:
            self.filtro_cnae_personalizado = FiltroCNAE(self.arquivo_filtro_cnae, self.nome_filtro_cnae)
            if self.filtro_cnae_personalizado.verificar_disponibilidade():
                estatisticas = self.filtro_cnae_personalizado.obter_estatisticas(self.situacao_filtro_cnae)
                logger.info(f"🎯 Filtro '{self.nome_filtro_cnae}' habilitado: {estatisticas['classes_filtradas']} classes ({estatisticas['percentual_filtrado']:.1f}%)")
            else:
                logger.warning(f"Filtro CNAE personalizado '{self.nome_filtro_cnae}' solicitado mas arquivo não disponível")
                self.arquivo_filtro_cnae = None
        
        # Inicializar filtro de empregos verdes se necessário (usando arquivo padrão)
        if self.filtrar_empregos_verdes:
            self.filtro_verdes = FiltroCNAE("db/cnae_classe_emprego_verde.csv", "Empregos Verdes")
            if self.filtro_verdes.verificar_disponibilidade():
                estatisticas = self.filtro_verdes.obter_estatisticas(1)
                logger.info(f"🌱 Filtro de empregos verdes habilitado: {estatisticas['classes_filtradas']} classes ({estatisticas['percentual_filtrado']:.1f}%)")
            else:
                logger.warning("Filtro de empregos verdes solicitado mas arquivo CNAE não disponível")
                self.filtrar_empregos_verdes = False
        
        # Criar diretório parquet se não existir
        Path("parquet").mkdir(exist_ok=True)
        
        # Lock para operações de criação de diretórios
        self._dir_lock = threading.Lock()
        
        if chunk_size is None:
            logger.info(f"Conversor configurado - Chunk size: automático, Max workers: {self.max_workers}")
        else:
            logger.info(f"Conversor configurado - Chunk size: {chunk_size}, Max workers: {self.max_workers}")
        
        if self.campos_especificos:
            logger.info(f"Conversor configurado - Campos específicos: {self.campos_especificos}")
        else:
            logger.info("Conversor configurado - Todos os campos serão incluídos")
        
        if self.limpar_arquivos_descompactados:
            logger.info("Conversor configurado - Arquivos TXT descompactados serão apagados após conversão")
        else:
            logger.info("Conversor configurado - Arquivos TXT descompactados serão mantidos após conversão")
        
        if self.filtrar_empregos_verdes:
            logger.info("🌱 Conversor configurado - Apenas empregos verdes serão incluídos")
        elif self.arquivo_filtro_cnae:
            logger.info(f"🎯 Conversor configurado - Apenas empregos da classificação '{self.nome_filtro_cnae}' serão incluídos")
        else:
            logger.info("📊 Conversor configurado - Todos os empregos serão incluídos")
    
    def _detectar_workers_otimos(self) -> int:
        """
        Detecta o número ótimo de workers baseado no hardware disponível.
        
        Returns:
            Número de workers recomendado
        """
        # Obter número de CPUs físicos e lógicos
        cpus_fisicos = multiprocessing.cpu_count()
        cpus_logicos = os.cpu_count() or multiprocessing.cpu_count()
        
        # Estratégia: usar 75% dos cores lógicos, mas não menos que 2 nem mais que 16
        workers_sugeridos = max(2, min(16, int(cpus_logicos * 0.75)))
        
        # Log das informações de detecção
        logger.info(f"Detecção automática de workers:")
        logger.info(f"  CPUs físicos: {cpus_fisicos}")
        logger.info(f"  CPUs lógicos: {cpus_logicos}")
        logger.info(f"  Workers sugeridos: {workers_sugeridos}")
        
        # Verificar se há limitações de memória (opcional)
        try:
            import psutil
            memoria_gb = psutil.virtual_memory().total / (1024**3)
            if memoria_gb < 8:  # Se menos de 8GB RAM
                workers_sugeridos = max(2, min(workers_sugeridos, 4))
                logger.info(f"  Memória limitada ({memoria_gb:.1f}GB), reduzindo workers para: {workers_sugeridos}")
        except ImportError:
            logger.debug("psutil não disponível, usando detecção padrão de workers")
        
        return workers_sugeridos
    
    def _obter_chunk_size_padrao(self, tamanho_arquivo_bytes: int) -> int:
        """
        Retorna o tamanho padrão de chunk baseado no tamanho do arquivo.
        Usa valores fixos baseados no tamanho do arquivo para balancear memória e performance.
        
        Args:
            tamanho_arquivo_bytes: Tamanho do arquivo em bytes
            
        Returns:
            Tamanho padrão de chunk em linhas
        """
        tamanho_arquivo_gb = tamanho_arquivo_bytes / (1024**3)
        
        # Valores fixos baseados no tamanho do arquivo
        if tamanho_arquivo_gb < 1:
            return 50000      # Arquivos pequenos
        elif tamanho_arquivo_gb < 5:
            return 100000     # Arquivos médios
        elif tamanho_arquivo_gb < 10:
            return 200000     # Arquivos grandes
        else:
            return 500000     # Arquivos muito grandes
    
    def _extrair_ano_arquivo(self, nome_arquivo: str) -> Optional[str]:
        """
        Extrai o ano do nome do arquivo ou do caminho completo.
        Retorna None se não encontrar um ano válido.
        """
        # Padrões comuns para arquivos RAIS
        padroes = [
            r"(20\d{2})",  # Ano no formato 20XX
            r"RAIS_(\d{4})",  # RAIS_2024
            r"(\d{4})_RAIS",  # 2024_RAIS
            r"RAIS(\d{4})",   # RAIS2024
        ]
        
        for padrao in padroes:
            match = re.search(padrao, nome_arquivo, re.IGNORECASE)
            if match:
                ano = match.group(1)
                # Validar se é um ano razoável (entre 1985 e 2030)
                if 1985 <= int(ano) <= 2030:
                    return ano
        
        return None
    
    def _extrair_ano_caminho(self, caminho_arquivo: Path) -> Optional[str]:
        """
        Extrai o ano do caminho completo do arquivo.
        Procura por pastas com nomes de ano no caminho.
        
        Args:
            caminho_arquivo: Caminho completo do arquivo
            
        Returns:
            Ano extraído ou None se não encontrado
        """
        # Converter para string e normalizar separadores
        caminho_str = str(caminho_arquivo).replace('\\', '/')
        
        # Procurar por padrões de ano no caminho
        padroes_ano = [
            r'/(\d{4})/',  # /2024/
            r'/(\d{4})$',  # /2024 (final do caminho)
            r'^(\d{4})/',  # 2024/ (início do caminho)
        ]
        
        for padrao in padroes_ano:
            match = re.search(padrao, caminho_str)
            if match:
                ano = match.group(1)
                # Validar se é um ano razoável (entre 1985 e 2030)
                if 1985 <= int(ano) <= 2030:
                    return ano
        
        return None
    
    def _detectar_encoding(self, caminho_arquivo: Path) -> str:
        """
        Detecta o encoding do arquivo TXT.
        
        Args:
            caminho_arquivo: Caminho do arquivo TXT
            
        Returns:
            Encoding detectado (padrão: 'latin1')
        """
        # Primeiro tentar usar chardet se disponível
        try:            
            # Ler uma amostra do arquivo para detectar encoding
            with open(caminho_arquivo, 'rb') as f:
                amostra = f.read(10000)  # Primeiros 10KB
            
            resultado = chardet.detect(amostra)
            encoding_detectado = resultado['encoding']
            confianca = resultado['confidence']
            
            if encoding_detectado and confianca > 0.5:
                # Testar se o encoding detectado funciona
                try:
                    with open(caminho_arquivo, 'r', encoding=encoding_detectado) as f:
                        # Tentar ler algumas linhas para testar o encoding
                        for i, _ in enumerate(f):
                            if i >= 5:  # Testar apenas as primeiras 5 linhas
                                break
                    logger.debug(f"Encoding detectado pelo chardet: '{encoding_detectado}' (confiança: {confianca:.2f}) para {caminho_arquivo}")
                    return encoding_detectado
                except UnicodeDecodeError:
                    logger.warning(f"Encoding detectado pelo chardet '{encoding_detectado}' falhou no teste, tentando outros...")
        except ImportError:
            logger.debug("chardet não disponível, usando método manual")
        except Exception as e:
            logger.warning(f"Erro ao usar chardet: {e}")
        
        # Fallback: testar encodings conhecidos
        encodings = ['latin1', 'iso-8859-1', 'utf-8', 'cp1252', 'windows-1252']
        
        for encoding in encodings:
            try:
                with open(caminho_arquivo, 'r', encoding=encoding) as f:
                    # Tentar ler algumas linhas para testar o encoding
                    for i, _ in enumerate(f):
                        if i >= 5:  # Testar apenas as primeiras 5 linhas
                            break
                logger.info(f"Encoding detectado manualmente: '{encoding}' para {caminho_arquivo}")
                return encoding
            except UnicodeDecodeError:
                continue
            except Exception:
                continue
        
        logger.warning(f"Não foi possível detectar encoding para {caminho_arquivo}, usando latin1")
        return 'latin1'  # Encoding padrão para arquivos brasileiros

    def _detectar_separador(self, caminho_arquivo: Path) -> str:
        """
        Detecta o separador do arquivo TXT analisando as primeiras linhas.
        
        Args:
            caminho_arquivo: Caminho do arquivo TXT
            
        Returns:
            Separador detectado (padrão: ';')
        """
        encoding = self._detectar_encoding(caminho_arquivo)
        
        try:
            with open(caminho_arquivo, 'r', encoding=encoding) as f:
                # Ler as primeiras linhas para detectar o separador
                linhas_teste = []
                for i, linha in enumerate(f):
                    if i >= 10:  # Analisar apenas as primeiras 10 linhas
                        break
                    linhas_teste.append(linha.strip())
                
                if not linhas_teste:
                    return ';'  # Separador padrão
                
                # Contar ocorrências de separadores comuns
                separadores = [';', ',', '\t', '|']
                contadores = {}
                
                for separador in separadores:
                    contadores[separador] = 0
                    for linha in linhas_teste:
                        if linha:
                            contadores[separador] += linha.count(separador)
                
                # Retornar o separador mais frequente
                separador_mais_frequente = max(contadores.items(), key=lambda x: x[1])[0]
                logger.debug(f"Separador detectado: '{separador_mais_frequente}' para {caminho_arquivo}")
                return separador_mais_frequente
                
        except Exception as e:
            logger.warning(f"Erro ao detectar separador para {caminho_arquivo}: {e}")
            return ';'  # Separador padrão
    
    def _mapear_campos_especificos(self, df_colunas: List[str]) -> Optional[List[str]]:
        """
        Mapeia os campos específicos para os nomes originais das colunas.
        
        Args:
            df_colunas: Lista com os nomes originais das colunas
            
        Returns:
            Lista com os nomes originais das colunas a serem incluídas, ou None se não há filtro
        """
        if not self.campos_especificos:
            return None
        
        # Padronizar colunas do DataFrame
        mapeamento_colunas = padronizar_colunas_dataframe(df_colunas)
        
        # Mapear campos específicos para nomes originais
        colunas_incluir = []
        campos_nao_encontrados = []
        
        for campo_especifico in self.campos_especificos:
            campo_encontrado = False
            
            # Procurar por correspondência exata ou parcial
            for nome_original, nome_padronizado in mapeamento_colunas.items():
                if campo_especifico == nome_padronizado or campo_especifico in nome_padronizado:
                    colunas_incluir.append(nome_original)
                    campo_encontrado = True
                    logger.debug(f"Campo '{campo_especifico}' mapeado para '{nome_original}'")
                    break
            
            if not campo_encontrado:
                campos_nao_encontrados.append(campo_especifico)
        
        if campos_nao_encontrados:
            logger.warning(f"Campos não encontrados: {campos_nao_encontrados}")
            logger.info(f"Colunas disponíveis: {list(mapeamento_colunas.values())}")
        
        if colunas_incluir:
            logger.info(f"Campos específicos mapeados: {colunas_incluir}")
            return colunas_incluir
        else:
            logger.error("Nenhum campo específico foi encontrado")
            return None
    
    def _contar_linhas_arquivo(self, caminho_arquivo: Path) -> int:
        """
        Conta o número total de linhas no arquivo.
        
        Args:
            caminho_arquivo: Caminho do arquivo
            
        Returns:
            Número total de linhas
        """
        encoding = self._detectar_encoding(caminho_arquivo)
        try:
            with open(caminho_arquivo, 'r', encoding=encoding) as f:
                return sum(1 for _ in f)
        except Exception as e:
            logger.error(f"Erro ao contar linhas do arquivo {caminho_arquivo}: {e}")
            return 0
    
    def _criar_schema_polars(self, caminho_arquivo: Path, separador: str, encoding: str) -> Optional[Dict[str, pl.DataType]]:
        """
        Cria o schema do Polars baseado no cabeçalho do arquivo.
        
        Args:
            caminho_arquivo: Caminho do arquivo TXT
            separador: Separador do arquivo
            encoding: Encoding do arquivo
            
        Returns:
            Schema do Polars ou None em caso de erro
        """
        try:
            # Tentar ler apenas o cabeçalho com configurações robustas
            df_cabecalho = pl.read_csv(
                caminho_arquivo,
                separator=separador,
                n_rows=1,
                encoding=encoding,
                ignore_errors=True,
                truncate_ragged_lines=True,
                try_parse_dates=False  # Evitar problemas com parsing de datas
            )
            
            if df_cabecalho.is_empty():
                logger.error(f"Arquivo vazio ou sem cabeçalho: {caminho_arquivo}")
                return None
            
            # Padronizar nomes das colunas
            mapeamento_colunas = padronizar_colunas_dataframe(df_cabecalho.columns)
            logger.info(f"Colunas padronizadas para {caminho_arquivo}: {len(mapeamento_colunas)} colunas processadas")
            
            # Criar schema com tipos de dados apropriados usando nomes originais
            # (o Polars precisa ler com os nomes originais)
            # Usar apenas Utf8 para evitar problemas com valores negativos
            schema = {}
            for coluna_original in df_cabecalho.columns:
                schema[coluna_original] = pl.Utf8  # Forçar todos os campos para texto
            
            # Armazenar mapeamento para uso posterior
            self._mapeamento_colunas = mapeamento_colunas
            
            logger.debug(f"Schema criado com {len(schema)} colunas para {caminho_arquivo}")
            return schema
            
        except Exception as e:
            logger.error(f"Erro ao criar schema para {caminho_arquivo}: {e}")
            # Tentar com encoding alternativo se o primeiro falhou
            if encoding != 'latin1':
                logger.info(f"Tentando novamente com encoding latin1 para {caminho_arquivo}")
                try:
                    df_cabecalho = pl.read_csv(
                        caminho_arquivo,
                        separator=separador,
                        n_rows=1,
                        encoding='latin1',
                        ignore_errors=True,
                        truncate_ragged_lines=True,
                        try_parse_dates=False
                    )
                    
                    if not df_cabecalho.is_empty():
                        # Padronizar nomes das colunas
                        mapeamento_colunas = padronizar_colunas_dataframe(df_cabecalho.columns)
                        logger.info(f"Colunas padronizadas (fallback latin1) para {caminho_arquivo}: {len(mapeamento_colunas)} colunas processadas")
                        
                        # Criar schema - usar apenas Utf8 para evitar problemas
                        schema = {}
                        for coluna_original in df_cabecalho.columns:
                            schema[coluna_original] = pl.Utf8  # Forçar todos os campos para texto
                        
                        # Armazenar mapeamento para uso posterior
                        self._mapeamento_colunas = mapeamento_colunas
                        
                        logger.info(f"Schema criado com fallback latin1 (todos Utf8): {len(schema)} colunas para {caminho_arquivo}")
                        return schema
                        
                except Exception as e2:
                    logger.error(f"Erro também com fallback latin1 para {caminho_arquivo}: {e2}")
            
            # Último recurso: tentar sem schema específico
            logger.warning(f"Tentando leitura sem schema específico para {caminho_arquivo}")
            try:
                df_cabecalho = pl.read_csv(
                    caminho_arquivo,
                    separator=separador,
                    n_rows=1,
                    encoding='latin1',
                    ignore_errors=True,
                    truncate_ragged_lines=True,
                    try_parse_dates=False
                )
                
                if not df_cabecalho.is_empty():
                    # Padronizar nomes das colunas
                    mapeamento_colunas = padronizar_colunas_dataframe(df_cabecalho.columns)
                    logger.info(f"Colunas padronizadas (sem schema): {len(mapeamento_colunas)} colunas processadas")
                    
                    # Armazenar mapeamento para uso posterior
                    self._mapeamento_colunas = mapeamento_colunas
                    
                    logger.info(f"Leitura sem schema bem-sucedida: {len(df_cabecalho.columns)} colunas para {caminho_arquivo}")
                    return {}  # Retornar schema vazio para usar inferência automática
                    
            except Exception as e3:
                logger.error(f"Erro também sem schema para {caminho_arquivo}: {e3}")
            
            return None
    
    def converter_arquivo_txt_para_parquet(self, caminho_arquivo_txt: Path, ano_especifico: Optional[int] = None) -> bool:
        """
        Converte um arquivo TXT para Parquet usando processamento em chunks.
        Cada chunk é salvo como um arquivo separado.
        
        Args:
            caminho_arquivo_txt: Caminho do arquivo TXT de entrada
            ano_especifico: Ano específico para usar (opcional)
            
        Returns:
            True se a conversão foi bem-sucedida, False caso contrário
        """
        if not caminho_arquivo_txt.exists():
            logger.error(f"Arquivo não encontrado: {caminho_arquivo_txt}")
            return False
        
        # Extrair informações do arquivo
        nome_arquivo = caminho_arquivo_txt.stem
        
        # Determinar o ano: usar o especificado, extrair do caminho ou do nome do arquivo
        if ano_especifico:
            ano = str(ano_especifico)
        else:
            # Primeiro tentar extrair do caminho completo
            ano = self._extrair_ano_caminho(caminho_arquivo_txt)
            if not ano:
                # Se não encontrar no caminho, tentar no nome do arquivo
                ano = self._extrair_ano_arquivo(nome_arquivo)
                if not ano:
                    ano = "desconhecido"
        
        # Definir diretório de destino: parquet/ANO/arquivo/
        diretorio_destino = Path("parquet").resolve() / ano / nome_arquivo
        
        # Criar diretório de destino se necessário
        with self._dir_lock:
            diretorio_destino.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Iniciando conversão de {caminho_arquivo_txt} para {diretorio_destino}")
        
        try:
            # Detectar separador
            separador = self._detectar_separador(caminho_arquivo_txt)
            
            # Contar total de linhas para a barra de progresso
            total_linhas = self._contar_linhas_arquivo(caminho_arquivo_txt)
            if total_linhas == 0:
                logger.error(f"Arquivo vazio: {caminho_arquivo_txt}")
                return False
            
            # Detectar encoding
            encoding = self._detectar_encoding(caminho_arquivo_txt)
            
            # Criar schema
            schema = self._criar_schema_polars(caminho_arquivo_txt, separador, encoding)
            if not schema:
                logger.error(f"Não foi possível criar schema para {caminho_arquivo_txt}")
                return False
            
            # Definir colunas a remover
            colunas_remover = [
                'Bairros SP',
                'Bairros Fortaleza',
                'Bairros RJ',
                'Distritos SP',
                'Regiões Adm DF'
            ]
            
            # Obter chunk size (fixo ou calculado)
            if self.chunk_size_fixo is None:
                chunk_size = self._obter_chunk_size_padrao(caminho_arquivo_txt.stat().st_size)
                logger.info(f"Chunk size calculado: {chunk_size:,} linhas")
            else:
                chunk_size = self.chunk_size_fixo
                logger.info(f"Chunk size fixo: {chunk_size:,} linhas")
            
            # Calcular número de chunks
            num_chunks = (total_linhas + chunk_size - 1) // chunk_size
            
            logger.info(f"Processando {total_linhas} linhas em {num_chunks} chunks com {self.max_workers} workers")
            
            # Ler arquivo completo uma vez e aplicar transformações
            logger.info("Lendo arquivo completo e aplicando transformações...")
            
            # Ler arquivo completo com configurações robustas
            # Forçar todos os campos para texto para evitar problemas de tipo
            df_completo = pl.read_csv(
                caminho_arquivo_txt,
                separator=separador,
                encoding=encoding,
                ignore_errors=True,
                truncate_ragged_lines=True,
                try_parse_dates=False,  # Evitar problemas com parsing de datas
                null_values=["", "NULL", "null", "None", "none"],  # Tratar valores nulos
                dtypes={col: pl.Utf8 for col in schema.keys()} if schema else None  # Forçar todos os campos para texto
            )
            
            # Aplicar mapeamento de colunas para renomear PRIMEIRO
            if hasattr(self, '_mapeamento_colunas') and self._mapeamento_colunas:
                # Criar mapeamento apenas com colunas existentes
                mapeamento_filtrado = {k: v for k, v in self._mapeamento_colunas.items() if k in df_completo.columns}
                
                if mapeamento_filtrado:
                    df_completo = df_completo.rename(mapeamento_filtrado)
                    logger.info(f"Colunas renomeadas com sucesso: {len(mapeamento_filtrado)} colunas")
                else:
                    logger.warning("Nenhuma coluna foi renomeada")
            
            # Remover colunas específicas que não são necessárias
            colunas_remover = [
                'BAIRROS_SP',
                'BAIRROS_FORTALEZA', 
                'BAIRROS_RJ',
                'DISTRITOS_SP',
                'REGIOES_ADM_DF'
            ]
            
            # Remover apenas as colunas que existem no DataFrame
            colunas_existentes_para_remover = [col for col in colunas_remover if col in df_completo.columns]
            if colunas_existentes_para_remover:
                df_completo = df_completo.drop(colunas_existentes_para_remover)
                logger.info(f"Colunas removidas: {colunas_existentes_para_remover}")
            
            # Filtrar apenas os campos específicos solicitados DEPOIS da renomeação
            if self.campos_especificos:
                # Verificar quais campos solicitados existem no DataFrame
                campos_disponiveis = [campo for campo in self.campos_especificos if campo in df_completo.columns]
                campos_nao_encontrados = [campo for campo in self.campos_especificos if campo not in df_completo.columns]
                
                if campos_nao_encontrados:
                    logger.warning(f"Campos não encontrados no arquivo: {campos_nao_encontrados}")
                
                if campos_disponiveis:
                    df_completo = df_completo.select(campos_disponiveis)
                    logger.info(f"Filtrando apenas os campos: {campos_disponiveis}")
                else:
                    logger.warning("Nenhum dos campos solicitados foi encontrado no arquivo")
            
            # Aplicar filtros ANTES da conversão para reduzir processamento
            if self.filtrar_empregos_verdes and self.filtro_verdes:
                logger.info("Aplicando filtro de empregos verdes...")
                df_completo = self.filtro_verdes.filtrar_dataframe(
                    df_completo, 
                    situacao_desejada=1
                )
            elif self.arquivo_filtro_cnae and self.filtro_cnae_personalizado:
                logger.info(f"Aplicando filtro CNAE personalizado '{self.nome_filtro_cnae}'...")
                df_completo = self.filtro_cnae_personalizado.filtrar_dataframe(
                    df_completo, 
                    situacao_desejada=self.situacao_filtro_cnae
                )
            
            # Adicionar coluna ANO
            df_completo = df_completo.with_columns([
                pl.lit(ano).alias('ANO')
            ])
            logger.info("Coluna ANO adicionada")
            
            chunks_processados = 0
            
            # Função para processar um chunk individual
            def processar_chunk(chunk_idx: int) -> bool:
                start_idx = chunk_idx * chunk_size
                end_idx = min(start_idx + chunk_size, df_completo.height)
                
                try:
                    # Extrair chunk do DataFrame já processado
                    df_chunk = df_completo.slice(start_idx, end_idx - start_idx)
                    
                    if df_chunk.height == 0:
                        logger.debug(f"Chunk {chunk_idx} vazio, pulando...")
                        return False
                    
                    # Forçar todos os campos para texto para evitar problemas de tipo
                    df_chunk_convertido = df_chunk.with_columns([
                        pl.col(col).cast(pl.Utf8, strict=False) for col in df_chunk.columns
                    ])
                    
                    # Salvar chunk como arquivo separado
                    nome_chunk = f"{nome_arquivo}_part{chunk_idx:04d}.parquet"
                    caminho_chunk = diretorio_destino / nome_chunk
                    
                    df_chunk_convertido.write_parquet(str(caminho_chunk), compression="snappy")
                    logger.debug(f"Chunk {chunk_idx + 1} salvo: {caminho_chunk} ({df_chunk.height} linhas)")
                    return True
                    
                except Exception as e:
                    logger.error(f"Erro ao processar chunk {chunk_idx} de {caminho_arquivo_txt}: {e}")
                    return False
            
            # Barra de progresso
            with tqdm(
                total=num_chunks,
                desc=f"Convertendo {nome_arquivo}",
                unit="chunk",
                position=0,
                leave=True
            ) as pbar:
                
                # Processar chunks em paralelo
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    # Submeter todos os chunks para processamento
                    future_to_chunk = {
                        executor.submit(processar_chunk, chunk_idx): chunk_idx 
                        for chunk_idx in range(num_chunks)
                    }
                    
                    # Processar resultados conforme completam
                    for future in as_completed(future_to_chunk):
                        chunk_idx = future_to_chunk[future]
                        try:
                            sucesso = future.result()
                            if sucesso:
                                chunks_processados += 1
                            pbar.update(1)
                        except Exception as e:
                            logger.error(f"Erro no chunk {chunk_idx}: {e}")
                            pbar.update(1)
            
            # Limpar atributos temporários
            if hasattr(self, '_mapeamento_colunas'):
                delattr(self, '_mapeamento_colunas')
            
            logger.info(f"Conversão concluída: {chunks_processados} chunks salvos em {diretorio_destino}")
            
            # Limpar arquivo descompactado se configurado
            if self.limpar_arquivos_descompactados:
                self._limpar_arquivo_descompactado(caminho_arquivo_txt)
            
            # Verificar se o diretório ficou vazio e removê-lo se necessário
            diretorio_arquivo = caminho_arquivo_txt.parent
            if self.limpar_arquivos_descompactados:
                self.limpar_diretorio_descompactado_vazio(diretorio_arquivo)
            
            return True
            
        except Exception as e:
            logger.error(f"Erro durante a conversão de {caminho_arquivo_txt}: {e}")
            
            # Limpar atributos temporários em caso de erro
            if hasattr(self, '_mapeamento_colunas'):
                delattr(self, '_mapeamento_colunas')
            
            return False
    
    def converter_diretorio(self, diretorio_origem: str = "files-unzip", ano: Optional[int] = None, consolidar: bool = False) -> Dict[str, Any]:
        """
        Converte todos os arquivos TXT de um diretório para Parquet.
        
        Args:
            diretorio_origem: Diretório com arquivos TXT (padrão: files-unzip)
            ano: Ano específico para converter (opcional)
            consolidar: Se True, consolida todos os arquivos do ano em um único arquivo RAIS_ANO.parquet
            
        Returns:
            Dicionário com estatísticas da conversão
        """
        diretorio = Path(diretorio_origem)
        if not diretorio.exists():
            logger.error(f"Diretório não encontrado: {diretorio}")
            return {"total": 0, "convertidos": 0, "falhas": 0}
        
        logger.info(f"Iniciando conversão de arquivos TXT em {diretorio}")
        
        # Encontrar arquivos TXT - filtrar por ano se especificado
        arquivos_txt = []
        if ano:
            # Se ano especificado, procurar apenas na pasta do ano
            ano_str = str(ano)
            diretorio_ano = diretorio / ano_str
            if diretorio_ano.exists():
                for arquivo in diretorio_ano.glob("*.txt"):
                    arquivos_txt.append(arquivo)
                logger.info(f"Procurando arquivos TXT do ano {ano} em {diretorio_ano}")
            else:
                logger.warning(f"Diretório do ano {ano} não encontrado: {diretorio_ano}")
        else:
            # Se não especificado, listar todos os arquivos .txt
            for arquivo in diretorio.glob("**/*.txt"):
                arquivos_txt.append(arquivo)
            logger.info("Procurando arquivos TXT em todos os anos")
        
        if not arquivos_txt:
            logger.info(f"Nenhum arquivo TXT encontrado em {diretorio}")
            return {"total": 0, "convertidos": 0, "falhas": 0}
        
        logger.info(f"Encontrados {len(arquivos_txt)} arquivos TXT para converter")
        
        total = len(arquivos_txt)
        convertidos = 0
        falhas = 0
        
        # Barra de progresso principal
        with tqdm(
            total=total,
            desc="Convertendo arquivos",
            unit="arquivo",
            position=0,
            leave=True
        ) as pbar_principal:
            
            for arquivo_txt in arquivos_txt:
                try:
                    sucesso = self.converter_arquivo_txt_para_parquet(arquivo_txt, ano)
                    if sucesso:
                        convertidos += 1
                    else:
                        falhas += 1
                except Exception as e:
                    logger.error(f"Erro ao converter {arquivo_txt}: {e}")
                    falhas += 1
                
                pbar_principal.update(1)
        
        resultado = {
            "total": total,
            "convertidos": convertidos,
            "falhas": falhas
        }
        
        logger.info(f"Conversão concluída: {convertidos} convertidos, {falhas} falhas")
        
        # Consolidar arquivos se houve conversões bem-sucedidas e a consolidação foi solicitada
        if convertidos > 0 and ano and consolidar:
            logger.info("Iniciando consolidação dos arquivos...")
            self.consolidar_arquivos_ano(ano)
            # Limpar arquivos chunk antigos na raiz
            self.limpar_chunks_antigos(ano)
        elif convertidos > 0 and ano and not consolidar:
            logger.info("Consolidação não solicitada. Arquivos mantidos em chunks separados.")
        
        # Limpar diretórios vazios se configurado
        if self.limpar_arquivos_descompactados and convertidos > 0:
            logger.info("Verificando diretórios vazios para limpeza...")
            self._limpar_diretorios_vazios(diretorio)
        
        return resultado

    def limpar_chunks_antigos(self, ano: int) -> None:
        """
        Remove arquivos antigos do tipo *_chunk_part*.parquet da raiz de parquet/ANO/.
        """
        diretorio_ano = Path("parquet") / str(ano)
        if not diretorio_ano.exists():
            return
        arquivos_antigos = list(diretorio_ano.glob("*_chunk_part*.parquet"))
        if arquivos_antigos:
            logger.info(f"Removendo {len(arquivos_antigos)} arquivos chunk antigos em {diretorio_ano}")
            for arquivo in arquivos_antigos:
                try:
                    arquivo.unlink()
                except Exception as e:
                    logger.error(f"Erro ao remover arquivo antigo {arquivo}: {e}")
        else:
            logger.info(f"Nenhum arquivo chunk antigo encontrado em {diretorio_ano}")
    
    def _limpar_arquivo_descompactado(self, caminho_arquivo_txt: Path) -> None:
        """
        Remove o arquivo TXT descompactado após a conversão bem-sucedida.
        
        Args:
            caminho_arquivo_txt: Caminho do arquivo TXT a ser removido
        """
        if not self.limpar_arquivos_descompactados:
            return
        
        try:
            if caminho_arquivo_txt.exists():
                # Calcular tamanho antes de remover para log
                tamanho_mb = caminho_arquivo_txt.stat().st_size / (1024 * 1024)
                
                # Remover arquivo
                caminho_arquivo_txt.unlink()
                
                logger.info(f"Arquivo descompactado removido: {caminho_arquivo_txt.name} ({tamanho_mb:.2f} MB)")
            else:
                logger.debug(f"Arquivo não encontrado para remoção: {caminho_arquivo_txt}")
                
        except Exception as e:
            logger.error(f"Erro ao remover arquivo descompactado {caminho_arquivo_txt}: {e}")
    
    def limpar_diretorio_descompactado_vazio(self, diretorio: Path) -> None:
        """
        Remove diretório descompactado se estiver vazio após a conversão.
        
        Args:
            diretorio: Caminho do diretório a ser verificado e removido se vazio
        """
        if not self.limpar_arquivos_descompactados:
            return
        
        try:
            if diretorio.exists() and diretorio.is_dir():
                # Verificar se o diretório está vazio
                arquivos_restantes = list(diretorio.glob("*"))
                if not arquivos_restantes:
                    # Remover diretório vazio
                    diretorio.rmdir()
                    logger.info(f"Diretório vazio removido: {diretorio}")
                else:
                    logger.debug(f"Diretório não está vazio, mantendo: {diretorio} ({len(arquivos_restantes)} arquivos restantes)")
                    
        except Exception as e:
            logger.error(f"Erro ao verificar/remover diretório {diretorio}: {e}")
    
    def _limpar_diretorios_vazios(self, diretorio_raiz: Path) -> None:
        """
        Remove diretórios vazios recursivamente após a conversão.
        
        Args:
            diretorio_raiz: Diretório raiz para verificar diretórios vazios
        """
        if not self.limpar_arquivos_descompactados:
            return
        
        try:
            diretorios_removidos = 0
            
            # Percorrer diretórios de forma recursiva, de baixo para cima
            for diretorio in sorted(diretorio_raiz.rglob("*"), key=lambda x: len(x.parts), reverse=True):
                if diretorio.is_dir():
                    # Verificar se o diretório está vazio
                    arquivos_restantes = list(diretorio.glob("*"))
                    if not arquivos_restantes:
                        try:
                            diretorio.rmdir()
                            diretorios_removidos += 1
                            logger.debug(f"Diretório vazio removido: {diretorio}")
                        except OSError:
                            # Diretório não está vazio ou não pode ser removido
                            pass
            
            if diretorios_removidos > 0:
                logger.info(f"Limpeza concluída: {diretorios_removidos} diretórios vazios removidos")
            else:
                logger.debug("Nenhum diretório vazio encontrado para remoção")
                
        except Exception as e:
            logger.error(f"Erro ao limpar diretórios vazios em {diretorio_raiz}: {e}")
    
    def consolidar_todos_anos(self, nome_arquivo_final: str = "RAIS_COMPLETO.parquet") -> Dict[str, Any]:
        """
        Consolida todos os arquivos RAIS_ANO.parquet em um único arquivo.
        
        Args:
            nome_arquivo_final: Nome do arquivo final consolidado
            
        Returns:
            Dicionário com estatísticas da consolidação
        """
        diretorio_parquet = Path("parquet")
        if not diretorio_parquet.exists():
            logger.error(f"Diretório parquet não encontrado: {diretorio_parquet}")
            return {"status": "erro", "erro": "Diretório parquet não encontrado"}
        
        logger.info(f"Iniciando consolidação de todos os anos em {nome_arquivo_final}")
        
        # Encontrar todos os arquivos RAIS_ANO.parquet
        arquivos_consolidados = []
        anos_encontrados = []
        
        for ano_dir in diretorio_parquet.iterdir():
            if ano_dir.is_dir():
                # Tentar extrair o ano do nome da pasta
                try:
                    ano = int(ano_dir.name)
                    if 1985 <= ano <= 2030:  # Ano válido
                        arquivo_ano = ano_dir / f"RAIS_{ano}.parquet"
                        if arquivo_ano.exists():
                            arquivos_consolidados.append(arquivo_ano)
                            anos_encontrados.append(ano)
                            logger.info(f"Encontrado arquivo consolidado: {arquivo_ano}")
                        else:
                            logger.warning(f"Arquivo consolidado não encontrado para ano {ano}: {arquivo_ano}")
                except ValueError:
                    logger.debug(f"Pasta não é um ano válido: {ano_dir.name}")
                    continue
        
        if not arquivos_consolidados:
            logger.error("Nenhum arquivo consolidado encontrado")
            return {"status": "erro", "erro": "Nenhum arquivo consolidado encontrado"}
        
        # Ordenar por ano
        arquivos_consolidados.sort(key=lambda x: int(x.parent.name))
        anos_encontrados.sort()
        
        logger.info(f"Encontrados {len(arquivos_consolidados)} arquivos consolidados dos anos: {anos_encontrados}")
        
        try:
            # Função para ler um arquivo consolidado
            def ler_arquivo_consolidado(arquivo: Path) -> Optional[pl.DataFrame]:
                try:
                    # Verificar se o arquivo tem tamanho mínimo
                    if arquivo.stat().st_size < 12:
                        logger.warning(f"Arquivo {arquivo} muito pequeno, pulando...")
                        return None
                    
                    df = pl.read_parquet(arquivo)
                    if df.height > 0:
                        logger.info(f"Lido arquivo {arquivo.name}: {df.height} linhas")
                        return df
                    else:
                        logger.warning(f"Arquivo {arquivo.name} está vazio")
                        return None
                except Exception as e:
                    logger.error(f"Erro ao ler arquivo {arquivo}: {e}")
                    return None
            
            # Ler todos os arquivos em paralelo
            dataframes = []
            total_linhas = 0
            
            with tqdm(
                total=len(arquivos_consolidados),
                desc="Lendo arquivos consolidados",
                unit="arquivo",
                position=0,
                leave=True
            ) as pbar_arquivos:
                
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    # Submeter todos os arquivos para leitura
                    future_to_arquivo = {
                        executor.submit(ler_arquivo_consolidado, arquivo): arquivo 
                        for arquivo in arquivos_consolidados
                    }
                    
                    # Processar resultados conforme completam
                    for future in as_completed(future_to_arquivo):
                        arquivo = future_to_arquivo[future]
                        try:
                            df = future.result()
                            if df is not None:
                                dataframes.append(df)
                                total_linhas += df.height
                            pbar_arquivos.update(1)
                        except Exception as e:
                            logger.error(f"Exceção ao ler arquivo {arquivo}: {e}")
                            pbar_arquivos.update(1)
            
            if not dataframes:
                logger.error("Nenhum arquivo foi lido com sucesso")
                return {"status": "erro", "erro": "Nenhum arquivo foi lido com sucesso"}
            
            # Concatenar todos os dataframes com tratamento de incompatibilidades de tipos
            logger.info(f"Concatenando {len(dataframes)} dataframes com {total_linhas} linhas totais")
            
            # Verificar e resolver incompatibilidades de tipos antes da concatenação
            if len(dataframes) > 1:
                df_final = self._concatenar_com_tratamento_tipos(dataframes)
            else:
                df_final = dataframes[0]
            
            # Salvar arquivo final consolidado
            arquivo_final = diretorio_parquet / nome_arquivo_final
            logger.info(f"Salvando arquivo final consolidado: {arquivo_final}")
            df_final.write_parquet(str(arquivo_final), compression="snappy")
            
            # Verificar se o arquivo foi criado
            if arquivo_final.exists():
                tamanho_mb = arquivo_final.stat().st_size / (1024 * 1024)
                tamanho_gb = tamanho_mb / 1024
                
                logger.info(f"Arquivo final consolidado criado: {arquivo_final}")
                logger.info(f"  Tamanho: {tamanho_gb:.2f} GB ({tamanho_mb:.2f} MB)")
                logger.info(f"  Linhas: {df_final.height:,}")
                logger.info(f"  Colunas: {len(df_final.columns)}")
                logger.info(f"  Anos incluídos: {anos_encontrados}")
                
                # Estatísticas por ano
                if 'ANO' in df_final.columns:
                    estatisticas_ano = df_final.group_by('ANO').count().sort('ANO')
                    logger.info("Estatísticas por ano:")
                    for row in estatisticas_ano.iter_rows():
                        ano, count = row
                        logger.info(f"  Ano {ano}: {count:,} linhas")
                
                return {
                    "status": "sucesso",
                    "arquivo_final": str(arquivo_final),
                    "tamanho_mb": tamanho_mb,
                    "tamanho_gb": tamanho_gb,
                    "total_linhas": df_final.height,
                    "total_colunas": len(df_final.columns),
                    "anos_incluidos": anos_encontrados,
                    "arquivos_processados": len(arquivos_consolidados)
                }
            else:
                logger.error(f"Arquivo final não foi criado: {arquivo_final}")
                return {"status": "erro", "erro": "Arquivo final não foi criado"}
                
        except Exception as e:
            logger.error(f"Erro na consolidação final: {e}")
            return {"status": "erro", "erro": str(e)}
    
    def _concatenar_com_tratamento_tipos(self, dataframes: List[pl.DataFrame]) -> pl.DataFrame:
        """
        Concatena dataframes tratando incompatibilidades de tipos automaticamente.
        
        Args:
            dataframes: Lista de dataframes para concatenar
            
        Returns:
            DataFrame concatenado com tipos compatíveis
        """
        if not dataframes:
            raise ValueError("Lista de dataframes vazia")
        
        if len(dataframes) == 1:
            return dataframes[0]
        
        logger.info("Analisando incompatibilidades de tipos entre dataframes...")
        
        # Obter todos os nomes de colunas únicos
        todas_colunas = set()
        for df in dataframes:
            todas_colunas.update(df.columns)
        
        # Analisar tipos de cada coluna em todos os dataframes
        tipos_por_coluna = {}
        for coluna in todas_colunas:
            tipos_por_coluna[coluna] = set()
            for df in dataframes:
                if coluna in df.columns:
                    tipos_por_coluna[coluna].add(str(df[coluna].dtype))
        
        # Identificar colunas com tipos incompatíveis
        colunas_incompativeis = {}
        for coluna, tipos in tipos_por_coluna.items():
            if len(tipos) > 1:
                colunas_incompativeis[coluna] = tipos
                logger.info(f"Coluna '{coluna}' tem tipos incompatíveis: {tipos}")
        
        # Converter tipos para compatibilidade
        dataframes_convertidos = []
        for i, df in enumerate(dataframes):
            df_convertido = df.clone()
            
            for coluna in todas_colunas:
                if coluna not in df_convertido.columns:
                    # Adicionar coluna ausente com valor padrão
                    if coluna in colunas_incompativeis:
                        # Usar string como tipo mais flexível para colunas problemáticas
                        df_convertido = df_convertido.with_columns(pl.lit(None).cast(pl.Utf8).alias(coluna))
                    else:
                        # Para colunas sem problemas, usar o tipo mais comum
                        tipos_mais_comuns = tipos_por_coluna[coluna]
                        if 'Int64' in tipos_mais_comuns:
                            df_convertido = df_convertido.with_columns(pl.lit(None).cast(pl.Int64).alias(coluna))
                        elif 'Float64' in tipos_mais_comuns:
                            df_convertido = df_convertido.with_columns(pl.lit(None).cast(pl.Float64).alias(coluna))
                        else:
                            df_convertido = df_convertido.with_columns(pl.lit(None).cast(pl.Utf8).alias(coluna))
                elif coluna in colunas_incompativeis:
                    # Converter para string para colunas com tipos incompatíveis
                    try:
                        df_convertido = df_convertido.with_columns(
                            df_convertido[coluna].cast(pl.Utf8).alias(coluna)
                        )
                    except Exception as e:
                        logger.warning(f"Erro ao converter coluna '{coluna}' para string no dataframe {i}: {e}")
                        # Se falhar, tentar converter para string com tratamento de erro
                        df_convertido = df_convertido.with_columns(
                            df_convertido[coluna].cast(pl.Utf8, strict=False).alias(coluna)
                        )
            
            dataframes_convertidos.append(df_convertido)
        
        # Agora concatenar os dataframes convertidos
        logger.info("Concatenando dataframes com tipos compatíveis...")
        try:
            df_final = pl.concat(dataframes_convertidos)
            logger.info(f"Concatenação bem-sucedida: {df_final.height} linhas, {len(df_final.columns)} colunas")
            return df_final
        except Exception as e:
            logger.error(f"Erro na concatenação mesmo após conversão de tipos: {e}")
            raise
    
    def consolidar_arquivos_ano(self, ano: int) -> None:
        """
        Consolida todos os arquivos de um ano em um único arquivo RAIS_ANO.parquet.
        Remove as subpastas após a consolidação.
        
        Args:
            ano: Ano dos arquivos a serem consolidados
        """
        diretorio_ano = Path("parquet") / str(ano)
        if not diretorio_ano.exists():
            logger.warning(f"Diretório do ano {ano} não encontrado: {diretorio_ano}")
            return
        
        logger.info(f"Consolidando todos os arquivos do ano {ano} em um único arquivo RAIS_{ano}.parquet")
        
        # Encontrar todas as subpastas (cada uma representa um arquivo)
        subpastas = [d for d in diretorio_ano.iterdir() if d.is_dir()]
        
        if not subpastas:
            logger.info(f"Nenhuma subpasta encontrada em {diretorio_ano}")
            return
        
        logger.info(f"Encontradas {len(subpastas)} subpastas para consolidar")
        
        # Coletar todos os chunks de todas as subpastas
        todos_chunks = []
        for subpasta in subpastas:
            chunks = sorted(subpasta.glob("*.parquet"))
            todos_chunks.extend(chunks)
            logger.info(f"Encontrados {len(chunks)} chunks em {subpasta.name}")
        
        if not todos_chunks:
            logger.warning(f"Nenhum chunk encontrado em nenhuma subpasta")
            return
        
        logger.info(f"Total de chunks a consolidar: {len(todos_chunks)}")
        
        try:
            # Função para ler um chunk individual
            def ler_chunk(chunk: Path) -> Optional[pl.DataFrame]:
                try:
                    # Verificar se o arquivo tem tamanho mínimo (12 bytes para header + footer)
                    if chunk.stat().st_size < 12:
                        logger.warning(f"Chunk {chunk} muito pequeno, pulando...")
                        return None
                    
                    df = pl.read_parquet(chunk)
                    if df.height > 0:  # Só retornar se tiver dados
                        return df
                    return None
                except Exception as e:
                    logger.error(f"Erro ao ler chunk {chunk}: {e}")
                    return None
            
            # Ler todos os chunks em paralelo
            dataframes = []
            
            with tqdm(
                total=len(todos_chunks),
                desc=f"Lendo todos os chunks do ano {ano}",
                unit="chunk",
                position=0,
                leave=True
            ) as pbar_chunks:
                
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    # Submeter todos os chunks para leitura
                    future_to_chunk = {
                        executor.submit(ler_chunk, chunk): chunk 
                        for chunk in todos_chunks
                    }
                    
                    # Processar resultados conforme completam
                    for future in as_completed(future_to_chunk):
                        chunk = future_to_chunk[future]
                        try:
                            df = future.result()
                            if df is not None:
                                dataframes.append(df)
                            pbar_chunks.update(1)
                        except Exception as e:
                            logger.error(f"Exceção ao ler chunk {chunk}: {e}")
                            pbar_chunks.update(1)
            
            if not dataframes:
                logger.error(f"Nenhum chunk foi lido com sucesso para o ano {ano}")
                return
            
            # Concatenar todos os dataframes
            logger.info(f"Concatenando {len(dataframes)} dataframes do ano {ano}")
            df_consolidado = pl.concat(dataframes)
            
            # Salvar arquivo consolidado único
            arquivo_consolidado = diretorio_ano / f"RAIS_{ano}.parquet"
            logger.info(f"Salvando arquivo consolidado único: {arquivo_consolidado}")
            df_consolidado.write_parquet(str(arquivo_consolidado), compression="snappy")
            
            # Verificar se o arquivo foi criado
            if arquivo_consolidado.exists():
                tamanho_mb = arquivo_consolidado.stat().st_size / (1024 * 1024)
                logger.info(f"Arquivo consolidado criado: {arquivo_consolidado} ({tamanho_mb:.2f} MB, {df_consolidado.height} linhas)")
                
                # Remover todas as subpastas com os chunks
                logger.info(f"Removendo {len(subpastas)} subpastas com chunks")
                import shutil
                for subpasta in subpastas:
                    try:
                        shutil.rmtree(subpasta)
                        logger.debug(f"Subpasta removida: {subpasta}")
                    except Exception as e:
                        logger.error(f"Erro ao remover subpasta {subpasta}: {e}")
                
                logger.info(f"Consolidação concluída com sucesso: {len(dataframes)} chunks consolidados em RAIS_{ano}.parquet")
            else:
                logger.error(f"Arquivo consolidado não foi criado: {arquivo_consolidado}")
                
        except Exception as e:
            logger.error(f"Erro na consolidação do ano {ano}: {e}")
    
 