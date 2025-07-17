import os
import logging
import re
from pathlib import Path
from typing import Optional, List, Dict, Any
import polars as pl
from tqdm import tqdm
import threading

# Configuração de logging
logger = logging.getLogger("conversor_parquet")

class ConversorParquet:
    def __init__(self, chunk_size: int = 100000, max_workers: int = 4):
        """
        Inicializa o conversor de arquivos TXT para Parquet.
        
        Args:
            chunk_size: Número de linhas por chunk para processamento
            max_workers: Número máximo de workers para processamento paralelo
        """
        self.chunk_size = chunk_size
        self.max_workers = max_workers
        
        # Criar diretório parquet se não existir
        Path("parquet").mkdir(exist_ok=True)
        
        # Lock para operações de criação de diretórios
        self._dir_lock = threading.Lock()
        
        logger.info(f"Conversor configurado - Chunk size: {chunk_size}, Max workers: {max_workers}")
    
    def _extrair_ano_arquivo(self, nome_arquivo: str) -> Optional[str]:
        """
        Extrai o ano do nome do arquivo.
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
    
    def _detectar_encoding(self, caminho_arquivo: Path) -> str:
        """
        Detecta o encoding do arquivo TXT.
        
        Args:
            caminho_arquivo: Caminho do arquivo TXT
            
        Returns:
            Encoding detectado (padrão: 'latin1')
        """
        encodings = ['latin1', 'iso-8859-1', 'utf-8', 'cp1252']
        
        for encoding in encodings:
            try:
                with open(caminho_arquivo, 'r', encoding=encoding) as f:
                    # Tentar ler algumas linhas para testar o encoding
                    for i, _ in enumerate(f):
                        if i >= 5:  # Testar apenas as primeiras 5 linhas
                            break
                logger.debug(f"Encoding detectado: '{encoding}' para {caminho_arquivo}")
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
                separador_mais_frequente = max(contadores, key=contadores.get)
                logger.debug(f"Separador detectado: '{separador_mais_frequente}' para {caminho_arquivo}")
                return separador_mais_frequente
                
        except Exception as e:
            logger.warning(f"Erro ao detectar separador para {caminho_arquivo}: {e}")
            return ';'  # Separador padrão
    
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
            # Ler apenas o cabeçalho
            df_cabecalho = pl.read_csv(
                caminho_arquivo,
                separator=separador,
                n_rows=1,
                encoding=encoding,
                ignore_errors=True
            )
            
            # Criar schema com tipos de dados apropriados
            schema = {}
            for coluna in df_cabecalho.columns:
                # Para arquivos RAIS, usar tipos apropriados
                if any(palavra in coluna.upper() for palavra in ['ANO', 'MES', 'DIA', 'ID']):
                    schema[coluna] = pl.Int64
                elif any(palavra in coluna.upper() for palavra in ['VALOR', 'SALARIO', 'REMUNERACAO']):
                    schema[coluna] = pl.Float64
                else:
                    schema[coluna] = pl.Utf8
            
            logger.debug(f"Schema criado com {len(schema)} colunas para {caminho_arquivo}")
            return schema
            
        except Exception as e:
            logger.error(f"Erro ao criar schema para {caminho_arquivo}: {e}")
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
        
        # Determinar o ano: usar o especificado ou extrair do nome do arquivo
        if ano_especifico:
            ano = str(ano_especifico)
        else:
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
            
            # Calcular número de chunks
            num_chunks = (total_linhas + self.chunk_size - 1) // self.chunk_size
            
            logger.info(f"Processando {total_linhas} linhas em {num_chunks} chunks")
            
            chunks_processados = 0
            
            # Barra de progresso
            with tqdm(
                total=num_chunks,
                desc=f"Convertendo {nome_arquivo}",
                unit="chunk",
                position=0,
                leave=True
            ) as pbar:
                
                # Processar em chunks
                for chunk_idx in range(num_chunks):
                    offset = chunk_idx * self.chunk_size
                    
                    try:
                        # Ler chunk
                        df_chunk = pl.read_csv(
                            caminho_arquivo_txt,
                            separator=separador,
                            skip_rows=offset + 1,  # +1 para pular cabeçalho
                            n_rows=self.chunk_size,
                            encoding=encoding,
                            ignore_errors=True,
                            schema=schema
                        )
                        
                        if df_chunk.height == 0:
                            logger.debug(f"Chunk {chunk_idx} vazio, pulando...")
                            pbar.update(1)
                            continue
                        
                        # Salvar chunk como arquivo separado
                        nome_chunk = f"{nome_arquivo}_part{chunk_idx:04d}.parquet"
                        caminho_chunk = diretorio_destino / nome_chunk
                        
                        df_chunk.write_parquet(str(caminho_chunk), compression="snappy")
                        chunks_processados += 1
                        
                        logger.debug(f"Chunk {chunk_idx + 1} salvo: {caminho_chunk} ({df_chunk.height} linhas)")
                        pbar.update(1)
                        
                    except Exception as e:
                        logger.error(f"Erro ao processar chunk {chunk_idx} de {caminho_arquivo_txt}: {e}")
                        pbar.update(1)
                        continue
            
            # Verificar se pelo menos um chunk foi processado
            if chunks_processados > 0:
                logger.info(f"Conversão concluída: {chunks_processados} chunks salvos em {diretorio_destino}")
                return True
            else:
                logger.error("Nenhum chunk foi processado com sucesso")
                return False
                
        except Exception as e:
            logger.error(f"Erro na conversão de {caminho_arquivo_txt}: {e}")
            return False
    
    def converter_diretorio(self, diretorio_origem: str = "files-unzip", ano: Optional[int] = None) -> Dict[str, Any]:
        """
        Converte todos os arquivos TXT de um diretório para Parquet.
        
        Args:
            diretorio_origem: Diretório com arquivos TXT (padrão: files-unzip)
            ano: Ano específico para converter (opcional)
            
        Returns:
            Dicionário com estatísticas da conversão
        """
        diretorio = Path(diretorio_origem)
        if not diretorio.exists():
            logger.error(f"Diretório não encontrado: {diretorio}")
            return {"total": 0, "convertidos": 0, "falhas": 0}
        
        logger.info(f"Iniciando conversão de arquivos TXT em {diretorio}")
        
        # Encontrar arquivos TXT - sempre listar todos os arquivos .txt
        arquivos_txt = []
        for arquivo in diretorio.glob("**/*.txt"):
            arquivos_txt.append(arquivo)
        
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
        return resultado 