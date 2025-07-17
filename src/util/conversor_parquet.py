import os
import logging
import re
from pathlib import Path
from typing import Optional, List, Dict, Any
import polars as pl
from tqdm import tqdm
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing

# Configuração de logging
logger = logging.getLogger("conversor_parquet")

class ConversorParquet:
    def __init__(self, chunk_size: Optional[int] = None, max_workers: Optional[int] = None):
        """
        Inicializa o conversor de arquivos TXT para Parquet.
        
        Args:
            chunk_size: Número de linhas por chunk para processamento.
                      Se None, será calculado automaticamente para cada arquivo.
            max_workers: Número máximo de workers para processamento paralelo.
                       Se None, será detectado automaticamente.
        """
        # Detectar número de workers automaticamente se não especificado
        if max_workers is None:
            self.max_workers = self._detectar_workers_otimos()
        else:
            self.max_workers = max_workers
        
        # Chunk size será calculado dinamicamente se não especificado
        self.chunk_size_fixo = chunk_size
        
        # Criar diretório parquet se não existir
        Path("parquet").mkdir(exist_ok=True)
        
        # Lock para operações de criação de diretórios
        self._dir_lock = threading.Lock()
        
        if chunk_size is None:
            logger.info(f"Conversor configurado - Chunk size: automático, Max workers: {self.max_workers}")
        else:
            logger.info(f"Conversor configurado - Chunk size: {chunk_size}, Max workers: {self.max_workers}")
    
    def _detectar_workers_otimos(self) -> int:
        """
        Detecta o número ótimo de workers baseado no hardware disponível.
        
        Returns:
            Número de workers recomendado
        """
        # Obter número de CPUs físicos e lógicos
        cpus_fisicos = multiprocessing.cpu_count()
        cpus_logicos = os.cpu_count()
        
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
        Usa valores fixos otimizados para arquivos temporários.
        
        Args:
            tamanho_arquivo_bytes: Tamanho do arquivo em bytes
            
        Returns:
            Tamanho padrão de chunk em linhas
        """
        tamanho_arquivo_gb = tamanho_arquivo_bytes / (1024**3)
        
        # Valores fixos otimizados para arquivos temporários
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
            
            chunks_processados = 0
            
            # Função para processar um chunk individual
            def processar_chunk(chunk_idx: int) -> bool:
                offset = chunk_idx * chunk_size
                
                try:
                    # Ler chunk
                    df_chunk = pl.read_csv(
                        caminho_arquivo_txt,
                        separator=separador,
                        skip_rows=offset + 1,  # +1 para pular cabeçalho
                        n_rows=chunk_size,
                        encoding=encoding,
                        ignore_errors=True,
                        schema=schema
                    )
                    
                    if df_chunk.height == 0:
                        logger.debug(f"Chunk {chunk_idx} vazio, pulando...")
                        return False
                    
                    # Salvar chunk como arquivo separado
                    nome_chunk = f"{nome_arquivo}_part{chunk_idx:04d}.parquet"
                    caminho_chunk = diretorio_destino / nome_chunk
                    
                    df_chunk.write_parquet(str(caminho_chunk), compression="snappy")
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
                            logger.error(f"Exceção no chunk {chunk_idx}: {e}")
                            pbar.update(1)
            
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
        
        # Consolidar arquivos se houve conversões bem-sucedidas
        if convertidos > 0 and ano:
            logger.info("Iniciando consolidação dos arquivos...")
            self.consolidar_arquivos_ano(ano)
            # Limpar arquivos chunk antigos na raiz
            self.limpar_chunks_antigos(ano)
        
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
    
    def consolidar_arquivos_ano(self, ano: int) -> None:
        """
        Consolida todos os chunks de cada arquivo em um único arquivo Parquet.
        Remove as subpastas após a consolidação.
        
        Args:
            ano: Ano dos arquivos a serem consolidados
        """
        diretorio_ano = Path("parquet") / str(ano)
        if not diretorio_ano.exists():
            logger.warning(f"Diretório do ano {ano} não encontrado: {diretorio_ano}")
            return
        
        logger.info(f"Consolidando arquivos do ano {ano}")
        
        # Encontrar todas as subpastas (cada uma representa um arquivo)
        subpastas = [d for d in diretorio_ano.iterdir() if d.is_dir()]
        
        if not subpastas:
            logger.info(f"Nenhuma subpasta encontrada em {diretorio_ano}")
            return
        
        logger.info(f"Encontradas {len(subpastas)} subpastas para consolidar")
        
        consolidados = 0
        falhas = 0
        
        # Barra de progresso para consolidação
        with tqdm(
            total=len(subpastas),
            desc="Consolidando arquivos",
            unit="arquivo",
            position=0,
            leave=True
        ) as pbar:
            
            # Consolidar arquivos em paralelo
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submeter todas as consolidações
                future_to_subpasta = {
                    executor.submit(self.consolidar_arquivo, subpasta, diretorio_ano): subpasta 
                    for subpasta in subpastas
                }
                
                # Processar resultados conforme completam
                for future in as_completed(future_to_subpasta):
                    subpasta = future_to_subpasta[future]
                    try:
                        sucesso = future.result()
                        if sucesso:
                            consolidados += 1
                        else:
                            falhas += 1
                    except Exception as e:
                        logger.error(f"Erro ao consolidar {subpasta}: {e}")
                        falhas += 1
                    
                    pbar.update(1)
        
        logger.info(f"Consolidação concluída: {consolidados} consolidados, {falhas} falhas")
    
    def consolidar_arquivo(self, subpasta: Path, diretorio_ano: Path) -> bool:
        """
        Consolida todos os chunks de um arquivo em um único arquivo Parquet.
        Remove a subpasta após a consolidação.
        
        Args:
            subpasta: Caminho da subpasta com os chunks
            diretorio_ano: Diretório do ano (pasta pai)
            
        Returns:
            True se a consolidação foi bem-sucedida, False caso contrário
        """
        nome_arquivo = subpasta.name
        
        # Encontrar todos os chunks da subpasta
        chunks = sorted(subpasta.glob("*.parquet"))
        
        if not chunks:
            logger.warning(f"Nenhum chunk encontrado em {subpasta}")
            return False
        
        logger.info(f"Consolidando {len(chunks)} chunks de {nome_arquivo}")
        
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
            
            # Ler chunks em paralelo
            dataframes = []
            
            with tqdm(
                total=len(chunks),
                desc=f"Lendo chunks de {nome_arquivo}",
                unit="chunk",
                position=1,
                leave=False
            ) as pbar_chunks:
                
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    # Submeter todos os chunks para leitura
                    future_to_chunk = {
                        executor.submit(ler_chunk, chunk): chunk 
                        for chunk in chunks
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
                logger.error(f"Nenhum chunk foi lido com sucesso para {nome_arquivo}")
                return False
            
            # Concatenar todos os dataframes
            logger.info(f"Concatenando {len(dataframes)} dataframes de {nome_arquivo}")
            df_consolidado = pl.concat(dataframes)
            
            # Salvar arquivo consolidado
            arquivo_consolidado = diretorio_ano / f"{nome_arquivo}.parquet"
            logger.info(f"Salvando arquivo consolidado: {arquivo_consolidado}")
            df_consolidado.write_parquet(str(arquivo_consolidado), compression="snappy")
            
            # Verificar se o arquivo foi criado
            if arquivo_consolidado.exists():
                tamanho_mb = arquivo_consolidado.stat().st_size / (1024 * 1024)
                logger.info(f"Arquivo consolidado criado: {arquivo_consolidado} ({tamanho_mb:.2f} MB, {df_consolidado.height} linhas)")
                
                # Remover subpasta com os chunks
                logger.info(f"Removendo subpasta: {subpasta}")
                import shutil
                shutil.rmtree(subpasta)
                
                return True
            else:
                logger.error(f"Arquivo consolidado não foi criado: {arquivo_consolidado}")
                return False
                
        except Exception as e:
            logger.error(f"Erro na consolidação de {nome_arquivo}: {e}")
            return False 