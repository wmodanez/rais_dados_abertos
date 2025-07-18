import logging
import threading
import time
from pathlib import Path
from typing import List, Dict, Any, Optional
from queue import Queue
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

from .gerenciador_ftp import GerenciadorArquivosFTP
from .descompactador import DescompactadorArquivos
from .conversor_parquet import ConversorParquet

logger = logging.getLogger("pipeline_paralelo")


class PipelineParalelo:
    """
    Gerencia um pipeline paralelo para processamento de dados RAIS.
    Permite que download, descompactação e conversão sejam executados simultaneamente.
    """
    
    def __init__(self, max_workers: Optional[int] = None, chunk_size: Optional[int] = None):
        """
        Inicializa o pipeline paralelo.
        
        Args:
            max_workers: Número máximo de workers para processamento paralelo
            chunk_size: Tamanho do chunk para conversão
        """
        self.max_workers = max_workers or 4
        self.chunk_size = chunk_size
        
        # Filas para comunicação entre etapas
        self.fila_download = Queue()
        self.fila_descompactacao = Queue()
        self.fila_conversao = Queue()
        
        # Contadores para estatísticas
        self.stats = {
            'download': {'total': 0, 'concluidos': 0, 'falhas': 0},
            'descompactacao': {'total': 0, 'concluidos': 0, 'falhas': 0},
            'conversao': {'total': 0, 'concluidos': 0, 'falhas': 0}
        }
        
        # Flags de controle
        self.download_finalizado = False
        self.descompactacao_finalizada = False
        self.conversao_finalizada = False
        
        # Lock para sincronização
        self.lock = threading.Lock()
    
    def processar_completo(self, 
                          servidor_ftp: str,
                          diretorio_remoto: str,
                          ano: Optional[int] = None,
                          ano_inicio: Optional[int] = None,
                          ano_fim: Optional[int] = None,
                          max_tentativas: int = 5,
                          tempo_espera: int = 10,
                          consolidar: bool = False) -> Dict[str, Any]:
        """
        Executa o pipeline completo: download -> descompactação -> conversão.
        
        Args:
            servidor_ftp: Servidor FTP
            diretorio_remoto: Diretório remoto no servidor
            ano: Ano específico
            ano_inicio: Ano inicial da faixa
            ano_fim: Ano final da faixa
            max_tentativas: Máximo de tentativas de download
            tempo_espera: Tempo de espera entre tentativas
            consolidar: Se deve consolidar os arquivos
            
        Returns:
            Dicionário com estatísticas do processamento
        """
        logger.info("Iniciando pipeline paralelo de processamento RAIS")
        
        # Inicializar componentes
        gerenciador = GerenciadorArquivosFTP(
            servidor_ftp=servidor_ftp,
            diretorio_remoto=diretorio_remoto,
            max_tentativas=max_tentativas,
            tempo_espera=tempo_espera,
            max_workers=self.max_workers
        )
        
        descompactador = DescompactadorArquivos(max_workers=self.max_workers)
        conversor = ConversorParquet(chunk_size=self.chunk_size, max_workers=self.max_workers)
        
        # Iniciar workers em threads separadas
        with ThreadPoolExecutor(max_workers=3) as executor:
            # Worker de download
            future_download = executor.submit(
                self._worker_download,
                gerenciador, ano, ano_inicio, ano_fim
            )
            
            # Worker de descompactação
            future_descompactacao = executor.submit(
                self._worker_descompactacao,
                descompactador, ano, ano_inicio, ano_fim
            )
            
            # Worker de conversão
            future_conversao = executor.submit(
                self._worker_conversao,
                conversor, ano, consolidar
            )
            
            # Aguardar conclusão de todas as etapas
            future_download.result()
            future_descompactacao.result()
            future_conversao.result()
        
        # Executar consolidação se solicitada
        if consolidar and ano:
            logger.info("Iniciando consolidação final...")
            conversor.consolidar_arquivos_ano(ano)
            conversor.limpar_chunks_antigos(ano)
        
        return self.stats
    
    def processar_sem_download(self, 
                              ano: Optional[int] = None,
                              ano_inicio: Optional[int] = None,
                              ano_fim: Optional[int] = None,
                              consolidar: bool = False) -> Dict[str, Any]:
        """
        Executa o pipeline sem download: descompactação -> conversão.
        
        Args:
            ano: Ano específico
            ano_inicio: Ano inicial da faixa
            ano_fim: Ano final da faixa
            consolidar: Se deve consolidar os arquivos
            
        Returns:
            Dicionário com estatísticas do processamento
        """
        logger.info("Iniciando pipeline paralelo sem download")
        
        # Inicializar componentes
        descompactador = DescompactadorArquivos(max_workers=self.max_workers)
        conversor = ConversorParquet(chunk_size=self.chunk_size, max_workers=self.max_workers)
        
        # Marcar download como já finalizado (não há download)
        self.download_finalizado = True
        
        # Primeiro, descompactar todos os arquivos
        logger.info("Iniciando descompactação de arquivos locais...")
        total_descompactar, descompactados, falhas_descompactar = descompactador.descompactar_arquivos_paralelo(
            ano=ano,
            ano_inicio=ano_inicio,
            ano_fim=ano_fim
        )
        
        # Atualizar estatísticas de descompactação
        self.stats['descompactacao']['total'] = total_descompactar
        self.stats['descompactacao']['concluidos'] = descompactados
        self.stats['descompactacao']['falhas'] = falhas_descompactar
        
        # Marcar descompactação como finalizada
        self.descompactacao_finalizada = True
        
        # Depois, converter todos os arquivos
        logger.info("Iniciando conversão de arquivos descompactados...")
        resultado_conversao = conversor.converter_diretorio("files-unzip", ano=ano, consolidar=consolidar)
        
        # Atualizar estatísticas de conversão
        self.stats['conversao']['total'] = resultado_conversao['total']
        self.stats['conversao']['concluidos'] = resultado_conversao['convertidos']
        self.stats['conversao']['falhas'] = resultado_conversao['falhas']
        
        # Marcar conversão como finalizada
        self.conversao_finalizada = True
        
        logger.info("Pipeline sem download concluído")
        return self.stats
    
    def processar_apenas_download(self, 
                                 servidor_ftp: str,
                                 diretorio_remoto: str,
                                 ano: Optional[int] = None,
                                 ano_inicio: Optional[int] = None,
                                 ano_fim: Optional[int] = None,
                                 max_tentativas: int = 5,
                                 tempo_espera: int = 10) -> Dict[str, Any]:
        """
        Executa apenas o download de arquivos.
        
        Args:
            servidor_ftp: Servidor FTP
            diretorio_remoto: Diretório remoto no servidor
            ano: Ano específico
            ano_inicio: Ano inicial da faixa
            ano_fim: Ano final da faixa
            max_tentativas: Máximo de tentativas de download
            tempo_espera: Tempo de espera entre tentativas
            
        Returns:
            Dicionário com estatísticas do download
        """
        logger.info("Iniciando pipeline de download")
        
        # Inicializar gerenciador
        gerenciador = GerenciadorArquivosFTP(
            servidor_ftp=servidor_ftp,
            diretorio_remoto=diretorio_remoto,
            max_tentativas=max_tentativas,
            tempo_espera=tempo_espera,
            max_workers=self.max_workers
        )
        
        # Executar download
        total, baixados, falhas = gerenciador.sincronizar_arquivos(
            ano=ano,
            ano_inicio=ano_inicio,
            ano_fim=ano_fim
        )
        
        # Atualizar estatísticas
        self.stats['download']['total'] = total
        self.stats['download']['concluidos'] = baixados
        self.stats['download']['falhas'] = falhas
        
        logger.info("Pipeline de download concluído")
        return self.stats
    
    def processar_download_descompactacao(self, 
                                         servidor_ftp: str,
                                         diretorio_remoto: str,
                                         ano: Optional[int] = None,
                                         ano_inicio: Optional[int] = None,
                                         ano_fim: Optional[int] = None,
                                         max_tentativas: int = 5,
                                         tempo_espera: int = 10) -> Dict[str, Any]:
        """
        Executa download e descompactação sequencial.
        
        Args:
            servidor_ftp: Servidor FTP
            diretorio_remoto: Diretório remoto no servidor
            ano: Ano específico
            ano_inicio: Ano inicial da faixa
            ano_fim: Ano final da faixa
            max_tentativas: Máximo de tentativas de download
            tempo_espera: Tempo de espera entre tentativas
            
        Returns:
            Dicionário com estatísticas do processamento
        """
        logger.info("Iniciando pipeline de download e descompactação")
        
        # Inicializar componentes
        gerenciador = GerenciadorArquivosFTP(
            servidor_ftp=servidor_ftp,
            diretorio_remoto=diretorio_remoto,
            max_tentativas=max_tentativas,
            tempo_espera=tempo_espera,
            max_workers=self.max_workers
        )
        
        descompactador = DescompactadorArquivos(max_workers=self.max_workers)
        
        # Executar download
        total_download, baixados, falhas_download = gerenciador.sincronizar_arquivos(
            ano=ano,
            ano_inicio=ano_inicio,
            ano_fim=ano_fim
        )
        
        # Atualizar estatísticas de download
        self.stats['download']['total'] = total_download
        self.stats['download']['concluidos'] = baixados
        self.stats['download']['falhas'] = falhas_download
        
        # Executar descompactação
        total_descompactar, descompactados, falhas_descompactar = descompactador.descompactar_arquivos_paralelo(
            ano=ano,
            ano_inicio=ano_inicio,
            ano_fim=ano_fim
        )
        
        # Atualizar estatísticas de descompactação
        self.stats['descompactacao']['total'] = total_descompactar
        self.stats['descompactacao']['concluidos'] = descompactados
        self.stats['descompactacao']['falhas'] = falhas_descompactar
        
        logger.info("Pipeline de download e descompactação concluído")
        return self.stats
    
    def processar_apenas_descompactacao(self, 
                                       ano: Optional[int] = None,
                                       ano_inicio: Optional[int] = None,
                                       ano_fim: Optional[int] = None) -> Dict[str, Any]:
        """
        Executa apenas a descompactação de arquivos locais.
        
        Args:
            ano: Ano específico
            ano_inicio: Ano inicial da faixa
            ano_fim: Ano final da faixa
            
        Returns:
            Dicionário com estatísticas da descompactação
        """
        logger.info("Iniciando pipeline de apenas descompactação")
        
        # Inicializar descompactador
        descompactador = DescompactadorArquivos(max_workers=self.max_workers)
        
        # Executar descompactação
        total_descompactar, descompactados, falhas_descompactar = descompactador.descompactar_arquivos_paralelo(
            ano=ano,
            ano_inicio=ano_inicio,
            ano_fim=ano_fim
        )
        
        # Atualizar estatísticas de descompactação
        self.stats['descompactacao']['total'] = total_descompactar
        self.stats['descompactacao']['concluidos'] = descompactados
        self.stats['descompactacao']['falhas'] = falhas_descompactar
        
        logger.info("Pipeline de apenas descompactação concluído")
        return self.stats
    
    def processar_apenas_conversao(self, 
                                  ano: Optional[int] = None,
                                  consolidar: bool = False) -> Dict[str, Any]:
        """
        Executa apenas a conversão de arquivos descompactados.
        
        Args:
            ano: Ano específico
            consolidar: Se deve consolidar os arquivos
            
        Returns:
            Dicionário com estatísticas da conversão
        """
        logger.info("Iniciando pipeline de apenas conversão")
        
        # Inicializar conversor
        conversor = ConversorParquet(chunk_size=self.chunk_size, max_workers=self.max_workers)
        
        # Executar conversão
        resultado_conversao = conversor.converter_diretorio("files-unzip", ano=ano, consolidar=consolidar)
        
        # Atualizar estatísticas de conversão
        self.stats['conversao']['total'] = resultado_conversao['total']
        self.stats['conversao']['concluidos'] = resultado_conversao['convertidos']
        self.stats['conversao']['falhas'] = resultado_conversao['falhas']
        
        logger.info("Pipeline de apenas conversão concluído")
        return self.stats
    
    def consolidar_arquivos(self, ano: int) -> Dict[str, Any]:
        """
        Consolida arquivos convertidos de um ano específico.
        
        Args:
            ano: Ano dos arquivos a serem consolidados
            
        Returns:
            Dicionário com estatísticas da consolidação
        """
        logger.info(f"Iniciando consolidação de arquivos do ano {ano}")
        
        # Inicializar conversor para usar seus métodos de consolidação
        conversor = ConversorParquet(chunk_size=self.chunk_size, max_workers=self.max_workers)
        
        try:
            # Executar consolidação
            conversor.consolidar_arquivos_ano(ano)
            
            # Limpar chunks antigos
            conversor.limpar_chunks_antigos(ano)
            
            logger.info(f"Consolidação do ano {ano} concluída com sucesso")
            
            # Retornar estatísticas da consolidação
            return {
                'ano': ano,
                'status': 'sucesso',
                'arquivo_consolidado': f"RAIS_{ano}.parquet"
            }
            
        except Exception as e:
            logger.error(f"Erro durante a consolidação do ano {ano}: {e}")
            return {
                'ano': ano,
                'status': 'erro',
                'erro': str(e)
            }
    
    def _worker_download(self, gerenciador: GerenciadorArquivosFTP,
                        ano: Optional[int], ano_inicio: Optional[int], ano_fim: Optional[int]):
        """Worker responsável pelo download de arquivos."""
        logger.info("Worker de download iniciado")
        
        try:
            # Listar arquivos disponíveis
            arquivos_disponiveis = gerenciador.listar_arquivos_remotos(ano, ano_inicio, ano_fim)
            
            # Extrair apenas os nomes dos arquivos
            nomes_arquivos = [arq["nome"] for arq in arquivos_disponiveis]
            
            with self.lock:
                self.stats['download']['total'] = len(nomes_arquivos)
            
            logger.info(f"Encontrados {len(nomes_arquivos)} arquivos para download")
            
            # Processar downloads em paralelo
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = []
                
                for nome_arquivo in nomes_arquivos:
                    future = executor.submit(self._download_arquivo, gerenciador, nome_arquivo)
                    futures.append(future)
                
                # Processar resultados conforme completam
                for future in as_completed(futures):
                    try:
                        sucesso, nome_arquivo = future.result()
                        if sucesso:
                            with self.lock:
                                self.stats['download']['concluidos'] += 1
                            # Adicionar à fila de descompactação
                            self.fila_descompactacao.put(nome_arquivo)
                            logger.debug(f"Download concluído: {nome_arquivo}")
                        else:
                            with self.lock:
                                self.stats['download']['falhas'] += 1
                            logger.error(f"Falha no download: {nome_arquivo}")
                    except Exception as e:
                        with self.lock:
                            self.stats['download']['falhas'] += 1
                        logger.error(f"Exceção no download: {e}")
            
            # Marcar download como finalizado
            self.download_finalizado = True
            logger.info("Worker de download finalizado")
            
        except Exception as e:
            logger.error(f"Erro no worker de download: {e}")
            self.download_finalizado = True
    
    def _worker_descompactacao(self, descompactador: DescompactadorArquivos,
                              ano: Optional[int], ano_inicio: Optional[int], ano_fim: Optional[int]):
        """Worker responsável pela descompactação de arquivos."""
        logger.info("Worker de descompactação iniciado")
        
        try:
            # Aguardar download finalizar
            while not self.download_finalizado:
                time.sleep(1)
            
            # Aguardar um pouco mais para garantir que todos os downloads foram concluídos
            time.sleep(2)
            
            # Usar o método descompactar_arquivos_paralelo do descompactador
            logger.info("Iniciando descompactação de todos os arquivos baixados...")
            total, descompactados, falhas = descompactador.descompactar_arquivos_paralelo(
                ano=ano,
                ano_inicio=ano_inicio,
                ano_fim=ano_fim
            )
            
            # Atualizar estatísticas
            with self.lock:
                self.stats['descompactacao']['total'] = total
                self.stats['descompactacao']['concluidos'] = descompactados
                self.stats['descompactacao']['falhas'] = falhas
            
            # Marcar descompactação como finalizada
            self.descompactacao_finalizada = True
            logger.info("Worker de descompactação finalizado")
            
        except Exception as e:
            logger.error(f"Erro no worker de descompactação: {e}")
            self.descompactacao_finalizada = True
    
    def _worker_conversao(self, conversor: ConversorParquet, ano: Optional[int], consolidar: bool):
        """Worker responsável pela conversão de arquivos."""
        logger.info("Worker de conversão iniciado")
        
        try:
            # Aguardar descompactação finalizar
            while not self.descompactacao_finalizada:
                time.sleep(1)
            
            # Aguardar um pouco mais para garantir que todos os arquivos foram descompactados
            time.sleep(2)
            
            # Usar o método converter_diretorio do conversor para processar todos os arquivos
            logger.info("Iniciando conversão de todos os arquivos descompactados...")
            resultado = conversor.converter_diretorio("files-unzip", ano=ano, consolidar=consolidar)
            
            # Atualizar estatísticas
            with self.lock:
                self.stats['conversao']['total'] = resultado['total']
                self.stats['conversao']['concluidos'] = resultado['convertidos']
                self.stats['conversao']['falhas'] = resultado['falhas']
            
            # Marcar conversão como finalizada
            self.conversao_finalizada = True
            logger.info("Worker de conversão finalizado")
            
        except Exception as e:
            logger.error(f"Erro no worker de conversão: {e}")
            self.conversao_finalizada = True
    
    def _download_arquivo(self, gerenciador: GerenciadorArquivosFTP, arquivo: str) -> tuple[bool, str]:
        """Download de um arquivo específico."""
        try:
            sucesso = gerenciador.baixar_arquivo(arquivo)
            return sucesso, arquivo
        except Exception as e:
            logger.error(f"Erro ao baixar {arquivo}: {e}")
            return False, arquivo
    
 