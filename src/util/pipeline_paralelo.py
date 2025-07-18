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
    
    def __init__(self, max_workers: Optional[int] = None, chunk_size: Optional[int] = None, campos_especificos: Optional[List[str]] = None):
        """
        Inicializa o pipeline paralelo.
        
        Args:
            max_workers: Número máximo de workers para processamento paralelo
            chunk_size: Tamanho do chunk para conversão
            campos_especificos: Lista de campos específicos a serem incluídos na conversão
        """
        self.max_workers = max_workers or 4
        self.chunk_size = chunk_size
        self.campos_especificos = campos_especificos
        
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
        
        # Monitor de progresso por arquivo
        self.monitor_arquivos = {}  # {arquivo: {'download': bool, 'descompactacao': bool, 'conversao': bool}}
        self.monitor_lock = threading.Lock()
    
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
        conversor = ConversorParquet(chunk_size=self.chunk_size, max_workers=self.max_workers, campos_especificos=self.campos_especificos)
        
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
                conversor, ano, ano_inicio, ano_fim, consolidar
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
        conversor = ConversorParquet(chunk_size=self.chunk_size, max_workers=self.max_workers, campos_especificos=self.campos_especificos)
        
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
        conversor = ConversorParquet(chunk_size=self.chunk_size, max_workers=self.max_workers, campos_especificos=self.campos_especificos)
        
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
        conversor = ConversorParquet(chunk_size=self.chunk_size, max_workers=self.max_workers, campos_especificos=self.campos_especificos)
        
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
    
    def _inicializar_monitor_arquivo(self, nome_arquivo: str):
        """Inicializa o monitor para um arquivo específico."""
        with self.monitor_lock:
            self.monitor_arquivos[nome_arquivo] = {
                'download': False,
                'descompactacao': False,
                'conversao': False
            }
    
    def _marcar_etapa_concluida(self, nome_arquivo: str, etapa: str):
        """Marca uma etapa como concluída para um arquivo específico."""
        with self.monitor_lock:
            if nome_arquivo in self.monitor_arquivos:
                self.monitor_arquivos[nome_arquivo][etapa] = True
                logger.debug(f"Etapa {etapa} concluída para {nome_arquivo}")
    
    def _arquivo_pronto_para_proxima_etapa(self, nome_arquivo: str, etapa_atual: str) -> bool:
        """Verifica se um arquivo está pronto para a próxima etapa."""
        with self.monitor_lock:
            if nome_arquivo not in self.monitor_arquivos:
                return False
            
            if etapa_atual == 'download':
                return self.monitor_arquivos[nome_arquivo]['download']
            elif etapa_atual == 'descompactacao':
                return self.monitor_arquivos[nome_arquivo]['download'] and not self.monitor_arquivos[nome_arquivo]['descompactacao']
            elif etapa_atual == 'conversao':
                return self.monitor_arquivos[nome_arquivo]['descompactacao'] and not self.monitor_arquivos[nome_arquivo]['conversao']
            
            return False
    
    def _worker_download(self, gerenciador: GerenciadorArquivosFTP,
                        ano: Optional[int], ano_inicio: Optional[int], ano_fim: Optional[int]):
        """Worker responsável pelo download de arquivos."""
        logger.info("Worker de download iniciado")
        
        try:
            # Listar arquivos disponíveis
            arquivos_disponiveis = gerenciador.listar_arquivos_remotos(ano, ano_inicio, ano_fim)
            
            # Extrair apenas os nomes dos arquivos
            nomes_arquivos = [arq["nome"] for arq in arquivos_disponiveis]
            
            # Inicializar monitor para cada arquivo
            for nome_arquivo in nomes_arquivos:
                self._inicializar_monitor_arquivo(nome_arquivo)
            
            with self.lock:
                self.stats['download']['total'] = len(nomes_arquivos)
            
            logger.info(f"Encontrados {len(nomes_arquivos)} arquivos para download")
            
            # Verificar quais arquivos precisam ser baixados
            arquivos_para_baixar = []
            ftp = gerenciador._conectar_ftp()
            
            if ftp:
                try:
                    logger.info("Verificando arquivos que precisam ser baixados...")
                    for nome_arquivo in nomes_arquivos:
                        # Obter informações detalhadas do arquivo remoto
                        info_remoto = gerenciador._verificar_arquivo_remoto(ftp, nome_arquivo)
                        
                        # Verificar se o arquivo precisa ser baixado
                        if gerenciador._arquivo_precisa_baixar(nome_arquivo, info_remoto):
                            arquivos_para_baixar.append(nome_arquivo)
                        else:
                            # Arquivo já existe e está atualizado, marcar como concluído
                            with self.lock:
                                self.stats['download']['concluidos'] += 1
                            self._marcar_etapa_concluida(nome_arquivo, 'download')
                            self.fila_descompactacao.put(nome_arquivo)
                            logger.debug(f"Arquivo {nome_arquivo} já existe e está atualizado, pulando download")
                    
                    logger.info(f"Verificação concluída: {len(arquivos_para_baixar)} de {len(nomes_arquivos)} arquivos precisam ser baixados")
                    
                finally:
                    try:
                        ftp.quit()
                    except:
                        pass
            else:
                logger.error("Não foi possível conectar ao FTP para verificação, baixando todos os arquivos")
                arquivos_para_baixar = nomes_arquivos
            
            # Se não há arquivos para baixar, finalizar
            if not arquivos_para_baixar:
                logger.info("Todos os arquivos estão atualizados!")
                self.download_finalizado = True
                return
            
            # Processar downloads em paralelo apenas dos arquivos que precisam
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = []
                
                for nome_arquivo in arquivos_para_baixar:
                    future = executor.submit(self._download_arquivo, gerenciador, nome_arquivo)
                    futures.append(future)
                
                # Processar resultados conforme completam
                for future in as_completed(futures):
                    try:
                        sucesso, nome_arquivo = future.result()
                        if sucesso:
                            with self.lock:
                                self.stats['download']['concluidos'] += 1
                            # Marcar download como concluído
                            self._marcar_etapa_concluida(nome_arquivo, 'download')
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
            while not self.download_finalizado or not self.fila_descompactacao.empty():
                try:
                    # Tentar obter arquivo da fila com timeout
                    arquivo = self.fila_descompactacao.get(timeout=1)
                    
                    # Verificar se o arquivo está pronto para descompactação
                    if self._arquivo_pronto_para_proxima_etapa(arquivo, 'descompactacao'):
                        with self.lock:
                            self.stats['descompactacao']['total'] += 1
                        
                        # Descompactar arquivo individual
                        sucesso = self._descompactar_arquivo_individual(descompactador, arquivo)
                        
                        if sucesso:
                            with self.lock:
                                self.stats['descompactacao']['concluidos'] += 1
                            # Marcar descompactação como concluída
                            self._marcar_etapa_concluida(arquivo, 'descompactacao')
                            # Adicionar à fila de conversão
                            self.fila_conversao.put(arquivo)
                            logger.debug(f"Descompactação concluída: {arquivo}")
                        else:
                            with self.lock:
                                self.stats['descompactacao']['falhas'] += 1
                            logger.error(f"Falha na descompactação: {arquivo}")
                    
                    self.fila_descompactacao.task_done()
                    
                except Exception as e:
                    # Timeout ou erro - continuar loop
                    continue
            
            # Marcar descompactação como finalizada
            self.descompactacao_finalizada = True
            logger.info("Worker de descompactação finalizado")
            
        except Exception as e:
            logger.error(f"Erro no worker de descompactação: {e}")
            self.descompactacao_finalizada = True
    
    def _worker_conversao(self, conversor: ConversorParquet, ano: Optional[int], ano_inicio: Optional[int] = None, ano_fim: Optional[int] = None, consolidar: bool = False):
        """Worker responsável pela conversão de arquivos."""
        logger.info("Worker de conversão iniciado")
        
        try:
            while not self.descompactacao_finalizada or not self.fila_conversao.empty():
                try:
                    # Tentar obter arquivo da fila com timeout
                    arquivo = self.fila_conversao.get(timeout=1)
                    
                    # Verificar se o arquivo está pronto para conversão
                    if self._arquivo_pronto_para_proxima_etapa(arquivo, 'conversao'):
                        with self.lock:
                            self.stats['conversao']['total'] += 1
                        
                        # Converter arquivo individual
                        sucesso = self._converter_arquivo_individual(conversor, arquivo, ano, ano_inicio, ano_fim)
                        
                        if sucesso:
                            with self.lock:
                                self.stats['conversao']['concluidos'] += 1
                            # Marcar conversão como concluída
                            self._marcar_etapa_concluida(arquivo, 'conversao')
                            logger.debug(f"Conversão concluída: {arquivo}")
                        else:
                            with self.lock:
                                self.stats['conversao']['falhas'] += 1
                            logger.error(f"Falha na conversão: {arquivo}")
                    
                    self.fila_conversao.task_done()
                    
                except Exception as e:
                    # Timeout ou erro - continuar loop
                    continue
            
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
    
    def _descompactar_arquivo_individual(self, descompactador: DescompactadorArquivos, arquivo: str) -> bool:
        """Descompactação de um arquivo específico."""
        try:
            # Construir caminho do arquivo zip
            caminho_arquivo_zip = Path("files-zip") / arquivo
            
            if not caminho_arquivo_zip.exists():
                logger.error(f"Arquivo zip não encontrado: {caminho_arquivo_zip}")
                return False
            
            # Verificar se o arquivo precisa ser descompactado
            if not descompactador._arquivo_precisa_descompactar(caminho_arquivo_zip):
                logger.debug(f"Arquivo {arquivo} já está descompactado, pulando...")
                return True  # Considerar sucesso se já está descompactado
            
            # Descompactar arquivo
            sucesso = descompactador.descompactar_arquivo(caminho_arquivo_zip)
            return sucesso
            
        except Exception as e:
            logger.error(f"Erro ao descompactar {arquivo}: {e}")
            return False
    
    def _converter_arquivo_individual(self, conversor: ConversorParquet, arquivo: str, ano: Optional[int], ano_inicio: Optional[int] = None, ano_fim: Optional[int] = None) -> bool:
        """Conversão de um arquivo específico."""
        try:
            # Extrair nome do arquivo sem extensão e pasta
            nome_arquivo = Path(arquivo).stem  # Remove extensão .7z
            
            # Procurar arquivos TXT descompactados deste arquivo
            diretorio_unzip = Path("files-unzip")
            arquivos_txt_encontrados = []
            
            # Determinar anos válidos para processamento
            anos_validos = set()
            if ano is not None:
                anos_validos.add(ano)
            elif ano_inicio is not None and ano_fim is not None:
                anos_validos = set(range(ano_inicio, ano_fim + 1))
            elif ano_inicio is not None:
                # Ano inicial até o último disponível (limite arbitrário)
                anos_validos = set(range(ano_inicio, 2100))
            
            # Procurar apenas nas pastas de anos válidos
            for pasta_ano in diretorio_unzip.iterdir():
                if pasta_ano.is_dir():
                    # Tentar extrair o ano da pasta
                    ano_pasta = None
                    try:
                        ano_pasta = int(pasta_ano.name)
                    except ValueError:
                        continue
                    
                    # Verificar se o ano da pasta está nos anos válidos
                    if anos_validos and ano_pasta not in anos_validos:
                        logger.debug(f"Pulando pasta {pasta_ano.name} - ano não solicitado")
                        continue
                    
                    for arquivo_txt in pasta_ano.glob("*.txt"):
                        # Verificar se o arquivo TXT corresponde ao arquivo zip original
                        if nome_arquivo.lower() in arquivo_txt.name.lower():
                            arquivos_txt_encontrados.append((arquivo_txt, ano_pasta))
            
            if not arquivos_txt_encontrados:
                logger.warning(f"Nenhum arquivo TXT encontrado para converter do arquivo {arquivo} nos anos solicitados")
                return False
            
            # Converter cada arquivo TXT encontrado
            sucessos = 0
            for arquivo_txt, ano_arquivo in arquivos_txt_encontrados:
                try:
                    # Usar o ano da pasta se disponível, senão usar o ano passado como parâmetro
                    ano_para_conversao = ano_arquivo if ano_arquivo else ano
                    sucesso = conversor.converter_arquivo_txt_para_parquet(arquivo_txt, ano_para_conversao)
                    if sucesso:
                        sucessos += 1
                        logger.debug(f"Conversão concluída: {arquivo_txt.name} (ano: {ano_para_conversao})")
                    else:
                        logger.error(f"Falha na conversão: {arquivo_txt.name}")
                except Exception as e:
                    logger.error(f"Erro ao converter {arquivo_txt}: {e}")
            
            # Retornar True se pelo menos um arquivo foi convertido com sucesso
            return sucessos > 0
            
        except Exception as e:
            logger.error(f"Erro ao converter {arquivo}: {e}")
            return False
    
 