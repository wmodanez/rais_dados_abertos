import logging
import py7zr
from pathlib import Path
from typing import List, Tuple, Optional
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import re

# Configuração de logging
logger = logging.getLogger("descompactador")

class DescompactadorArquivos:
    def __init__(self, max_workers: int = 4):
        """
        Inicializa o descompactador de arquivos.
        
        Args:
            max_workers: Número máximo de workers para descompactação paralela
        """
        self.max_workers = max_workers
        
        # Criar diretório files-unzip se não existir
        Path("files-unzip").mkdir(exist_ok=True)
    
    def _extrair_ano_arquivo(self, nome_arquivo: str) -> Optional[str]:
        """
        Extrai o ano do nome do arquivo.
        Retorna None se não encontrar um ano válido.
        """
        match = re.search(r"(20\d{2})", nome_arquivo)
        if match:
            return match.group(1)
        return None
    
    def _destino_arquivo_unzip(self, caminho_arquivo_zip: Path) -> Path:
        """
        Retorna o caminho de destino para o arquivo descompactado, respeitando a estrutura files-unzip/ANO/
        """
        # Extrair o ano do caminho do arquivo zip
        ano = self._extrair_ano_arquivo(str(caminho_arquivo_zip))
        if not ano:
            ano = "desconhecido"
        return Path("files-unzip") / ano
    
    def descompactar_arquivo(self, caminho_arquivo_zip: Path) -> bool:
        """
        Descompacta um arquivo .7z.
        
        Args:
            caminho_arquivo_zip: Caminho do arquivo .7z a ser descompactado
            
        Returns:
            True se a descompactação foi bem-sucedida, False caso contrário
        """
        if not caminho_arquivo_zip.exists():
            logger.error(f"Arquivo não encontrado: {caminho_arquivo_zip}")
            return False
        
        if not str(caminho_arquivo_zip).endswith('.7z'):
            logger.warning(f"Arquivo não é .7z: {caminho_arquivo_zip}")
            return False
        
        try:
            # Criar diretório de destino
            diretorio_destino = self._destino_arquivo_unzip(caminho_arquivo_zip)
            diretorio_destino.mkdir(parents=True, exist_ok=True)
            
            logger.info(f"Descompactando {caminho_arquivo_zip.name} para {diretorio_destino}")
            
            # Descompactar arquivo
            with py7zr.SevenZipFile(caminho_arquivo_zip, mode='r') as z:
                z.extractall(diretorio_destino)
            
            logger.info(f"Descompactação concluída: {caminho_arquivo_zip.name}")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao descompactar {caminho_arquivo_zip}: {e}")
            return False
    
    def listar_arquivos_zip_locais(self, ano: Optional[int] = None, ano_inicio: Optional[int] = None, ano_fim: Optional[int] = None) -> List[Path]:
        """
        Lista arquivos .7z na pasta files-zip, opcionalmente filtrados por ano.
        
        Args:
            ano: Ano específico para filtrar
            ano_inicio: Ano inicial da faixa
            ano_fim: Ano final da faixa
            
        Returns:
            Lista de caminhos dos arquivos .7z encontrados e filtrados
        """
        arquivos_zip = []
        diretorio_zip = Path("files-zip")
        
        # Criar diretório se não existir
        diretorio_zip.mkdir(exist_ok=True)
        
        try:
            # Listar todos os arquivos .7z
            todos_arquivos = list(diretorio_zip.glob("**/*.7z"))
            
            # Aplicar filtros de ano se especificados
            if ano is not None or ano_inicio is not None or ano_fim is not None:
                anos_validos = set()
                
                if ano is not None:
                    anos_validos.add(str(ano))
                elif ano_inicio is not None and ano_fim is not None:
                    anos_validos = set(str(a) for a in range(ano_inicio, ano_fim + 1))
                elif ano_inicio is not None:
                    # Ano inicial até o último disponível
                    anos_validos = set(str(a) for a in range(ano_inicio, 2100))  # 2100: limite arbitrário
                
                # Filtrar arquivos por ano
                for arquivo in todos_arquivos:
                    ano_arquivo = self._extrair_ano_arquivo(arquivo.name)
                    if ano_arquivo and ano_arquivo in anos_validos:
                        arquivos_zip.append(arquivo)
                        logger.debug(f"Arquivo incluído no filtro: {arquivo.name} (ano: {ano_arquivo})")
                    else:
                        logger.debug(f"Arquivo excluído do filtro: {arquivo.name} (ano extraído: {ano_arquivo})")
            else:
                # Sem filtro de ano, incluir todos
                arquivos_zip = todos_arquivos
            
            logger.info(f"Encontrados {len(arquivos_zip)} arquivos .7z para descompactar (filtro: ano={ano}, ano_inicio={ano_inicio}, ano_fim={ano_fim})")
            return arquivos_zip
            
        except Exception as e:
            logger.error(f"Erro ao listar arquivos .7z: {e}")
            return []
    
    def descompactar_arquivos_paralelo(self, max_workers: Optional[int] = None, ano: Optional[int] = None, ano_inicio: Optional[int] = None, ano_fim: Optional[int] = None) -> Tuple[int, int, int]:
        """
        Descompacta arquivos .7z de forma paralela, opcionalmente filtrados por ano.
        
        Args:
            max_workers: Número máximo de workers (usa self.max_workers se None)
            ano: Ano específico para filtrar
            ano_inicio: Ano inicial da faixa
            ano_fim: Ano final da faixa
            
        Returns:
            Tupla com (total de arquivos, arquivos descompactados, falhas)
        """
        if max_workers is None:
            max_workers = self.max_workers
        
        logger.info("Iniciando descompactação paralela de arquivos")
        
        # Listar arquivos .7z com filtros
        arquivos_zip = self.listar_arquivos_zip_locais(ano, ano_inicio, ano_fim)
        
        if not arquivos_zip:
            logger.info("Nenhum arquivo .7z encontrado para descompactar")
            return 0, 0, 0
        
        total = len(arquivos_zip)
        descompactados = 0
        falhas = 0
        
        logger.info(f"Iniciando descompactação paralela de {total} arquivos com {max_workers} workers")
        
        # Barra de progresso principal
        with tqdm(
            total=total,
            desc="Descompactando arquivos",
            unit="arquivo",
            position=0,
            leave=True
        ) as pbar_principal:
            
            # Usar ThreadPoolExecutor para descompactação paralela
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submeter todas as descompactações
                future_to_arquivo = {
                    executor.submit(self.descompactar_arquivo, arquivo_zip): arquivo_zip 
                    for arquivo_zip in arquivos_zip
                }
                
                # Processar resultados conforme completam
                for future in as_completed(future_to_arquivo):
                    arquivo_zip = future_to_arquivo[future]
                    try:
                        sucesso = future.result()
                        if sucesso:
                            descompactados += 1
                            pbar_principal.set_postfix({
                                'Descompactados': descompactados,
                                'Falhas': falhas,
                                'Atual': arquivo_zip.name[:30] + '...' if len(arquivo_zip.name) > 30 else arquivo_zip.name
                            })
                            logger.info(f"Descompactação concluída: {arquivo_zip.name}")
                        else:
                            falhas += 1
                            pbar_principal.set_postfix({
                                'Descompactados': descompactados,
                                'Falhas': falhas,
                                'Atual': arquivo_zip.name[:30] + '...' if len(arquivo_zip.name) > 30 else arquivo_zip.name
                            })
                            logger.error(f"Falha na descompactação: {arquivo_zip.name}")
                        
                        # Atualizar barra de progresso
                        pbar_principal.update(1)
                        
                    except Exception as e:
                        falhas += 1
                        pbar_principal.set_postfix({
                            'Descompactados': descompactados,
                            'Falhas': falhas,
                            'Atual': arquivo_zip.name[:30] + '...' if len(arquivo_zip.name) > 30 else arquivo_zip.name
                        })
                        logger.error(f"Exceção na descompactação de {arquivo_zip.name}: {e}")
                        pbar_principal.update(1)
        
        logger.info(f"Descompactação concluída: {descompactados} arquivos descompactados, {falhas} falhas")
        return total, descompactados, falhas 