import logging
import py7zr
from pathlib import Path
from typing import List, Tuple, Optional, Dict, Any
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import hashlib
import time

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
        # Extrair o ano do caminho do arquivo zip (pasta pai)
        ano = "desconhecido"
        for parte in caminho_arquivo_zip.parts:
            if re.match(r"^(20\d{2})$", parte):
                ano = parte
                break
        
        return Path("files-unzip") / ano
    
    def _verificar_arquivo_zip(self, caminho_arquivo_zip: Path) -> Dict[str, Any]:
        """
        Verifica informações do arquivo .7z.
        
        Args:
            caminho_arquivo_zip: Caminho do arquivo .7z
            
        Returns:
            Dicionário com informações do arquivo
        """
        info = {
            "existe": False,
            "tamanho": 0,
            "data_modificacao": None,
            "hash_md5": None
        }
        
        if caminho_arquivo_zip.exists():
            stat = caminho_arquivo_zip.stat()
            info["existe"] = True
            info["tamanho"] = stat.st_size
            info["data_modificacao"] = stat.st_mtime
            
            # Calcular hash MD5
            try:
                with open(caminho_arquivo_zip, 'rb') as f:
                    hash_md5 = hashlib.md5()
                    for chunk in iter(lambda: f.read(4096), b""):
                        hash_md5.update(chunk)
                    info["hash_md5"] = hash_md5.hexdigest()
            except Exception as e:
                logger.debug(f"Erro ao calcular hash MD5 de {caminho_arquivo_zip}: {e}")
        
        return info
    
    def _verificar_arquivo_descompactado(self, caminho_arquivo_zip: Path) -> Dict[str, Any]:
        """
        Verifica se o arquivo já foi descompactado e retorna informações.
        
        Args:
            caminho_arquivo_zip: Caminho do arquivo .7z
            
        Returns:
            Dicionário com informações do arquivo descompactado
        """
        info = {
            "existe": False,
            "arquivos_txt": [],
            "tamanho_total": 0,
            "data_modificacao": None
        }
        
        # Determinar diretório de destino
        diretorio_destino = self._destino_arquivo_unzip(caminho_arquivo_zip)
        
        if diretorio_destino.exists():
            # Procurar por arquivos .txt que correspondam ao arquivo .7z
            nome_base = caminho_arquivo_zip.stem  # Nome sem extensão
            arquivos_txt = list(diretorio_destino.glob(f"{nome_base}*.txt"))
            
            if arquivos_txt:
                info["existe"] = True
                info["arquivos_txt"] = [str(arq) for arq in arquivos_txt]
                
                # Calcular tamanho total e data de modificação mais recente
                tamanho_total = 0
                data_mais_recente = 0
                
                for arquivo_txt in arquivos_txt:
                    if arquivo_txt.exists():
                        stat = arquivo_txt.stat()
                        tamanho_total += stat.st_size
                        data_mais_recente = max(data_mais_recente, stat.st_mtime)
                
                info["tamanho_total"] = tamanho_total
                info["data_modificacao"] = data_mais_recente
        
        return info
    
    def _arquivo_precisa_descompactar(self, caminho_arquivo_zip: Path) -> bool:
        """
        Verifica se o arquivo precisa ser descompactado comparando informações do .7z e arquivos descompactados.
        
        Args:
            caminho_arquivo_zip: Caminho do arquivo .7z
            
        Returns:
            True se o arquivo precisa ser descompactado, False caso contrário
        """
        info_zip = self._verificar_arquivo_zip(caminho_arquivo_zip)
        info_descompactado = self._verificar_arquivo_descompactado(caminho_arquivo_zip)
        
        # Se o arquivo .7z não existe, não precisa descompactar
        if not info_zip["existe"]:
            logger.warning(f"Arquivo .7z não encontrado: {caminho_arquivo_zip}")
            return False
        
        # Se não há arquivos descompactados, precisa descompactar
        if not info_descompactado["existe"]:
            logger.info(f"Arquivo {caminho_arquivo_zip.name} não foi descompactado ainda")
            return True
        
        # Se há arquivos descompactados, verificar se estão atualizados
        # Comparar data de modificação do .7z com a dos arquivos descompactados
        if info_zip["data_modificacao"] and info_descompactado["data_modificacao"]:
            if info_zip["data_modificacao"] > info_descompactado["data_modificacao"]:
                logger.info(f"Arquivo {caminho_arquivo_zip.name} foi modificado após descompactação")
                return True
        
        # Verificar se os arquivos .txt existem e têm tamanho > 0
        arquivos_txt = info_descompactado["arquivos_txt"]
        if not arquivos_txt:
            logger.info(f"Nenhum arquivo .txt encontrado para {caminho_arquivo_zip.name}")
            return True
        
        # Verificar se todos os arquivos .txt têm tamanho > 0
        for arquivo_txt in arquivos_txt:
            if not Path(arquivo_txt).exists() or Path(arquivo_txt).stat().st_size == 0:
                logger.info(f"Arquivo .txt vazio ou não encontrado: {arquivo_txt}")
                return True
        
        logger.debug(f"Arquivo {caminho_arquivo_zip.name} já está descompactado e atualizado")
        return False
    
    def _verificar_arquivo_para_descompactar(self, caminho_arquivo_zip: Path) -> bool:
        """
        Verifica se um arquivo .7z precisa ser descompactado (versão para uso paralelo).
        
        Args:
            caminho_arquivo_zip: Caminho do arquivo .7z
            
        Returns:
            True se o arquivo precisa ser descompactado, False caso contrário
        """
        # Verificar se o arquivo .7z existe
        info_zip = self._verificar_arquivo_zip(caminho_arquivo_zip)
        if not info_zip["existe"]:
            logger.warning(f"Arquivo .7z não encontrado: {caminho_arquivo_zip}")
            return False
        
        # Verificar se já foi descompactado
        info_descompactado = self._verificar_arquivo_descompactado(caminho_arquivo_zip)
        
        # Se não existe arquivo descompactado, precisa descompactar
        if not info_descompactado["existe"]:
            return True
        
        # Se existe arquivo descompactado, verificar se é mais recente que o .7z
        if info_descompactado["data_modificacao"] and info_zip["data_modificacao"]:
            if info_descompactado["data_modificacao"] < info_zip["data_modificacao"]:
                return True
        
        # Se chegou aqui, o arquivo já está descompactado e atualizado
        return False
    
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
            
            # Descompactar arquivo
            with py7zr.SevenZipFile(caminho_arquivo_zip, mode='r') as z:
                z.extractall(diretorio_destino)
            
            return True
            
        except Exception as e:
            logger.error(f"Erro ao descompactar {caminho_arquivo_zip}: {e}")
            return False
    
    def descompactar_arquivos_paralelo(self, max_workers: Optional[int] = None, ano: Optional[int] = None, ano_inicio: Optional[int] = None, ano_fim: Optional[int] = None) -> Tuple[int, int, int]:
        """
        Descompacta arquivos .7z de forma paralela, opcionalmente filtrados por ano.
        Verifica se arquivos já foram descompactados para evitar reprocessamento.
        
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
        
        #logger.info(f"Encontrados {len(arquivos_zip)} arquivos .7z para descompactar (filtro: ano={ano}, ano_inicio={ano_inicio}, ano_fim={ano_fim})")
        
        # Verificar quais arquivos precisam ser descompactados EM PARALELO
        logger.info("Verificando arquivos que precisam ser descompactados...")
        
        arquivos_para_descompactar = []
        arquivos_verificados = 0
        
        # Usar ThreadPoolExecutor para verificação paralela
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submeter verificação de todos os arquivos
            future_to_arquivo = {
                executor.submit(self._verificar_arquivo_para_descompactar, arquivo_zip): arquivo_zip 
                for arquivo_zip in arquivos_zip
            }
            
            # Processar resultados conforme completam
            for future in as_completed(future_to_arquivo):
                arquivo_zip = future_to_arquivo[future]
                arquivos_verificados += 1
                
                try:
                    precisa_descompactar = future.result()
                    if precisa_descompactar:
                        arquivos_para_descompactar.append(arquivo_zip)
                except Exception as e:
                    logger.error(f"Erro ao verificar {arquivo_zip.name}: {e}")
                
                # Log de progresso a cada 20 arquivos verificados (menos verboso)
                if arquivos_verificados % 20 == 0:
                    logger.info(f"Verificados {arquivos_verificados}/{len(arquivos_zip)} arquivos...")
        
        total = len(arquivos_zip)
        descompactados = 0
        falhas = 0
        
        logger.info(f"✓ Verificação concluída: {len(arquivos_para_descompactar)} de {total} arquivos precisam ser descompactados")
        
        if not arquivos_para_descompactar:
            logger.info("✓ Todos os arquivos já estão descompactados!")
            return total, 0, 0
        
        logger.info(f"🚀 Iniciando descompactação paralela de {len(arquivos_para_descompactar)} arquivos com {max_workers} workers")
        
        # Barra de progresso principal
        with tqdm(
            total=len(arquivos_para_descompactar),
            desc="Descompactando arquivos",
            unit="arquivo",
            position=0,
            leave=True
        ) as pbar_principal:
            
            # Usar ThreadPoolExecutor para descompactação paralela
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submeter apenas os arquivos que precisam ser descompactados
                future_to_arquivo = {
                    executor.submit(self.descompactar_arquivo, arquivo_zip): arquivo_zip 
                    for arquivo_zip in arquivos_para_descompactar
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
                        else:
                            falhas += 1
                            pbar_principal.set_postfix({
                                'Descompactados': descompactados,
                                'Falhas': falhas,
                                'Atual': arquivo_zip.name[:30] + '...' if len(arquivo_zip.name) > 30 else arquivo_zip.name
                            })
                            logger.error(f"❌ Falha na descompactação: {arquivo_zip.name}")
                        
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
        
        logger.info(f"✅ Descompactação concluída: {descompactados} arquivos descompactados, {falhas} falhas")
        
        # Mostrar resumo por ano se houver múltiplos anos
        if ano_inicio and ano_fim and ano_inicio != ano_fim:
            self._mostrar_resumo_por_ano(arquivos_para_descompactar)
        
        return total, descompactados, falhas 
    
    def _mostrar_resumo_por_ano(self, arquivos_processados: List[Path]) -> None:
        """
        Mostra um resumo dos arquivos processados organizados por ano.
        
        Args:
            arquivos_processados: Lista de arquivos que foram processados
        """
        from collections import defaultdict
        
        # Agrupar arquivos por ano
        arquivos_por_ano = defaultdict(list)
        for arquivo in arquivos_processados:
            ano = self._extrair_ano_arquivo(arquivo.name)
            if ano:
                arquivos_por_ano[ano].append(arquivo)
        
        if len(arquivos_por_ano) > 1:
            logger.info("📊 Resumo por ano:")
            for ano in sorted(arquivos_por_ano.keys()):
                quantidade = len(arquivos_por_ano[ano])
                logger.info(f"   {ano}: {quantidade} arquivo{'s' if quantidade > 1 else ''}")
    
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
                
                # Para cada ano válido, listar todos os arquivos .7z dentro da pasta do ano
                for ano_str in anos_validos:
                    pasta_ano = diretorio_zip / ano_str
                    if pasta_ano.exists() and pasta_ano.is_dir():
                        arquivos_do_ano = list(pasta_ano.glob("*.7z"))
                        arquivos_zip.extend(arquivos_do_ano)
                        logger.debug(f"Encontrados {len(arquivos_do_ano)} arquivos .7z na pasta {ano_str}/")
                        for arquivo in arquivos_do_ano:
                            logger.debug(f"Arquivo incluído no filtro: {arquivo.name} (pasta: {ano_str})")
            else:
                # Sem filtro de ano, incluir todos os arquivos .7z em qualquer pasta
                arquivos_zip = list(diretorio_zip.glob("**/*.7z"))
            
            logger.info(f"Encontrados {len(arquivos_zip)} arquivos .7z para descompactar (filtro: ano={ano}, ano_inicio={ano_inicio}, ano_fim={ano_fim})")
            return arquivos_zip
            
        except Exception as e:
            logger.error(f"Erro ao listar arquivos .7z: {e}")
            return [] 