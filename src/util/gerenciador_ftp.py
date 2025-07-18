import os
import hashlib
import logging
import time
import ftplib
from pathlib import Path
from typing import List, Dict, Tuple, Optional, Any
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import re
from datetime import datetime

# Configuração de logging
logger = logging.getLogger("gerenciador_arquivos")

class GerenciadorArquivosFTP:
    def __init__(
        self, 
        servidor_ftp: str,
        diretorio_remoto: str,
        max_tentativas: int = 3, 
        tempo_espera: int = 5,
        max_workers: int = 4
    ):
        """
        Inicializa o gerenciador de arquivos FTP.
        
        Args:
            servidor_ftp: Endereço do servidor FTP
            diretorio_remoto: Diretório remoto no servidor FTP
            max_tentativas: Número máximo de tentativas de download em caso de falha
            tempo_espera: Tempo de espera em segundos entre tentativas
            max_workers: Número máximo de workers para download paralelo
        """
        self.servidor_ftp = servidor_ftp
        self.diretorio_remoto = diretorio_remoto
        self.max_tentativas = max_tentativas
        self.tempo_espera = tempo_espera
        self.max_workers = max_workers
        
        # Criar diretório de logs se não existir
        Path("logs").mkdir(exist_ok=True)
        # Criar diretório files-zip se não existir
        Path("files-zip").mkdir(exist_ok=True)
        # Lock para operações de criação de diretórios
        self._dir_lock = threading.Lock()
    
    def _conectar_ftp(self) -> Optional[ftplib.FTP]:
        """
        Estabelece conexão com o servidor FTP.
        
        Returns:
            Objeto de conexão FTP ou None em caso de falha
        """
        try:
            ftp = ftplib.FTP(self.servidor_ftp)
            ftp.login()  # Login anônimo
            ftp.cwd(self.diretorio_remoto)
            return ftp
        except Exception as e:
            logger.error(f"Erro ao conectar ao servidor FTP: {e}")
            return None
    
    def listar_diretorios_remotos(self) -> List[str]:
        """
        Lista todos os diretórios disponíveis no repositório remoto FTP.
        
        Returns:
            Lista com os nomes dos diretórios remotos
        """
        logger.info(f"Listando diretórios remotos em ftp://{self.servidor_ftp}/{self.diretorio_remoto}")
        
        ftp = self._conectar_ftp()
        if not ftp:
            return []
        
        diretorios = []
        try:
            itens = []
            ftp.dir(lambda x: itens.append(x))
            logger.debug(f"Total de itens encontrados: {len(itens)}")
            for item in itens:
                logger.debug(f"Processando item: {item}")
                # Para FTP do MTPS, diretórios têm '<DIR>' na linha
                if '<DIR>' in item:
                    nome = item.rsplit(None, 1)[-1]
                    diretorios.append(nome)
                    logger.debug(f"Diretório encontrado: {nome}")
            logger.info(f"Encontrados {len(diretorios)} diretórios remotos")
            return diretorios
        except Exception as e:
            logger.error(f"Erro ao listar diretórios remotos: {e}")
            return []
        finally:
            try:
                ftp.quit()
            except:
                pass
    
    def calcular_hash_arquivo(self, caminho_arquivo: Path) -> str:
        """
        Calcula o hash MD5 de um arquivo.
        
        Args:
            caminho_arquivo: Caminho do arquivo
            
        Returns:
            Hash MD5 do arquivo em formato hexadecimal
        """
        hash_md5 = hashlib.md5()
        
        try:
            with open(caminho_arquivo, "rb") as f:
                for bloco in iter(lambda: f.read(4096), b""):
                    hash_md5.update(bloco)
            return hash_md5.hexdigest()
        except Exception as e:
            logger.error(f"Erro ao calcular hash do arquivo {caminho_arquivo}: {e}")
            return ""
    
    def _listar_arquivos_diretorio_ftp(self, ftp: ftplib.FTP, diretorio: str = "") -> List[Dict[str, Any]]:
        """
        Lista arquivos em um diretório FTP, incluindo subdiretórios.
        
        Args:
            ftp: Conexão FTP ativa
            diretorio: Diretório relativo a ser listado
            
        Returns:
            Lista de dicionários com informações dos arquivos
        """
        arquivos = []
        diretorio_atual = self.diretorio_remoto
        
        if diretorio:
            diretorio_atual = f"{diretorio_atual}/{diretorio}"
            try:
                ftp.cwd(f"{self.diretorio_remoto}/{diretorio}")
            except:
                logger.error(f"Erro ao acessar diretório {diretorio}")
                return []
        else:
            try:
                ftp.cwd(self.diretorio_remoto)
            except:
                logger.error(f"Erro ao acessar diretório base {self.diretorio_remoto}")
                return []
        
        # Listar conteúdo do diretório
        itens = []
        try:
            ftp.dir(lambda x: itens.append(x))
        except Exception as e:
            logger.error(f"Erro ao listar diretório {diretorio_atual}: {e}")
            return []
        
        logger.debug(f"Processando {len(itens)} itens no diretório {diretorio_atual}")
        
        # Processar cada item
        for item in itens:
            logger.debug(f"Processando item: {item}")
            
            # Para FTP do MTPS, verificar se é diretório pela presença de '<DIR>'
            if '<DIR>' in item:
                # É um diretório
                nome = item.rsplit(None, 1)[-1]
                caminho_relativo = nome
                if diretorio:
                    caminho_relativo = f"{diretorio}/{nome}"
                
                logger.debug(f"Encontrado diretório: {nome}")
                
                # Recursivamente listar arquivos do subdiretório usando nova conexão
                subdiretorio = caminho_relativo
                try:
                    # Criar nova conexão FTP para o subdiretório
                    sub_ftp = self._conectar_ftp()
                    if sub_ftp:
                        # Sempre navegar a partir do diretório remoto base
                        sub_ftp.cwd(f"{self.diretorio_remoto}/{subdiretorio}")
                        # Listar arquivos do subdiretório
                        subarquivos = self._listar_arquivos_diretorio_ftp(sub_ftp, subdiretorio)
                        arquivos.extend(subarquivos)
                        sub_ftp.quit()
                except Exception as e:
                    logger.error(f"Erro ao acessar subdiretório {subdiretorio}: {e}")
            else:
                # É um arquivo - tentar extrair informações
                try:
                    partes = item.split()
                    if len(partes) >= 4:
                        # Formato típico: "MM/DD/YYYY HH:MM [DIR] ou tamanho nome_arquivo"
                        # Para arquivos, o tamanho está na posição 2 (após data/hora)
                        tamanho_str = partes[2]
                        nome = " ".join(partes[3:])
                        
                        # Verificar se o tamanho é numérico
                        if tamanho_str.isdigit():
                            tamanho = int(tamanho_str)
                            caminho_relativo = nome
                            if diretorio:
                                caminho_relativo = f"{diretorio}/{nome}"
                            
                            logger.debug(f"Encontrado arquivo: {nome} (tamanho: {tamanho})")
                            
                            arquivos.append({
                                "nome": caminho_relativo,
                                "tamanho": tamanho,
                                "tipo": "arquivo"
                            })
                except Exception as e:
                    logger.debug(f"Erro ao processar item '{item}': {e}")
                    continue
        
        return arquivos
    
    def _listar_arquivos_diretorio_ftp_por_ano(self, ftp: ftplib.FTP, anos_validos: set) -> List[Dict[str, Any]]:
        """
        Lista arquivos FTP acessando apenas diretórios de anos específicos.
        Evita listar todos os diretórios quando apenas alguns anos são necessários.
        
        Args:
            ftp: Conexão FTP ativa
            anos_validos: Set com anos válidos (strings)
            
        Returns:
            Lista de dicionários com informações dos arquivos
        """
        arquivos = []
        
        try:
            # Navegar para o diretório base
            ftp.cwd(self.diretorio_remoto)
        except:
            logger.error(f"Erro ao acessar diretório base {self.diretorio_remoto}")
            return []
        
        # Listar conteúdo do diretório base
        itens = []
        try:
            ftp.dir(lambda x: itens.append(x))
        except Exception as e:
            logger.error(f"Erro ao listar diretório base: {e}")
            return []
        
        logger.debug(f"Processando {len(itens)} itens no diretório base")
        
        # Processar apenas diretórios dos anos válidos
        for item in itens:
            logger.debug(f"Processando item: {item}")
            
            # Verificar se é diretório
            if '<DIR>' in item:
                nome = item.rsplit(None, 1)[-1]
                
                # Verificar se o nome do diretório é um ano válido
                if nome in anos_validos:
                    logger.debug(f"Encontrado diretório do ano válido: {nome}")
                    
                    # Listar arquivos apenas deste diretório
                    try:
                        # Criar nova conexão FTP para o subdiretório
                        sub_ftp = self._conectar_ftp()
                        if sub_ftp:
                            # Navegar para o diretório do ano
                            sub_ftp.cwd(f"{self.diretorio_remoto}/{nome}")
                            
                            # Listar arquivos do diretório do ano
                            subitens = []
                            sub_ftp.dir(lambda x: subitens.append(x))
                            
                            # Processar arquivos do diretório
                            for subitem in subitens:
                                if '<DIR>' not in subitem:  # Apenas arquivos
                                    try:
                                        partes = subitem.split()
                                        if len(partes) >= 4:
                                            tamanho_str = partes[2]
                                            nome_arquivo = " ".join(partes[3:])
                                            
                                            # Verificar se o tamanho é numérico
                                            if tamanho_str.isdigit():
                                                tamanho = int(tamanho_str)
                                                caminho_relativo = f"{nome}/{nome_arquivo}"
                                                
                                                # Verificar se deve excluir por padrão
                                                if not self._deve_excluir_arquivo(nome_arquivo):
                                                    logger.debug(f"Encontrado arquivo: {caminho_relativo} (tamanho: {tamanho})")
                                                    
                                                    arquivos.append({
                                                        "nome": caminho_relativo,
                                                        "tamanho": tamanho,
                                                        "tipo": "arquivo"
                                                    })
                                                else:
                                                    logger.debug(f"Arquivo excluído por padrão: {nome_arquivo}")
                                    except Exception as e:
                                        logger.debug(f"Erro ao processar arquivo '{subitem}': {e}")
                                        continue
                            
                            sub_ftp.quit()
                    except Exception as e:
                        logger.error(f"Erro ao acessar diretório do ano {nome}: {e}")
                else:
                    logger.debug(f"Ignorando diretório não relevante: {nome}")
        
        return arquivos
    
    def listar_arquivos_remotos(self, ano: Optional[int] = None, ano_inicio: Optional[int] = None, ano_fim: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Lista arquivos disponíveis no repositório remoto FTP, com filtro por ano.
        
        Args:
            ano: Ano específico para filtrar
            ano_inicio: Ano inicial da faixa
            ano_fim: Ano final da faixa
            
        Returns:
            Lista de dicionários com informações dos arquivos remotos
        """
        # Determinar anos válidos para filtro
        anos_validos = set()
        if ano:
            anos_validos.add(str(ano))
        elif ano_inicio and ano_fim:
            anos_validos = set(str(a) for a in range(ano_inicio, ano_fim + 1))
        elif ano_inicio:
            # Ano inicial até o último disponível
            anos_validos = set(str(a) for a in range(ano_inicio, 2100))  # 2100: limite arbitrário
        
        if anos_validos:
            logger.info(f"Listando arquivos remotos para anos específicos: {sorted(anos_validos)}")
        else:
            logger.info(f"Listando todos os arquivos remotos em ftp://{self.servidor_ftp}/{self.diretorio_remoto}")
        
        ftp = self._conectar_ftp()
        if not ftp:
            return []
        
        try:
            if anos_validos:
                # Listagem seletiva: apenas diretórios dos anos específicos
                arquivos = self._listar_arquivos_diretorio_ftp_por_ano(ftp, anos_validos)
            else:
                # Listagem completa: todos os arquivos
                arquivos = self._listar_arquivos_diretorio_ftp(ftp)
            
            logger.info(f"Encontrados {len(arquivos)} arquivos remotos")
            return arquivos
        except Exception as e:
            logger.error(f"Erro ao listar arquivos remotos: {e}")
            return []
        finally:
            try:
                ftp.quit()
            except:
                pass
    
    def listar_arquivos_locais(self) -> Dict[str, str]:
        """
        Lista todos os arquivos no diretório local (files-zip) e seus hashes.
        
        Returns:
            Dicionário com nome do arquivo como chave e hash MD5 como valor
        """
        diretorio_local = Path("files-zip")
        logger.info(f"Listando arquivos locais em {diretorio_local}")
        
        arquivos_locais = {}
        try:
            for arquivo in diretorio_local.glob("**/*"):
                if arquivo.is_file():
                    caminho_relativo = str(arquivo.relative_to(diretorio_local))
                    hash_arquivo = self.calcular_hash_arquivo(arquivo)
                    arquivos_locais[caminho_relativo] = hash_arquivo
            
            logger.info(f"Encontrados {len(arquivos_locais)} arquivos locais")
            return arquivos_locais
        except Exception as e:
            logger.error(f"Erro ao listar arquivos locais: {e}")
            return {}
    
    def _extrair_ano_arquivo(self, nome_arquivo: str) -> Optional[str]:
        """
        Extrai o ano do caminho do arquivo remoto.
        Retorna None se não encontrar um ano válido.
        """
        match = re.search(r"(20\d{2})", nome_arquivo)
        if match:
            return match.group(1)
        return None

    def _deve_excluir_arquivo(self, nome_arquivo: str) -> bool:
        """
        Verifica se o arquivo deve ser excluído baseado em padrões no nome.
        Retorna True se o arquivo deve ser excluído.
        """
        padroes_exclusao = ["_EST", "ESTB", "IGN", "NI"]
        nome_upper = nome_arquivo.upper()
        
        for padrao in padroes_exclusao:
            if padrao in nome_upper:
                logger.debug(f"Arquivo {nome_arquivo} excluído por conter padrão '{padrao}'")
                return True
        
        return False

    def filtrar_arquivos_por_ano(self, arquivos: List[Dict[str, Any]], ano: Optional[int] = None, ano_inicio: Optional[int] = None, ano_fim: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Filtra a lista de arquivos remotos para incluir apenas os arquivos do(s) ano(s) desejado(s).
        Exclui arquivos que contenham padrões específicos (_EST, ESTB, IGN, NI).
        """
        anos_validos = set()
        if ano:
            anos_validos.add(str(ano))
        elif ano_inicio and ano_fim:
            anos_validos = set(str(a) for a in range(ano_inicio, ano_fim + 1))
        elif ano_inicio:
            # Ano inicial até o último disponível
            anos_validos = set(str(a) for a in range(ano_inicio, 2100))  # 2100: limite arbitrário
        
        filtrados = []
        for arq in arquivos:
            nome_arquivo = arq["nome"]
            
            # Verificar se o arquivo deve ser excluído por padrão
            if self._deve_excluir_arquivo(nome_arquivo):
                continue
            
            # Se não há filtro de ano, incluir o arquivo
            if not anos_validos:
                filtrados.append(arq)
                continue
            
            # Verificar se o arquivo está no ano desejado
            ano_arq = self._extrair_ano_arquivo(nome_arquivo)
            if ano_arq and ano_arq in anos_validos:
                filtrados.append(arq)
        
        logger.info(f"Filtrados {len(filtrados)} arquivos de {len(arquivos)} total (excluindo padrões _EST, ESTB, IGN, NI)")
        return filtrados

    def _destino_arquivo_zip(self, nome_arquivo: str) -> Path:
        """
        Retorna o caminho de destino correto para o arquivo zip, respeitando a estrutura files-zip/ANO/arquivo.zip
        """
        ano = self._extrair_ano_arquivo(nome_arquivo)
        if not ano:
            ano = "desconhecido"
        return Path("files-zip") / ano / Path(nome_arquivo).name



    def _verificar_arquivo_remoto(self, ftp: ftplib.FTP, nome_arquivo: str) -> Dict[str, Any]:
        """
        Obtém informações detalhadas do arquivo remoto.
        
        Args:
            ftp: Conexão FTP ativa
            nome_arquivo: Nome do arquivo remoto
            
        Returns:
            Dicionário com informações do arquivo (tamanho, data_modificacao, etc.)
        """
        info = {
            "tamanho": 0,
            "data_modificacao": None,
            "hash_md5": None
        }
        
        try:
            # Obter tamanho
            ftp.voidcmd('TYPE I')  # Modo binário
            info["tamanho"] = ftp.size(nome_arquivo)
            
            # Obter data de modificação
            try:
                # Tentar obter data de modificação via MDTM
                resposta = ftp.voidcmd(f'MDTM {nome_arquivo}')
                if resposta.startswith('213'):
                    # Formato: 213 20231201120000
                    data_str = resposta[4:].strip()
                    if len(data_str) >= 14:
                        ano = int(data_str[:4])
                        mes = int(data_str[4:6])
                        dia = int(data_str[6:8])
                        hora = int(data_str[8:10])
                        minuto = int(data_str[10:12])
                        segundo = int(data_str[12:14])
                        from datetime import datetime
                        info["data_modificacao"] = datetime(ano, mes, dia, hora, minuto, segundo)
            except:
                # Se MDTM não funcionar, usar LIST
                try:
                    itens = []
                    ftp.dir(nome_arquivo, lambda x: itens.append(x))
                    if itens:
                        # Parse da linha LIST para extrair data
                        linha = itens[0]
                        # Formato típico: "MM/DD/YYYY HH:MM tamanho nome"
                        partes = linha.split()
                        if len(partes) >= 6:
                            data_str = f"{partes[0]} {partes[1]} {partes[2]} {partes[3]}"
                            from datetime import datetime
                            try:
                                info["data_modificacao"] = datetime.strptime(data_str, "%m/%d/%Y %H:%M")
                            except:
                                pass
                except:
                    pass
            
            # Tentar obter hash MD5 se o servidor suportar
            try:
                resposta = ftp.voidcmd(f'MD5 {nome_arquivo}')
                if resposta.startswith('213'):
                    info["hash_md5"] = resposta[4:].strip()
            except:
                # Servidor não suporta MD5
                pass
                
        except Exception as e:
            logger.debug(f"Erro ao obter informações do arquivo remoto {nome_arquivo}: {e}")
        
        return info
    
    def _verificar_arquivo_local(self, caminho_arquivo: Path) -> Dict[str, Any]:
        """
        Obtém informações detalhadas do arquivo local.
        
        Args:
            caminho_arquivo: Caminho do arquivo local
            
        Returns:
            Dicionário com informações do arquivo (tamanho, data_modificacao, hash_md5)
        """
        info = {
            "tamanho": 0,
            "data_modificacao": None,
            "hash_md5": None
        }
        
        if not caminho_arquivo.exists():
            return info
        
        try:
            stat = caminho_arquivo.stat()
            info["tamanho"] = stat.st_size
            info["data_modificacao"] = datetime.fromtimestamp(stat.st_mtime)
            info["hash_md5"] = self.calcular_hash_arquivo(caminho_arquivo)
        except Exception as e:
            logger.debug(f"Erro ao obter informações do arquivo local {caminho_arquivo}: {e}")
        
        return info
    
    def _arquivo_precisa_baixar(self, nome_arquivo: str, info_remoto: Dict[str, Any]) -> bool:
        """
        Verifica se o arquivo precisa ser baixado comparando informações locais e remotas.
        
        Args:
            nome_arquivo: Nome do arquivo
            info_remoto: Informações do arquivo remoto
            
        Returns:
            True se o arquivo precisa ser baixado, False caso contrário
        """
        caminho_local = self._destino_arquivo_zip(nome_arquivo)
        info_local = self._verificar_arquivo_local(caminho_local)
        
        # Se o arquivo local não existe, precisa baixar
        if not caminho_local.exists():
            logger.info(f"Arquivo {nome_arquivo} não existe localmente")
            return True
        
        # Comparar tamanho
        if info_remoto["tamanho"] > 0 and info_local["tamanho"] != info_remoto["tamanho"]:
            logger.info(f"Arquivo {nome_arquivo} tem tamanho diferente (local: {info_local['tamanho']}, remoto: {info_remoto['tamanho']})")
            return True
        
        # Comparar hash MD5 se disponível
        if info_remoto["hash_md5"] and info_local["hash_md5"]:
            if info_local["hash_md5"] != info_remoto["hash_md5"]:
                logger.info(f"Arquivo {nome_arquivo} tem hash MD5 diferente")
                return True
        
        # Comparar data de modificação se disponível
        if info_remoto["data_modificacao"] and info_local["data_modificacao"]:
            if info_local["data_modificacao"] < info_remoto["data_modificacao"]:
                logger.info(f"Arquivo {nome_arquivo} é mais antigo que o remoto")
                return True
        
        logger.debug(f"Arquivo {nome_arquivo} está atualizado, pulando download")
        return False
    
    def baixar_arquivo(self, nome_arquivo: str) -> bool:
        """
        Baixa um arquivo do servidor FTP com tolerância a falhas.
        Salva em files-zip/ANO/arquivo.zip
        """
        caminho_destino = self._destino_arquivo_zip(nome_arquivo)
        
        # Criar diretórios intermediários se necessário (com lock para evitar conflitos)
        with self._dir_lock:
            caminho_destino.parent.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Iniciando download de {nome_arquivo}")
        
        tentativas = 0
        while tentativas < self.max_tentativas:
            tentativas += 1
            try:
                ftp = self._conectar_ftp()
                if not ftp:
                    if tentativas < self.max_tentativas:
                        logger.info(f"Aguardando {self.tempo_espera} segundos antes da próxima tentativa")
                        time.sleep(self.tempo_espera)
                        continue
                    else:
                        return False
                
                # Obter tamanho do arquivo para a barra de progresso
                tamanho_total = 0
                try:
                    ftp.voidcmd('TYPE I')  # Mudar para modo binário
                    tamanho_total = ftp.size(nome_arquivo)
                except:
                    pass
                
                # Criar uma barra de progresso individual (posição 1 para não conflitar com a principal)
                nome_arquivo_curto = nome_arquivo.split('/')[-1]
                if len(nome_arquivo_curto) > 40:
                    nome_arquivo_curto = nome_arquivo_curto[:37] + "..."
                
                with tqdm(
                    desc=nome_arquivo_curto,
                    total=tamanho_total,
                    unit='B',
                    unit_scale=True,
                    unit_divisor=1024,
                    position=1,
                    leave=False,
                    ncols=80
                ) as barra:
                    
                    # Callback para atualizar a barra de progresso
                    def callback(dados):
                        barra.update(len(dados))
                    
                    # Baixar o arquivo
                    with open(caminho_destino, 'wb') as arquivo_local:
                        ftp.retrbinary(f'RETR {nome_arquivo}', lambda dados: (arquivo_local.write(dados), callback(dados)))
                
                logger.info(f"Download de {nome_arquivo} concluído com sucesso")
                return True
                
            except Exception as e:
                logger.error(f"Erro no download de {nome_arquivo}: {e}. Tentativa {tentativas}/{self.max_tentativas}")
                
                if tentativas < self.max_tentativas:
                    logger.info(f"Aguardando {self.tempo_espera} segundos antes da próxima tentativa")
                    time.sleep(self.tempo_espera)
                else:
                    logger.error(f"Número máximo de tentativas excedido para {nome_arquivo}")
                    return False
            finally:
                try:
                    if ftp:
                        ftp.quit()
                except:
                    pass
        
        return False
    
    def sincronizar_arquivos(self, ano: Optional[int] = None, ano_inicio: Optional[int] = None, ano_fim: Optional[int] = None) -> Tuple[int, int, int]:
        """
        Sincroniza arquivos locais com o repositório remoto FTP usando download paralelo.
        Permite filtrar por ano, faixa de anos ou do ano X até o último disponível.
        """
        logger.info("Iniciando sincronização de arquivos com download paralelo")
        
        # Listar arquivos remotos e locais
        arquivos_remotos = self.listar_arquivos_remotos(ano, ano_inicio, ano_fim)
        arquivos_locais = self.listar_arquivos_locais()
        
        # Filtrar arquivos que precisam ser baixados
        arquivos_para_baixar = []
        arquivos_verificados = 0
        
        logger.info("Verificando arquivos que precisam ser baixados...")
        
        # Conectar ao FTP uma vez para verificar todos os arquivos
        ftp = self._conectar_ftp()
        if not ftp:
            logger.error("Não foi possível conectar ao FTP para verificação")
            return 0, 0, 0
        
        try:
            for arquivo_info in arquivos_remotos:
                nome_arquivo = arquivo_info.get("nome")
                arquivos_verificados += 1
                
                # Obter informações detalhadas do arquivo remoto
                info_remoto = self._verificar_arquivo_remoto(ftp, nome_arquivo)
                
                # Verificar se o arquivo precisa ser baixado
                if self._arquivo_precisa_baixar(nome_arquivo, info_remoto):
                    arquivos_para_baixar.append(nome_arquivo)
                
                # Log de progresso a cada 10 arquivos verificados
                if arquivos_verificados % 10 == 0:
                    logger.info(f"Verificados {arquivos_verificados}/{len(arquivos_remotos)} arquivos...")
        
        finally:
            try:
                ftp.quit()
            except:
                pass
        
        total = len(arquivos_remotos)
        baixados = 0
        falhas = 0
        
        logger.info(f"Verificação concluída: {len(arquivos_para_baixar)} de {total} arquivos precisam ser baixados")
        
        if not arquivos_para_baixar:
            logger.info("Todos os arquivos estão atualizados!")
            return total, 0, 0
        
        logger.info(f"Iniciando download paralelo de {len(arquivos_para_baixar)} arquivos com {self.max_workers} workers")
        
        # Barra de progresso principal
        with tqdm(
            total=len(arquivos_para_baixar),
            desc="Sincronizando arquivos",
            unit="arquivo",
            position=0,
            leave=True
        ) as pbar_principal:
            
            # Usar ThreadPoolExecutor para download paralelo
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submeter todos os downloads
                future_to_arquivo = {
                    executor.submit(self.baixar_arquivo, nome_arquivo): nome_arquivo 
                    for nome_arquivo in arquivos_para_baixar
                }
                
                # Processar resultados conforme completam
                for future in as_completed(future_to_arquivo):
                    nome_arquivo = future_to_arquivo[future]
                    try:
                        sucesso = future.result()
                        if sucesso:
                            baixados += 1
                            pbar_principal.set_postfix({
                                'Baixados': baixados,
                                'Falhas': falhas,
                                'Atual': nome_arquivo.split('/')[-1][:30] + '...' if len(nome_arquivo) > 30 else nome_arquivo
                            })
                            logger.info(f"Download concluído: {nome_arquivo}")
                        else:
                            falhas += 1
                            pbar_principal.set_postfix({
                                'Baixados': baixados,
                                'Falhas': falhas,
                                'Atual': nome_arquivo.split('/')[-1][:30] + '...' if len(nome_arquivo) > 30 else nome_arquivo
                            })
                            logger.error(f"Falha no download: {nome_arquivo}")
                        
                        # Atualizar barra de progresso
                        pbar_principal.update(1)
                        
                    except Exception as e:
                        falhas += 1
                        pbar_principal.set_postfix({
                            'Baixados': baixados,
                            'Falhas': falhas,
                            'Atual': nome_arquivo.split('/')[-1][:30] + '...' if len(nome_arquivo) > 30 else nome_arquivo
                        })
                        logger.error(f"Exceção no download de {nome_arquivo}: {e}")
                        pbar_principal.update(1)
        
        logger.info(f"Sincronização concluída: {baixados} arquivos baixados, {falhas} falhas")
        return total, baixados, falhas 