import os
import py7zr
import shutil
from pathlib import Path
import argparse
from tqdm import tqdm
import time
import dask.dataframe as dd

import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import re
import unicodedata
import ftplib
import requests
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from urllib.parse import urljoin
import threading
import psutil
import logging
from datetime import datetime
import json
import asyncio
import concurrent.futures
import queue
import threading

# ==========================================
# SISTEMA DE CONTROLE DE ERROS
# ==========================================

class ControladorErros:
    """
    Classe para rastrear e registrar erros durante o processamento.
    """
    def __init__(self):
        self.erros_por_ano = {}
        self.anos_com_falhas = set()
        self.lock = threading.Lock()
    
    def registrar_erro(self, ano, etapa, arquivo, erro):
        """
        Registra um erro durante o processamento.
        
        Parâmetros:
        - ano: Ano onde ocorreu o erro
        - etapa: Etapa do processamento (download, descompactacao, conversao, consolidacao, teste)
        - arquivo: Nome do arquivo que causou o erro
        - erro: Descrição do erro
        """
        with self.lock:
            if ano not in self.erros_por_ano:
                self.erros_por_ano[ano] = {
                    'download': [],
                    'descompactacao': [],
                    'conversao': [],
                    'consolidacao': [],
                    'teste': []
                }
            
            # Garantir que a etapa existe
            if etapa not in self.erros_por_ano[ano]:
                self.erros_por_ano[ano][etapa] = []
            
            self.erros_por_ano[ano][etapa].append({
                'arquivo': arquivo,
                'erro': str(erro),
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
            
            # Marcar ano como com falhas
            self.anos_com_falhas.add(ano)
            
            # Log do erro
            logger = logging.getLogger(__name__)
            logger.error(f"ERRO registrado - Ano: {ano}, Etapa: {etapa}, Arquivo: {arquivo}, Erro: {erro}")
    
    def ano_tem_erros(self, ano):
        """Verifica se um ano específico teve erros."""
        return ano in self.anos_com_falhas
    
    def get_erros_ano(self, ano):
        """Retorna todos os erros de um ano específico."""
        return self.erros_por_ano.get(ano, {})
    
    def salvar_relatorio_erros(self):
        """
        Registra um relatório detalhado dos erros apenas no log.
        """
        if not self.erros_por_ano:
            return None
            
        logger = logging.getLogger(__name__)
        
        # Gerar relatório apenas no log
        logger.info("="*80)
        logger.info("RELATÓRIO DE ERROS - PROCESSAMENTO RAIS")
        logger.info("="*80)
        logger.info(f"Total de anos com erros: {len(self.anos_com_falhas)}")
        logger.info(f"Anos afetados: {', '.join(map(str, sorted(self.anos_com_falhas)))}")
        logger.info("="*80)
        
        for ano in sorted(self.anos_com_falhas):
            logger.info(f"ANO {ano}:")
            logger.info("-" * 40)
            
            for etapa, erros in self.erros_por_ano[ano].items():
                if erros:
                    logger.info(f"{etapa.upper()}: {len(erros)} erros")
                    for erro in erros:
                        logger.info(f"  • {erro['timestamp']} - {erro['arquivo']}: {erro['erro']}")
            
            logger.info("="*80)
        
        return None

def gerar_relatorio_problemas_logs():
    """
    Exibe um resumo dos problemas encontrados nos logs.
    """
    print("❌ Funcionalidade removida: Os relatórios de erro agora são registrados apenas nos logs.")
    print("   Para analisar problemas, consulte os arquivos de log na pasta 'logs/'.")
    print("   Use: --log-level DEBUG para obter informações mais detalhadas durante a execução.")

def gerar_relatorio_final_arquivos_pulados():
    """
    Gera um relatório final detalhado dos arquivos que foram pulados durante o processamento.
    """
    logger = logging.getLogger(__name__)
    
    if not controlador_erros.anos_com_falhas:
        print("\n🎉 RELATÓRIO FINAL: Nenhum arquivo foi pulado durante o processamento!")
        print("✅ Todos os arquivos foram processados com sucesso.")
        return
    
    print("\n" + "="*80)
    print("📋 RELATÓRIO FINAL - ARQUIVOS PULADOS")
    print("="*80)
    
    total_arquivos_pulados = 0
    anos_afetados = sorted(controlador_erros.anos_com_falhas)
    
    print(f"⚠️  RESUMO GERAL:")
    print(f"   • Anos com arquivos pulados: {len(anos_afetados)}")
    print(f"   • Anos afetados: {', '.join(map(str, anos_afetados))}")
    print()
    
    # Detalhes por ano
    for ano in anos_afetados:
        erros_ano = controlador_erros.get_erros_ano(ano)
        
        print(f"📅 ANO {ano}:")
        print("-" * 50)
        
        # Contar arquivos pulados por etapa
        arquivos_pulados_ano = 0
        
        for etapa, lista_erros in erros_ano.items():
            if lista_erros:
                print(f"   🔧 {etapa.upper()}:")
                for erro in lista_erros:
                    arquivo = erro['arquivo']
                    motivo = erro['erro']
                    timestamp = erro['timestamp']
                    
                    # Identificar se foi pulado ou erro fatal
                    if "PULANDO" in motivo or "pulando" in motivo.lower():
                        status = "⏭️  PULADO"
                        arquivos_pulados_ano += 1
                    else:
                        status = "❌ ERRO"
                    
                    print(f"      {status}: {arquivo}")
                    print(f"         Motivo: {motivo}")
                    print(f"         Horário: {timestamp}")
                    print()
        
        total_arquivos_pulados += arquivos_pulados_ano
        print(f"   📊 Total de arquivos pulados no ano {ano}: {arquivos_pulados_ano}")
        print("="*80)
    
    print(f"\n📊 ESTATÍSTICAS FINAIS:")
    print(f"   • Total de arquivos pulados: {total_arquivos_pulados}")
    print(f"   • Anos afetados: {len(anos_afetados)}")
    print(f"   • Percentual de anos com problemas: {len(anos_afetados)/len(anos_afetados)*100:.1f}%")
    
    # Recomendações
    print(f"\n💡 RECOMENDAÇÕES:")
    if total_arquivos_pulados > 0:
        print("   1. Os arquivos pulados geralmente têm problemas no servidor FTP oficial")
        print("   2. Você pode tentar executar novamente em outro momento")
        print("   3. O processamento continuou normalmente com os arquivos válidos")
        print("   4. Consulte os logs detalhados na pasta 'logs/' para mais informações")
        
        # Salvar relatório em arquivo
        pasta_logs = Path('logs')
        pasta_logs.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y_%m_%d_%H%M%S')
        arquivo_relatorio = pasta_logs / f'arquivos_pulados_{timestamp}.txt'
        
        with open(arquivo_relatorio, 'w', encoding='utf-8') as f:
            f.write("="*80 + "\n")
            f.write("RELATÓRIO FINAL - ARQUIVOS PULADOS\n")
            f.write("="*80 + "\n")
            f.write(f"Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total de arquivos pulados: {total_arquivos_pulados}\n")
            f.write(f"Anos afetados: {len(anos_afetados)}\n")
            f.write(f"Lista de anos: {', '.join(map(str, anos_afetados))}\n")
            f.write("="*80 + "\n\n")
            
            for ano in anos_afetados:
                erros_ano = controlador_erros.get_erros_ano(ano)
                f.write(f"ANO {ano}:\n")
                f.write("-" * 50 + "\n")
                
                for etapa, lista_erros in erros_ano.items():
                    if lista_erros:
                        f.write(f"\n{etapa.upper()}:\n")
                        for erro in lista_erros:
                            arquivo = erro['arquivo']
                            motivo = erro['erro']
                            timestamp = erro['timestamp']
                            
                            status = "PULADO" if "PULANDO" in motivo or "pulando" in motivo.lower() else "ERRO"
                            f.write(f"  {status}: {arquivo}\n")
                            f.write(f"    Motivo: {motivo}\n")
                            f.write(f"    Horário: {timestamp}\n\n")
                
                f.write("="*80 + "\n\n")
        
        print(f"\n💾 Relatório detalhado salvo em: {arquivo_relatorio}")
    else:
        print("   ✅ Nenhum arquivo foi pulado - processamento 100% bem-sucedido!")
    
    print("="*80)
    
    logger.info(f"Relatório final gerado: {total_arquivos_pulados} arquivos pulados em {len(anos_afetados)} anos")

def verificar_integridade_arquivo_7z(arquivo_7z):
    """
    Verifica a integridade de um arquivo 7z específico de forma robusta.
    
    Parâmetros:
    - arquivo_7z: Path para o arquivo 7z
    
    Retorna:
    - tuple: (is_valid, error_message)
    """
    try:
        # Verificar se o arquivo existe
        if not arquivo_7z.exists():
            return False, 'Arquivo não encontrado'
        
        # Verificar se o arquivo não está vazio
        tamanho_arquivo = arquivo_7z.stat().st_size
        if tamanho_arquivo == 0:
            return False, 'Arquivo vazio (0 bytes)'
        
        # Verificar se o arquivo é muito pequeno (menos de 1KB é suspeito)
        if tamanho_arquivo < 1024:
            return False, f'Arquivo muito pequeno ({tamanho_arquivo} bytes)'
        
        # Tentar abrir o arquivo 7z
        with py7zr.SevenZipFile(arquivo_7z, mode='r') as archive:
            # Verificar se consegue listar o conteúdo
            file_list = archive.getnames()
            
            if not file_list:
                return False, 'Arquivo 7z sem conteúdo interno'
            
            # Verificar se há arquivos TXT no conteúdo
            arquivos_txt = [f for f in file_list if f.lower().endswith('.txt')]
            if not arquivos_txt:
                return False, 'Nenhum arquivo TXT encontrado no conteúdo'
            
            # Tentar testar a integridade (verificação básica)
            try:
                archive.testzip()
                return True, None
            except Exception as e:
                return False, f'Falha no teste de integridade: {str(e)}'
                
    except py7zr.Bad7zFile:
        return False, 'Arquivo 7z corrompido ou inválido'
        
    except PermissionError:
        return False, 'Sem permissão para acessar o arquivo'
        
    except FileNotFoundError:
        return False, 'Arquivo não encontrado'
        
    except Exception as e:
        # Verificar se é erro de senha
        if 'password' in str(e).lower():
            return False, 'Arquivo 7z protegido por senha'
        return False, f'Erro inesperado: {str(e)}'

def baixar_arquivo_individual(ano, nome_arquivo, pasta_destino, sobrescrever=True):
    """
    Baixa um arquivo individual do FTP com tolerância a erros.
    Implementa múltiplas tentativas com tempo de espera entre elas.
    
    Parâmetros:
    - ano: Ano do arquivo
    - nome_arquivo: Nome do arquivo no FTP
    - pasta_destino: Pasta onde salvar o arquivo
    - sobrescrever: Se deve sobrescrever arquivo existente
    
    Retorna:
    - True se sucesso, False caso contrário
    """
    logger = logging.getLogger(__name__)
    
    arquivo_local = pasta_destino / nome_arquivo
    
    logger.info(f"🔄 Baixando arquivo corrompido: {nome_arquivo} do ano {ano}")
    
    # Configurações de tolerância a erros
    max_tentativas = config_tolerancia['max_tentativas']
    tempo_espera_base = config_tolerancia['tempo_espera_base']
    
    for tentativa in range(1, max_tentativas + 1):
        logger.debug(f"Tentativa {tentativa}/{max_tentativas} para re-download de {nome_arquivo} do ano {ano}")
        
        ftp = None
        try:
            # Conectar ao FTP
            ftp = conectar_ftp()
            if not ftp:
                raise Exception("Falha ao conectar ao FTP")
            
            # Navegar para o diretório do ano
            ftp.cwd(str(ano))
            
            # Obter tamanho do arquivo para barra de progresso
            try:
                size = ftp.size(nome_arquivo)
            except:
                size = None
            
            inicio_download = time.time()
            
            # Baixar o arquivo com barra de progresso
            desc_tentativa = f"📥 Re-baixando {nome_arquivo}" if tentativa == 1 else f"🔄 Re-baixando {nome_arquivo} (tentativa {tentativa})"
            
            with open(arquivo_local, 'wb') as f:
                if size:
                    with tqdm(total=size, unit='B', unit_scale=True, 
                             desc=desc_tentativa, leave=False) as pbar:
                        def callback(data):
                            f.write(data)
                            pbar.update(len(data))
                        ftp.retrbinary(f'RETR {nome_arquivo}', callback)
                else:
                    ftp.retrbinary(f'RETR {nome_arquivo}', f.write)
            
            tempo_download = time.time() - inicio_download
            tamanho_final = arquivo_local.stat().st_size
            velocidade = tamanho_final / tempo_download / (1024*1024)  # MB/s
            
            ftp.quit()
            
            # Validar se o download foi bem-sucedido
            if size and tamanho_final != size:
                raise Exception(f"Tamanho do arquivo incorreto: esperado {size}, obtido {tamanho_final}")
            
            if tamanho_final == 0:
                raise Exception("Arquivo baixado está vazio")
            
            # Re-download bem-sucedido
            if tentativa > 1:
                logger.info(f"✅ Re-download concluído na tentativa {tentativa}: {nome_arquivo} ({tamanho_final/1024/1024:.1f}MB em {tempo_download:.1f}s, {velocidade:.1f}MB/s)")
            else:
                logger.info(f"✅ Re-download concluído: {nome_arquivo} ({tamanho_final/1024/1024:.1f}MB em {tempo_download:.1f}s, {velocidade:.1f}MB/s)")
            
            return True
            
        except Exception as e:
            if ftp:
                try:
                    ftp.quit()
                except:
                    pass
            
            # Remover arquivo parcial se existir
            if arquivo_local.exists():
                try:
                    arquivo_local.unlink()
                except:
                    pass
            
            error_msg = str(e)
            logger.warning(f"Tentativa {tentativa}/{max_tentativas} falhou para re-download de {nome_arquivo}: {error_msg}")
            
            # Se não é a última tentativa, aguardar antes de tentar novamente
            if tentativa < max_tentativas:
                tempo_espera = tempo_espera_base * tentativa  # Backoff exponencial
                logger.info(f"Aguardando {tempo_espera}s antes da próxima tentativa...")
                time.sleep(tempo_espera)
            else:
                # Última tentativa falhou
                logger.error(f"Todas as {max_tentativas} tentativas falharam para re-download de {nome_arquivo}: {error_msg}")
                return False
    
    # Não deveria chegar aqui, mas por segurança
    return False

def verificar_integridade_arquivos_7z():
    """
    Verifica a integridade de todos os arquivos 7z disponíveis.
    """
    pasta_zip = Path('dados-abertos-zip')
    
    if not pasta_zip.exists():
        print("❌ Pasta 'dados-abertos-zip' não encontrada.")
        print("   Execute primeiro: python main.py --modo baixar")
        return
    
    print("🔍 VERIFICAÇÃO PREVENTIVA DE ARQUIVOS 7Z")
    print("="*60)
    
    # Encontrar todas as pastas de anos
    pastas_anos = [d for d in pasta_zip.iterdir() if d.is_dir()]
    
    if not pastas_anos:
        print("❌ Nenhuma pasta de ano encontrada em 'dados-abertos-zip'")
        return
    
    anos_ordenados = sorted(pastas_anos, key=lambda x: x.name)
    print(f"📂 Encontradas {len(anos_ordenados)} pastas de anos: {', '.join([p.name for p in anos_ordenados])}")
    print()
    
    # Estatísticas globais
    total_arquivos = 0
    arquivos_ok = 0
    arquivos_com_problema = 0
    problemas_por_ano = {}
    
    for pasta_ano in anos_ordenados:
        ano = pasta_ano.name
        print(f"📅 Verificando ano {ano}...")
        
        # Encontrar arquivos 7z válidos (excluindo estabelecimentos)
        arquivos_7z = list(pasta_ano.glob('*.7z'))
        arquivos_7z_filtrados = [
            arq for arq in arquivos_7z 
            if not any(termo in arq.stem.upper() for termo in ["_EST", "ESTB", "IGN", "NI"])
        ]
        
        if not arquivos_7z_filtrados:
            print(f"   ⚠️  Nenhum arquivo 7z válido encontrado")
            continue
        
        print(f"   📦 Verificando {len(arquivos_7z_filtrados)} arquivos...")
        
        problemas_ano = []
        arquivos_ok_ano = 0
        
        # Verificar cada arquivo
        for arquivo_7z in arquivos_7z_filtrados:
            total_arquivos += 1
            
            try:
                # Tentar abrir o arquivo 7z
                with py7zr.SevenZipFile(arquivo_7z, mode='r') as archive:
                    # Verificar se consegue listar o conteúdo
                    file_list = archive.getnames()
                    
                    if not file_list:
                        problemas_ano.append({
                            'arquivo': arquivo_7z.name,
                            'problema': 'Arquivo vazio (sem conteúdo)',
                            'severidade': 'ERROR'
                        })
                        arquivos_com_problema += 1
                        continue
                    
                    # Verificar se há arquivos TXT no conteúdo
                    arquivos_txt = [f for f in file_list if f.lower().endswith('.txt')]
                    if not arquivos_txt:
                        problemas_ano.append({
                            'arquivo': arquivo_7z.name,
                            'problema': 'Nenhum arquivo TXT encontrado no conteúdo',
                            'severidade': 'WARNING'
                        })
                        # Não incrementa arquivos_com_problema pois é só warning
                    
                    # Tentar testar a integridade (verificação básica)
                    try:
                        archive.testzip()
                        arquivos_ok_ano += 1
                        arquivos_ok += 1
                    except Exception as e:
                        problemas_ano.append({
                            'arquivo': arquivo_7z.name,
                            'problema': f'Falha no teste de integridade: {str(e)}',
                            'severidade': 'ERROR'
                        })
                        arquivos_com_problema += 1
                        
            except py7zr.Bad7zFile:
                problemas_ano.append({
                    'arquivo': arquivo_7z.name,
                    'problema': 'Arquivo 7z corrompido ou inválido',
                    'severidade': 'ERROR'
                })
                arquivos_com_problema += 1
                
            except FileNotFoundError:
                problemas_ano.append({
                    'arquivo': arquivo_7z.name,
                    'problema': 'Arquivo não encontrado',
                    'severidade': 'ERROR'
                })
                arquivos_com_problema += 1
                
            except Exception as e:
                problemas_ano.append({
                    'arquivo': arquivo_7z.name,
                    'problema': f'Erro inesperado: {str(e)}',
                    'severidade': 'ERROR'
                })
                arquivos_com_problema += 1
        
        # Mostrar resultado do ano
        if problemas_ano:
            problemas_por_ano[ano] = problemas_ano
            
            # Contar por severidade
            erros = [p for p in problemas_ano if p['severidade'] == 'ERROR']
            warnings = [p for p in problemas_ano if p['severidade'] == 'WARNING']
            
            if erros:
                print(f"   ❌ {len(erros)} arquivos com ERROS:")
                for problema in erros:
                    print(f"      • {problema['arquivo']}: {problema['problema']}")
            
            if warnings:
                print(f"   ⚠️  {len(warnings)} arquivos com AVISOS:")
                for problema in warnings:
                    print(f"      • {problema['arquivo']}: {problema['problema']}")
        else:
            print(f"   ✅ Todos os {arquivos_ok_ano} arquivos estão íntegros")
        
        print()
    
    # Resumo final
    print("="*60)
    print("📊 RESUMO DA VERIFICAÇÃO:")
    print(f"   • Total de arquivos verificados: {total_arquivos}")
    print(f"   • Arquivos íntegros: {arquivos_ok} ({arquivos_ok/total_arquivos*100:.1f}%)")
    print(f"   • Arquivos com problemas: {arquivos_com_problema} ({arquivos_com_problema/total_arquivos*100:.1f}%)")
    
    if problemas_por_ano:
        print(f"   • Anos com problemas: {len(problemas_por_ano)}")
        print(f"   • Anos afetados: {', '.join(sorted(problemas_por_ano.keys()))}")
    
    # Salvar relatório detalhado
    if problemas_por_ano:
        pasta_logs = Path('logs')
        pasta_logs.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y_%m_%d_%H%M%S')
        arquivo_relatorio = pasta_logs / f'verificacao_7z_{timestamp}.txt'
        
        with open(arquivo_relatorio, 'w', encoding='utf-8') as f:
            f.write("="*80 + "\n")
            f.write("RELATÓRIO DE VERIFICAÇÃO PREVENTIVA - ARQUIVOS 7Z\n")
            f.write("="*80 + "\n")
            f.write(f"Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total de arquivos verificados: {total_arquivos}\n")
            f.write(f"Arquivos íntegros: {arquivos_ok}\n")
            f.write(f"Arquivos com problemas: {arquivos_com_problema}\n")
            f.write(f"Anos com problemas: {len(problemas_por_ano)}\n")
            f.write("="*80 + "\n\n")
            
            for ano, problemas in sorted(problemas_por_ano.items()):
                f.write(f"ANO {ano}:\n")
                f.write("-" * 40 + "\n")
                
                erros = [p for p in problemas if p['severidade'] == 'ERROR']
                warnings = [p for p in problemas if p['severidade'] == 'WARNING']
                
                if erros:
                    f.write(f"\nERROS ({len(erros)} arquivos):\n")
                    for problema in erros:
                        f.write(f"  • {problema['arquivo']}\n")
                        f.write(f"    Problema: {problema['problema']}\n")
                
                if warnings:
                    f.write(f"\nAVISOS ({len(warnings)} arquivos):\n")
                    for problema in warnings:
                        f.write(f"  • {problema['arquivo']}\n")
                        f.write(f"    Problema: {problema['problema']}\n")
                
                f.write("\n" + "="*80 + "\n\n")
        
        print(f"\n💾 Relatório detalhado salvo em: {arquivo_relatorio}")
    else:
        print("\n✅ Nenhum problema encontrado! Todos os arquivos estão íntegros.")

# Instância global do controlador de erros
controlador_erros = ControladorErros()

# Variável global para controlar tempos
tempos_execucao = {}

# Configurações globais de tolerância a erros
config_tolerancia = {
    'max_tentativas': 3,
    'tempo_espera_base': 5
}

def log_and_write(message, level='info', tqdm_write=True):
    """
    Função utilitária para escrever mensagens tanto no log quanto no tqdm.
    
    Parâmetros:
    - message: Mensagem a ser escrita
    - level: Nível do log ('debug', 'info', 'warning', 'error', 'critical')
    - tqdm_write: Se True, escreve também via tqdm.write()
    """
    logger = logging.getLogger(__name__)
    
    # Escrever no log
    if level == 'debug':
        logger.debug(message)
    elif level == 'info':
        logger.info(message)
    elif level == 'warning':
        logger.warning(message)
    elif level == 'error':
        logger.error(message)
    elif level == 'critical':
        logger.critical(message)
    
    # Escrever no tqdm se solicitado
    if tqdm_write:
        from tqdm import tqdm
        tqdm.write(message)

def registrar_erro_completo(ano, etapa, arquivo, erro, exception=None):
    """
    Registra um erro completo com traceback nos logs e no controlador de erros.
    
    Parâmetros:
    - ano: Ano onde ocorreu o erro
    - etapa: Etapa do processamento
    - arquivo: Nome do arquivo que causou o erro
    - erro: Descrição do erro
    - exception: Exceção original (opcional, para traceback)
    """
    logger = logging.getLogger(__name__)
    
    # Registrar no controlador de erros
    controlador_erros.registrar_erro(ano, etapa, arquivo, str(erro))
    
    # Log básico do erro
    logger.error(f"ERRO - Ano: {ano}, Etapa: {etapa}, Arquivo: {arquivo}")
    logger.error(f"Descrição: {erro}")
    
    # Log do traceback se disponível
    if exception:
        import traceback
        logger.error(f"Traceback completo:")
        logger.error(traceback.format_exc())
    
    # Mensagem formatada para o usuário
    return f"⏭️  PULANDO {arquivo}: {erro}"

def iniciar_tempo(etapa):
    """Inicia a contagem de tempo para uma etapa."""
    tempos_execucao[etapa] = {'inicio': time.time()}
    logger = logging.getLogger(__name__)
    logger.info(f"⏱️  Iniciando cronômetro para: {etapa}")

def finalizar_tempo(etapa):
    """Finaliza a contagem de tempo para uma etapa e retorna o tempo decorrido."""
    if etapa in tempos_execucao:
        tempo_decorrido = time.time() - tempos_execucao[etapa]['inicio']
        tempos_execucao[etapa]['duracao'] = tempo_decorrido
        
        logger = logging.getLogger(__name__)
        logger.info(f"⏱️  {etapa} concluída em: {formatar_tempo(tempo_decorrido)}")
        
        return tempo_decorrido
    return 0

def formatar_tempo(segundos):
    """Formata segundos em formato legível (HH:MM:SS)."""
    horas = int(segundos // 3600)
    minutos = int((segundos % 3600) // 60)
    segundos_restantes = int(segundos % 60)
    
    if horas > 0:
        return f"{horas:02d}h{minutos:02d}m{segundos_restantes:02d}s"
    elif minutos > 0:
        return f"{minutos:02d}m{segundos_restantes:02d}s"
    else:
        return f"{segundos_restantes}s"

def mostrar_resumo_tempos():
    """Mostra um resumo dos tempos de execução de todas as etapas."""
    logger = logging.getLogger(__name__)
    
    print("\n" + "="*60)
    print("📊 RESUMO DOS TEMPOS DE EXECUÇÃO")
    print("="*60)
    
    tempo_total = 0
    for etapa, dados in tempos_execucao.items():
        if 'duracao' in dados:
            duracao = dados['duracao']
            tempo_total += duracao
            print(f"⏱️  {etapa:<30}: {formatar_tempo(duracao)}")
            logger.info(f"Tempo da etapa {etapa}: {formatar_tempo(duracao)}")
    
    print("-" * 60)
    print(f"⏱️  {'TEMPO TOTAL':<30}: {formatar_tempo(tempo_total)}")
    print("="*60)
    
    logger.info(f"TEMPO TOTAL DE EXECUÇÃO: {formatar_tempo(tempo_total)}")
    
    return tempo_total

def configurar_logging(nivel_console='INFO'):
    """
    Configura o sistema de logging para arquivo e console.
    
    Parâmetros:
    - nivel_console: Nível de log para o console ('DEBUG', 'INFO', 'WARNING', 'ERROR')
    
    Cria um arquivo de log diário na pasta 'logs' com formato:
    rais_YYYY_MM_DD.log
    """
    # Criar pasta de logs se não existir
    pasta_logs = Path('logs')
    if not pasta_logs.exists():
        pasta_logs.mkdir(parents=True)
    
    # Nome do arquivo baseado na data e hora atual (incluindo segundos)
    data_hora_atual = datetime.now().strftime('%Y_%m_%d_%H%M%S')
    arquivo_log = pasta_logs / f'rais_{data_hora_atual}.log'
    
    # Configurar formato dos logs
    formato_log = '%(asctime)s | %(levelname)-8s | %(funcName)-20s | %(message)s'
    formato_data = '%Y-%m-%d %H:%M:%S'
    
    # Mapear string para nível do logging
    niveis_log = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }
    
    nivel_console_num = niveis_log.get(nivel_console.upper(), logging.INFO)
    
    # Configurar logging
    logging.basicConfig(
        level=logging.DEBUG,  # Nível do root logger (mais baixo para capturar tudo)
        format=formato_log,
        datefmt=formato_data,
        handlers=[
            # Handler para arquivo (todos os níveis)
            logging.FileHandler(arquivo_log, mode='a', encoding='utf-8'),
            # Handler para console (nível configurável)
            logging.StreamHandler()
        ],
        force=True  # Forçar reconfiguração se já estiver configurado
    )
    
    # Configurar nível do console conforme solicitado
    console_handler = logging.getLogger().handlers[1]
    console_handler.setLevel(nivel_console_num)
    
    # Log inicial
    logger = logging.getLogger(__name__)
    logger.info("="*80)
    logger.info(f"RAIS - Sistema de Processamento Iniciado")
    logger.info(f"Arquivo de log: {arquivo_log}")
    logger.info(f"Nível do console: {nivel_console.upper()}")
    logger.info(f"Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*80)
    
    return logger

def calcular_workers_otimizados():
    """
    Calcula valores otimizados de workers baseado nos recursos da máquina.
    
    Retorna:
    - Tupla com (workers_download, workers_extract, workers_convert)
    """
    logger = logging.getLogger(__name__)
    
    # Obter informações do sistema
    cpu_count = psutil.cpu_count(logical=True) or 4  # CPUs lógicos (com hyperthreading)
    cpu_physical = psutil.cpu_count(logical=False) or cpu_count // 2  # CPUs físicos
    memoria_gb = psutil.virtual_memory().total / (1024**3)  # RAM em GB
    
    logger.info(f"Recursos do sistema detectados:")
    logger.info(f"  - CPUs físicos: {cpu_physical}")
    logger.info(f"  - CPUs lógicos: {cpu_count}")
    logger.info(f"  - Memória RAM: {memoria_gb:.1f} GB")
    
    print(f"Recursos detectados:")
    print(f"  - CPUs físicos: {cpu_physical}")
    print(f"  - CPUs lógicos: {cpu_count}")
    print(f"  - Memória RAM: {memoria_gb:.1f} GB")
    
    # Calcular workers otimizados
    # Download: I/O bound, pode usar mais threads
    workers_download = min(8, max(4, cpu_count))
    
    # Extração: CPU bound mas com I/O, usar CPU físicos + margem
    workers_extract = min(8, max(2, cpu_physical + 2))
    
    # Conversão: Memory bound, mais conservador
    # Usar menos workers se RAM for limitada
    if memoria_gb < 8:
        workers_convert = 1
    elif memoria_gb < 16:
        workers_convert = 2
    elif memoria_gb < 32:
        workers_convert = max(2, min(4, cpu_physical))
    else:
        workers_convert = max(2, min(6, cpu_physical))
    
    logger.info(f"Workers otimizados calculados:")
    logger.info(f"  - Download: {workers_download} (I/O intensivo)")
    logger.info(f"  - Extração: {workers_extract} (CPU + I/O)")
    logger.info(f"  - Conversão: {workers_convert} (memória intensiva)")
    
    print(f"Workers otimizados calculados:")
    print(f"  - Download: {workers_download} (I/O intensivo)")
    print(f"  - Extração: {workers_extract} (CPU + I/O)")
    print(f"  - Conversão: {workers_convert} (memória intensiva)")
    print()
    
    return workers_download, workers_extract, workers_convert

def verificar_arquivo_extraido(arquivo_7z, pasta_destino):
    """
    Verifica se o arquivo já foi extraído verificando a existência
    de arquivos com o mesmo nome base na pasta de destino.
    
    Retorna True se já foi extraído, False caso contrário.
    """
    nome_base = arquivo_7z.stem
    
    # Verifica se existe pelo menos um arquivo com o mesmo nome base na pasta destino
    arquivos_extraidos = list(pasta_destino.glob(f"{nome_base}*"))
    return len(arquivos_extraidos) > 0

def conectar_ftp():
    """
    Conecta ao servidor FTP da RAIS.
    
    Retorna:
    - Objeto FTP conectado ou None se houver erro
    """
    try:
        ftp = ftplib.FTP('ftp.mtps.gov.br')
        ftp.login()  # Login anônimo
        ftp.cwd('/pdet/microdados/RAIS/')
        return ftp
    except Exception as e:
        print(f"Erro ao conectar ao FTP: {e}")
        return None

def listar_anos_disponiveis_ftp():
    """
    Lista os anos disponíveis no servidor FTP.
    
    Retorna:
    - Lista de anos disponíveis
    """
    ftp = conectar_ftp()
    if not ftp:
        return []
    
    try:
        # Listar diretórios/arquivos
        items = ftp.nlst()
        anos = []
        
        for item in items:
            # Procurar por padrões de ano (ex: 2015, 2016, etc.)
            if item.isdigit() and len(item) == 4:
                anos.append(int(item))
        
        ftp.quit()
        return sorted(anos)
    except Exception as e:
        print(f"Erro ao listar anos disponíveis: {e}")
        if ftp:
            ftp.quit()
        return []

def baixar_arquivo_ftp(args):
    """
    Baixa um arquivo específico do FTP com tolerância a erros.
    Implementa múltiplas tentativas com tempo de espera entre elas.
    
    Parâmetros:
    - args: tupla contendo (ano, nome_arquivo, pasta_destino, sobrescrever)
    
    Retorna:
    - String com resultado da operação
    """
    ano, nome_arquivo, pasta_destino, sobrescrever = args
    max_tentativas = config_tolerancia['max_tentativas']
    tempo_espera_base = config_tolerancia['tempo_espera_base']
    logger = logging.getLogger(__name__)
    
    arquivo_local = pasta_destino / nome_arquivo
    
    # Verificar se o arquivo já existe
    if arquivo_local.exists() and not sobrescrever:
        logger.debug(f"Arquivo {nome_arquivo} já existe localmente, pulando download")
        return f"⏭️  Pulando {nome_arquivo} - já existe localmente"
    
    for tentativa in range(1, max_tentativas + 1):
        logger.debug(f"Tentativa {tentativa}/{max_tentativas} para download de {nome_arquivo} do ano {ano}")
        
        ftp = None
        try:
            # Conectar ao FTP
            ftp = conectar_ftp()
            if not ftp:
                raise Exception("Falha ao conectar ao FTP")
            
            # Navegar para o diretório do ano
            ftp.cwd(str(ano))
            
            # Obter tamanho do arquivo para barra de progresso
            try:
                size = ftp.size(nome_arquivo)
                logger.debug(f"Tamanho do arquivo {nome_arquivo}: {size} bytes")
            except:
                size = None
                logger.warning(f"Não foi possível obter o tamanho do arquivo {nome_arquivo}")
            
            inicio_download = time.time()
            
            # Baixar o arquivo com barra de progresso
            desc_tentativa = f"📥 {nome_arquivo}" if tentativa == 1 else f"🔄 {nome_arquivo} (tentativa {tentativa})"
            
            with open(arquivo_local, 'wb') as f:
                if size:
                    with tqdm(total=size, unit='B', unit_scale=True, 
                             desc=desc_tentativa, leave=False) as pbar:
                        def callback(data):
                            f.write(data)
                            pbar.update(len(data))
                        ftp.retrbinary(f'RETR {nome_arquivo}', callback)
                else:
                    ftp.retrbinary(f'RETR {nome_arquivo}', f.write)
            
            tempo_download = time.time() - inicio_download
            tamanho_final = arquivo_local.stat().st_size
            velocidade = tamanho_final / tempo_download / (1024*1024)  # MB/s
            
            ftp.quit()
            
            # Validar se o download foi bem-sucedido
            if size and tamanho_final != size:
                raise Exception(f"Tamanho do arquivo incorreto: esperado {size}, obtido {tamanho_final}")
            
            if tamanho_final == 0:
                raise Exception("Arquivo baixado está vazio")
            
            # Download bem-sucedido
            if tentativa > 1:
                logger.info(f"Download concluído na tentativa {tentativa}: {nome_arquivo} ({tamanho_final/1024/1024:.1f}MB em {tempo_download:.1f}s, {velocidade:.1f}MB/s)")
                return f"✅ {nome_arquivo} baixado com sucesso (tentativa {tentativa})"
            else:
                logger.info(f"Download concluído: {nome_arquivo} ({tamanho_final/1024/1024:.1f}MB em {tempo_download:.1f}s, {velocidade:.1f}MB/s)")
                return f"✅ {nome_arquivo} baixado com sucesso"
            
        except Exception as e:
            if ftp:
                try:
                    ftp.quit()
                except:
                    pass
            
            # Remover arquivo parcial se existir
            if arquivo_local.exists():
                try:
                    arquivo_local.unlink()
                except:
                    pass
            
            error_msg = str(e)
            logger.warning(f"Tentativa {tentativa}/{max_tentativas} falhou para {nome_arquivo}: {error_msg}")
            
            # Se não é a última tentativa, aguardar antes de tentar novamente
            if tentativa < max_tentativas:
                tempo_espera = tempo_espera_base * tentativa  # Backoff exponencial
                logger.info(f"Aguardando {tempo_espera}s antes da próxima tentativa...")
                time.sleep(tempo_espera)
            else:
                # Última tentativa falhou
                logger.error(f"Todas as {max_tentativas} tentativas falharam para {nome_arquivo}: {error_msg}")
                # Registrar erro no controlador
                controlador_erros.registrar_erro(ano, 'download', nome_arquivo, f"Falha após {max_tentativas} tentativas: {error_msg}")
                return f"⏭️  PULANDO {nome_arquivo}: Falha no download após {max_tentativas} tentativas - {error_msg}"
    
    # Não deveria chegar aqui, mas por segurança
    controlador_erros.registrar_erro(ano, 'download', nome_arquivo, "Erro inesperado no download")
    return f"⏭️  PULANDO {nome_arquivo}: Erro inesperado no download"

def listar_arquivos_ftp_ano(ano):
    """
    Lista os arquivos disponíveis para um ano específico no FTP.
    
    Parâmetros:
    - ano: Ano para listar arquivos
    
    Retorna:
    - Lista de nomes de arquivos
    """
    ftp = conectar_ftp()
    if not ftp:
        return []
    
    try:
        ftp.cwd(str(ano))
        arquivos = ftp.nlst()
        
        # Filtrar apenas arquivos .7z
        arquivos_7z = [arq for arq in arquivos if arq.endswith('.7z')]
        
        ftp.quit()
        return arquivos_7z
    except Exception as e:
        print(f"Erro ao listar arquivos para o ano {ano}: {e}")
        if ftp:
            ftp.quit()
        return []

def listar_arquivos_ftp_ano_detalhado(ano):
    """
    Lista os arquivos de um ano no FTP com informações detalhadas.
    
    Parâmetros:
    - ano: Ano para listar arquivos
    
    Retorna:
    - Lista de dicionários com informações dos arquivos: 
      [{'nome': str, 'tamanho': int, 'data_modificacao': str}, ...]
    """
    ftp = conectar_ftp()
    if not ftp:
        return []
    
    try:
        ftp.cwd(str(ano))
        arquivos_info = []
        
        # Usar MLSD para obter informações detalhadas (se suportado)
        try:
            for nome, facts in ftp.mlsd():
                if nome.endswith('.7z'):
                    info = {
                        'nome': nome,
                        'tamanho': int(facts.get('size', 0)),
                        'data_modificacao': facts.get('modify', ''),
                        'tipo': facts.get('type', '')
                    }
                    arquivos_info.append(info)
        except:
            # Fallback para LIST se MLSD não for suportado
            arquivos = ftp.nlst()
            for nome in arquivos:
                if nome.endswith('.7z'):
                    try:
                        # Tentar obter tamanho do arquivo
                        tamanho = ftp.size(nome)
                    except:
                        tamanho = 0
                    
                    info = {
                        'nome': nome,
                        'tamanho': tamanho,
                        'data_modificacao': '',
                        'tipo': 'file'
                    }
                    arquivos_info.append(info)
        
        ftp.quit()
        return arquivos_info
        
    except Exception as e:
        print(f"Erro ao listar arquivos detalhados para o ano {ano}: {e}")
        if ftp:
            try:
                ftp.quit()
            except:
                pass
        return []

def verificar_arquivos_para_download(ano, pasta_destino):
    """
    Verifica quais arquivos de um ano precisam ser baixados comparando com arquivos locais.
    
    Parâmetros:
    - ano: Ano para verificar
    - pasta_destino: Pasta onde os arquivos estão/serão salvos
    
    Retorna:
    - Lista de arquivos que precisam ser baixados com motivo
    """
    # Obter informações dos arquivos remotos
    arquivos_remotos = listar_arquivos_ftp_ano_detalhado(ano)
    
    if not arquivos_remotos:
        return []
    
    # Aplicar filtros (remover EST, ESTB, IGN, NI)
    arquivos_remotos_filtrados = []
    for arquivo in arquivos_remotos:
        nome = arquivo['nome'].upper()
        if "_EST" not in nome and "ESTB" not in nome and "IGN" not in nome and "NI" not in nome:
            arquivos_remotos_filtrados.append(arquivo)
    
    # Verificar arquivos locais existentes
    pasta_ano = pasta_destino / str(ano)
    arquivos_locais = []
    if pasta_ano.exists():
        arquivos_locais = [f.name for f in pasta_ano.glob('*.7z')]
    
    # Verificar se a quantidade de arquivos é diferente
    if len(arquivos_locais) != len(arquivos_remotos_filtrados):
        print(f"  ⚠️  Ano {ano}: Quantidade de arquivos divergente - Local: {len(arquivos_locais)}, Remoto: {len(arquivos_remotos_filtrados)}")
    
    # Verificar quais arquivos precisam ser baixados
    arquivos_para_baixar = []
    
    for arquivo_remoto in arquivos_remotos_filtrados:
        nome_arquivo = arquivo_remoto['nome']
        arquivo_local = pasta_ano / nome_arquivo
        
        deve_baixar = False
        motivo = ""
        
        if not arquivo_local.exists():
            deve_baixar = True
            motivo = "não existe localmente"
        else:
            # Verificar tamanho
            tamanho_local = arquivo_local.stat().st_size
            tamanho_remoto = arquivo_remoto['tamanho']
            
            if tamanho_local != tamanho_remoto:
                deve_baixar = True
                motivo = f"tamanho diferente (local: {tamanho_local:,} bytes, remoto: {tamanho_remoto:,} bytes)"
            elif tamanho_local == 0:
                deve_baixar = True
                motivo = "arquivo local vazio"
        
        if deve_baixar:
            arquivo_remoto['motivo'] = motivo
            arquivos_para_baixar.append(arquivo_remoto)
    
    # Verificar arquivos locais órfãos (que não existem no remoto)
    nomes_remotos = {arquivo['nome'] for arquivo in arquivos_remotos_filtrados}
    arquivos_orfaos = [nome for nome in arquivos_locais if nome not in nomes_remotos]
    
    if arquivos_orfaos:
        print(f"  🗑️  Ano {ano}: {len(arquivos_orfaos)} arquivos órfãos encontrados (existem localmente mas não no FTP)")
        for orfao in arquivos_orfaos[:3]:  # Mostrar apenas os primeiros 3
            print(f"      • {orfao}")
        if len(arquivos_orfaos) > 3:
            print(f"      • ... e mais {len(arquivos_orfaos) - 3} arquivos")
    
    return arquivos_para_baixar

def obter_info_arquivo_ftp(ano, nome_arquivo):
    """
    Obtém informações de um arquivo específico no FTP.
    
    Parâmetros:
    - ano: Ano do arquivo
    - nome_arquivo: Nome do arquivo
    
    Retorna:
    - dict com informações do arquivo ou None se erro
    """
    ftp = conectar_ftp()
    if not ftp:
        return None
    
    try:
        ftp.cwd(str(ano))
        
        # Obter tamanho do arquivo
        try:
            size = ftp.size(nome_arquivo)
        except:
            size = None
        
        # Obter data de modificação
        try:
            timestamp = ftp.voidcmd(f"MDTM {nome_arquivo}")
            # Formato: 213 YYYYMMDDHHMMSS
            if timestamp.startswith('213 '):
                date_str = timestamp[4:]
                # Converter para datetime
                from datetime import datetime
                mod_time = datetime.strptime(date_str, '%Y%m%d%H%M%S')
            else:
                mod_time = None
        except:
            mod_time = None
        
        ftp.quit()
        
        return {
            'nome': nome_arquivo,
            'tamanho': size,
            'data_modificacao': mod_time,
            'ano': ano
        }
        
    except Exception as e:
        if ftp:
            ftp.quit()
        return None


def baixar_dados_ftp(anos=None, sobrescrever=False, max_workers=4):
    """
    Baixa dados do FTP da RAIS.
    
    Parâmetros:
    - anos: Lista de anos para baixar, 'atual' para o mais recente, 'todos' para todos os anos,
            ou tupla (ano_inicio, ano_fim) para uma faixa. Se None, baixa apenas anos não baixados.
    - sobrescrever: Se True, sobrescreve arquivos existentes.
    - max_workers: Número máximo de threads para download paralelo.
    
    Retorna:
    - Lista de arquivos baixados com sucesso
    """
    iniciar_tempo("Download FTP")
    
    logger = logging.getLogger(__name__)
    logger.info("INICIANDO ETAPA DE DOWNLOAD DO FTP")
    logger.info(f"Parâmetros: anos={anos}, sobrescrever={sobrescrever}, max_workers={max_workers}")
    
    print("=== INICIANDO DOWNLOAD DOS DADOS ===")
    
    # Criar diretório de destino
    pasta_destino = Path('dados-abertos-zip')
    if not pasta_destino.exists():
        pasta_destino.mkdir(parents=True)
    
    # Obter anos disponíveis no FTP
    anos_disponiveis = listar_anos_disponiveis_ftp()
    if not anos_disponiveis:
        print("Nenhum ano disponível no FTP")
        finalizar_tempo("Download FTP")
        return []
    
    print(f"Anos disponíveis no FTP: {anos_disponiveis}")
    
    # Determinar quais anos baixar
    anos_para_baixar = []
    arquivos_especificos_faltantes = {}  # Dict para armazenar arquivos específicos por ano
    
    if anos == 'atual':
        anos_para_baixar = [max(anos_disponiveis)]
        print(f"📅 Modo: Baixar apenas o ano mais atual ({max(anos_disponiveis)})")
    elif anos == 'todos':
        anos_para_baixar = anos_disponiveis
        print(f"📅 Modo: Baixar todos os anos disponíveis ({len(anos_disponiveis)} anos)")
    elif anos == 'faltantes':
        # Baixar apenas anos que não foram baixados completamente ou que têm arquivos divergentes
        anos_para_baixar = []
        total_arquivos_faltantes = 0
        
        print(f"📅 Modo: Verificação inteligente de anos faltantes")
        print(f"  🔍 Comparando arquivos locais vs. remotos...")
        
        for ano in tqdm(anos_disponiveis, desc="🔍 Verificando anos", unit="ano"):
            arquivos_necessarios = verificar_arquivos_para_download(ano, pasta_destino)
            
            if arquivos_necessarios:
                anos_para_baixar.append(ano)
                total_arquivos_faltantes += len(arquivos_necessarios)
                # Armazenar arquivos específicos para este ano
                arquivos_especificos_faltantes[ano] = arquivos_necessarios
                
                # Mostrar detalhes dos primeiros problemas encontrados
                if len(arquivos_necessarios) <= 3:
                    motivos = [f"{arq['nome']} ({arq['motivo']})" for arq in arquivos_necessarios]
                    print(f"  📁 Ano {ano}: {len(arquivos_necessarios)} arquivo(s) - {', '.join(motivos)}")
                else:
                    print(f"  📁 Ano {ano}: {len(arquivos_necessarios)} arquivos precisam ser baixados/atualizados")
        
        print(f"\n📊 Resumo da verificação:")
        print(f"  • Anos disponíveis no FTP: {len(anos_disponiveis)}")
        print(f"  • Anos que precisam de download/atualização: {len(anos_para_baixar)}")
        print(f"  • Total de arquivos a baixar/atualizar: {total_arquivos_faltantes}")
        print(f"  • Anos: {sorted(anos_para_baixar) if anos_para_baixar else 'Nenhum'}")
    elif isinstance(anos, tuple) and len(anos) == 2:
        ano_inicio, ano_fim = anos
        anos_para_baixar = [a for a in anos_disponiveis if ano_inicio <= a <= ano_fim]
        print(f"📅 Modo: Baixar faixa de anos ({ano_inicio}-{ano_fim})")
    elif isinstance(anos, list):
        anos_para_baixar = [a for a in anos if a in anos_disponiveis]
        anos_nao_disponiveis = [a for a in anos if a not in anos_disponiveis]
        print(f"📅 Modo: Baixar anos específicos: {anos}")
        if anos_nao_disponiveis:
            print(f"  ⚠️  Anos não disponíveis no FTP: {anos_nao_disponiveis}")
    else:
        # Fallback para o comportamento de 'faltantes' se nenhuma opção específica
        anos_existentes = []
        for d in pasta_destino.iterdir():
            if d.is_dir() and d.name.isdigit():
                # Verificar se a pasta tem conteúdo (arquivos .7z)
                arquivos_na_pasta = list(d.glob('*.7z'))
                if arquivos_na_pasta:  # Só considera "existente" se tiver arquivos
                    anos_existentes.append(int(d.name))
        
        anos_para_baixar = [a for a in anos_disponiveis if a not in anos_existentes]
        print(f"📅 Modo: Fallback - Baixar anos faltantes")
    
    if not anos_para_baixar:
        print("✅ Todos os anos já foram baixados ou nenhum ano corresponde aos critérios.")
        finalizar_tempo("Download FTP")
        return []
    
    print(f"Anos que serão baixados: {anos_para_baixar}")
    
    # Preparar lista de downloads
    downloads = []
    for ano in anos_para_baixar:
        # Criar pasta do ano
        pasta_ano = pasta_destino / str(ano)
        if not pasta_ano.exists():
            pasta_ano.mkdir(parents=True)
        
        # Obter lista de arquivos que precisam ser baixados
        if anos == 'faltantes':
            # Para modo faltantes, usar apenas os arquivos específicos já identificados
            arquivos_necessarios = arquivos_especificos_faltantes.get(ano, [])
            arquivos_filtrados = [arq['nome'] for arq in arquivos_necessarios]
            
            # Mostrar detalhes dos arquivos que serão baixados
            print(f"Ano {ano}: {len(arquivos_filtrados)} arquivos específicos para baixar")
            for arquivo_info in arquivos_necessarios:
                print(f"  📄 {arquivo_info['nome']} - {arquivo_info['motivo']}")
        else:
            # Para outros modos, usar a função original com filtros
            arquivos_ano = listar_arquivos_ftp_ano(ano)
            # Filtrar arquivos que não contenham "_EST", "ESTB", "IGN" ou "NI" no nome
            arquivos_filtrados = [arq for arq in arquivos_ano if "_EST" not in arq.upper() and "ESTB" not in arq.upper() 
                and "IGN" not in arq.upper() and "NI" not in arq.upper()]
            print(f"Ano {ano}: {len(arquivos_filtrados)} arquivos para baixar")
        
        for arquivo in arquivos_filtrados:
            # Para modo faltantes, forçar sobrescrever dos arquivos identificados
            if anos == 'faltantes':
                downloads.append((ano, arquivo, pasta_ano, True))  # Forçar sobrescrever=True
            else:
                downloads.append((ano, arquivo, pasta_ano, sobrescrever))
    
    if not downloads:
        print("Nenhum arquivo para baixar")
        finalizar_tempo("Download FTP")
        return []
    
    print(f"Total de {len(downloads)} arquivos para baixar")
    
    # Download em paralelo
    arquivos_baixados = []
    sucesso_count = 0
    erro_count = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submeter todos os downloads
        future_to_download = {executor.submit(baixar_arquivo_ftp, download): download 
                             for download in downloads}
        
        # Processar resultados com barra de progresso
        with tqdm(total=len(downloads), desc="🌐 Download FTP", unit="arquivo", 
                 bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]') as pbar:
            for future in as_completed(future_to_download):
                download = future_to_download[future]
                try:
                    resultado = future.result()
                    ano, nome_arquivo, _, _ = download
                    
                    if "✅" in resultado:
                        arquivos_baixados.append(f"{ano}/{nome_arquivo}")
                        sucesso_count += 1
                        pbar.set_postfix_str(f"✅{sucesso_count} ❌{erro_count}")
                    elif "❌" in resultado:
                        erro_count += 1
                        pbar.set_postfix_str(f"✅{sucesso_count} ❌{erro_count}")
                    
                    tqdm.write(resultado)
                    pbar.update(1)
                    
                except Exception as e:
                    ano, nome_arquivo, _, _ = download
                    erro_count += 1
                    erro_msg = f"❌ Erro no download {ano}/{nome_arquivo}: {str(e)}"
                    tqdm.write(erro_msg)
                    
                    # Registrar erro no log
                    logger.error(f"Erro no download {ano}/{nome_arquivo}: {str(e)}")
                    controlador_erros.registrar_erro(ano, 'download', nome_arquivo, str(e))
                    
                    pbar.set_postfix_str(f"✅{sucesso_count} ❌{erro_count}")
                    pbar.update(1)
    
    logger.info(f"DOWNLOAD CONCLUÍDO: {len(arquivos_baixados)} arquivos baixados, {sucesso_count} sucessos, {erro_count} erros")
    print(f"\nDownload concluído. {len(arquivos_baixados)} arquivos baixados com sucesso.")
    
    finalizar_tempo("Download FTP")
    return arquivos_baixados

def descompactar_arquivo_worker(args):
    """
    Worker para descompactar um arquivo específico.
    Inclui verificação de integridade e re-download automático com múltiplas tentativas.
    
    Parâmetros:
    - args: tupla contendo (arquivo_7z, pasta_destino, sobrescrever, ano, auto_redownload)
    
    Retorna:
    - Resultado da operação
    """
    arquivo_7z, pasta_destino, sobrescrever, ano, auto_redownload = args
    logger = logging.getLogger(__name__)
    
    try:
        # Verificar se o arquivo já foi descompactado
        if verificar_arquivo_extraido(arquivo_7z, pasta_destino) and not sobrescrever:
            logger.debug(f"Arquivo {arquivo_7z.name} já foi descompactado, pulando")
            return f"⏭️  Pulando {arquivo_7z.name} - já existe no destino"
        
        # NOVA FUNCIONALIDADE: Verificar integridade com múltiplas tentativas de re-download
        if auto_redownload:
            logger.debug(f"🔍 Verificando integridade de {arquivo_7z.name}")
            is_valid, error_message = verificar_integridade_arquivo_7z(arquivo_7z)
            
            if not is_valid:
                logger.warning(f"⚠️  Arquivo {arquivo_7z.name} com problema: {error_message}")
                
                # Tentar re-download até 2 vezes
                max_tentativas_redownload = 2
                pasta_ano = arquivo_7z.parent
                
                for tentativa in range(1, max_tentativas_redownload + 1):
                    logger.info(f"🔄 Tentativa {tentativa}/{max_tentativas_redownload} de re-download de {arquivo_7z.name}")
                    
                    sucesso_download = baixar_arquivo_individual(ano, arquivo_7z.name, pasta_ano, sobrescrever=True)
                    
                    if sucesso_download:
                        # Verificar integridade após re-download
                        logger.debug(f"🔍 Verificando integridade após re-download (tentativa {tentativa}) de {arquivo_7z.name}")
                        is_valid_after_download, error_after_download = verificar_integridade_arquivo_7z(arquivo_7z)
                        
                        if is_valid_after_download:
                            logger.info(f"✅ Arquivo {arquivo_7z.name} re-baixado e verificado com sucesso na tentativa {tentativa}")
                            break
                        else:
                            logger.warning(f"⚠️  Arquivo {arquivo_7z.name} ainda corrompido após tentativa {tentativa}: {error_after_download}")
                            if tentativa == max_tentativas_redownload:
                                # Última tentativa falhou - registrar erro e pular arquivo
                                error_msg = f"Arquivo corrompido após {max_tentativas_redownload} tentativas de re-download: {error_after_download}"
                                controlador_erros.registrar_erro(ano, 'descompactacao', arquivo_7z.name, error_msg)
                                return f"⏭️  PULANDO {arquivo_7z.name}: {error_msg}"
                    else:
                        logger.warning(f"⚠️  Falha no re-download (tentativa {tentativa}) de {arquivo_7z.name}")
                        if tentativa == max_tentativas_redownload:
                            # Última tentativa falhou - registrar erro e pular arquivo
                            error_msg = f"Falha no re-download após {max_tentativas_redownload} tentativas: {error_message}"
                            controlador_erros.registrar_erro(ano, 'descompactacao', arquivo_7z.name, error_msg)
                            return f"⏭️  PULANDO {arquivo_7z.name}: {error_msg}"
                
                # Se chegou aqui, o arquivo foi corrigido com sucesso
                is_valid = True
        
        # Só continua se o arquivo estiver válido
        if not is_valid:
            error_msg = f"Arquivo inválido e não foi possível corrigir: {error_message}"
            controlador_erros.registrar_erro(ano, 'descompactacao', arquivo_7z.name, error_msg)
            return f"⏭️  PULANDO {arquivo_7z.name}: {error_msg}"
        
        # Obter tamanho do arquivo para estimativa
        tamanho_arquivo = arquivo_7z.stat().st_size
        logger.debug(f"Iniciando descompactação de {arquivo_7z.name} ({tamanho_arquivo/1024/1024:.1f}MB)")
        
        inicio_extracao = time.time()
        
        # Descompactar com barra de progresso
        with tqdm(total=tamanho_arquivo, unit='B', unit_scale=True, 
                 desc=f"📦 Extraindo {arquivo_7z.name}", leave=False) as pbar:
            
            with py7zr.SevenZipFile(arquivo_7z, mode='r') as z:
                # Listar arquivos para mostrar progresso
                file_list = z.getnames()
                logger.debug(f"Arquivo {arquivo_7z.name} contém {len(file_list)} arquivos para extrair")
                
                # Extrair arquivos
                z.extractall(path=pasta_destino)
                
                # Simular progresso baseado no tamanho do arquivo
                pbar.update(tamanho_arquivo)
        
        tempo_extracao = time.time() - inicio_extracao
        logger.info(f"Extração concluída: {arquivo_7z.name} ({len(file_list)} arquivos em {tempo_extracao:.1f}s)")
        
        return f"✅ {arquivo_7z.name} descompactado com sucesso ({len(file_list)} arquivos extraídos)"
        
    except Exception as e:
        # Registrar erro no controlador
        controlador_erros.registrar_erro(ano, 'descompactacao', arquivo_7z.name, str(e))
        logger.error(f"Erro durante extração de {arquivo_7z.name}: {str(e)}")
        return f"⏭️  PULANDO {arquivo_7z.name}: Erro na descompactação - {str(e)}"

def descompactar_arquivos(anos=None, sobrescrever=False, max_arquivos=None, pausar=0, max_workers=4, auto_redownload=True):
    """
    Descompacta todos os arquivos .7z das subpastas de dados-abertos-zip
    para a pasta dados-abertos, mantendo a mesma estrutura de diretórios.
    
    Parâmetros:
    - anos: Lista de anos para processar (ex: [2015, 2016]). Se None, processa todos.
    - sobrescrever: Se True, sobrescreve arquivos existentes no destino.
    - max_arquivos: Número máximo de arquivos a processar (None para todos).
    - pausar: Segundos de pausa entre cada arquivo (para evitar sobrecarga).
    - max_workers: Número máximo de workers para processamento paralelo.
    - auto_redownload: Se True, verifica integridade e re-baixa arquivos corrompidos automaticamente.
    """
    iniciar_tempo("Descompactação")
    
    logger = logging.getLogger(__name__)
    diretorio_origem = Path('dados-abertos-zip')
    diretorio_destino = Path('dados-abertos')
    
    # Criar o diretório de destino se não existir
    if not diretorio_destino.exists():
        diretorio_destino.mkdir(parents=True)
    
    # Filtrar as pastas de anos se especificado
    diretorios_anos = [d for d in diretorio_origem.iterdir() if d.is_dir()]
    
    # Converter anos para strings para comparação
    anos_str = None
    if anos is not None:
        anos_str = [str(ano) for ano in anos]
    
    if anos_str:
        # Filtrar apenas os anos solicitados
        diretorios_anos = [d for d in diretorios_anos if d.name in anos_str]
        print(f"Filtrando apenas os anos: {', '.join(anos_str)}")
        
    if not diretorios_anos:
        print("Nenhuma pasta de ano encontrada para processar.")
        finalizar_tempo("Descompactação")
        return
        
    print(f"Encontradas {len(diretorios_anos)} pastas de anos para processar: {[d.name for d in diretorios_anos]}")
    
    # Coletar todos os arquivos para descompactar
    tarefas_descompactacao = []
    
    for ano_dir in diretorios_anos:
        # Criar a pasta correspondente no diretório de destino
        pasta_destino = diretorio_destino / ano_dir.name
        if not pasta_destino.exists():
            pasta_destino.mkdir(parents=True)
        
        print(f"\nColetando arquivos da pasta: {ano_dir.name}")
        
        # Listar todos os arquivos .7z na pasta do ano
        arquivos_7z = list(ano_dir.glob('*.7z'))
        
        # Filtrar arquivos que não contenham "_EST", "ESTB", "IGN" ou "NI" no nome
        arquivos_7z = [arq for arq in arquivos_7z if "_EST" not in arq.stem.upper() and "ESTB" not in arq.stem.upper() 
            and "IGN" not in arq.stem.upper() and "NI" not in arq.stem.upper()]
        
        if not arquivos_7z:
            print(f"  Nenhum arquivo .7z válido encontrado em {ano_dir.name}")
            continue
            
        print(f"  Encontrados {len(arquivos_7z)} arquivos para descompactar")
        
        # Adicionar às tarefas (limitar se especificado)
        for arquivo_7z in arquivos_7z:
            if max_arquivos is not None and len(tarefas_descompactacao) >= max_arquivos:
                break
            tarefas_descompactacao.append((arquivo_7z, pasta_destino, sobrescrever, int(ano_dir.name), auto_redownload))
        
        if max_arquivos is not None and len(tarefas_descompactacao) >= max_arquivos:
            break
    
    if not tarefas_descompactacao:
        print("Nenhum arquivo para descompactar")
        finalizar_tempo("Descompactação")
        return
    
    print(f"\nIniciando descompactação de {len(tarefas_descompactacao)} arquivos...")
    
    # Descompactação em paralelo
    total_processados = 0
    total_erros = 0
    anos_processados = set()
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submeter todas as tarefas
        future_to_task = {executor.submit(descompactar_arquivo_worker, task): task 
                         for task in tarefas_descompactacao}
        
        # Processar resultados com barra de progresso
        with tqdm(total=len(tarefas_descompactacao), desc="📦 Extração 7z", unit="arquivo",
                 bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]') as pbar:
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                try:
                    resultado = future.result()
                    _, _, _, ano, _ = task  # Corrigido: agora desempacota 5 elementos
                    anos_processados.add(ano)
                    
                    if "✅" in resultado:
                        total_processados += 1
                        pbar.set_postfix_str(f"✅{total_processados} ❌{total_erros}")
                    elif "❌" in resultado:
                        total_erros += 1
                        pbar.set_postfix_str(f"✅{total_processados} ❌{total_erros}")
                    
                    tqdm.write(resultado)
                    pbar.update(1)
                    
                    # Pausa se especificada
                    if pausar > 0:
                        time.sleep(pausar)
                        
                except Exception as e:
                    arquivo_7z, _, _, ano, _ = task  # Corrigido: agora desempacota 5 elementos
                    total_erros += 1
                    
                    # Registrar erro inesperado com detalhes completos
                    import traceback
                    erro_detalhado = traceback.format_exc()
                    controlador_erros.registrar_erro(ano, 'descompactacao', arquivo_7z.name, f"Erro inesperado: {str(e)}")
                    
                    # Log detalhado do erro
                    logger.error(f"Erro inesperado ao descompactar {arquivo_7z.name}: {str(e)}")
                    logger.error(f"Traceback completo para {arquivo_7z.name}:")
                    logger.error(erro_detalhado)
                    
                    erro_msg = f"❌ Erro inesperado ao descompactar {arquivo_7z.name}: {str(e)}"
                    tqdm.write(erro_msg)
                    pbar.set_postfix_str(f"✅{total_processados} ❌{total_erros}")
                    pbar.update(1)
    
    # Verificar erros por ano e gerar relatório
    anos_com_erros = []
    for ano in anos_processados:
        if controlador_erros.ano_tem_erros(ano):
            anos_com_erros.append(ano)
    
    print(f"\nDescompactação concluída. Total de {total_processados} arquivos processados com sucesso.")
    
    if anos_com_erros:
        print(f"⚠️  ATENÇÃO: {len(anos_com_erros)} ano(s) com erros de descompactação: {sorted(anos_com_erros)}")
        print("🚫 Estes anos serão EXCLUÍDOS do processamento subsequente.")
        
        # Salvar relatório de erros
        arquivos_relatorio = controlador_erros.salvar_relatorio_erros()
        if arquivos_relatorio:
            print(f"📋 Relatório de erros salvo em: {arquivos_relatorio[1]}")
    
    finalizar_tempo("Descompactação")
    
    # Retornar lista de anos que podem prosseguir (sem erros)
    anos_validos = [ano for ano in anos_processados if not controlador_erros.ano_tem_erros(ano)]
    return anos_validos

def converter_arquivo_worker(args):
    """
    Worker para converter um arquivo TXT para Parquet.
    
    Parâmetros:
    - args: tupla contendo (arquivo_txt, pasta_destino, sobrescrever, npartitions, ano)
    
    Retorna:
    - Resultado da operação
    """
    arquivo_txt, pasta_destino, sobrescrever, npartitions, ano = args
    logger = logging.getLogger(__name__)
    
    try:
        # Nome do arquivo de saída
        arquivo_parquet = pasta_destino / f"{arquivo_txt.stem}.parquet"
        
        # Verificar se o arquivo já foi convertido
        if arquivo_parquet.exists() and not sobrescrever:
            logger.debug(f"Arquivo {arquivo_txt.name} já foi convertido, pulando")
            return f"⏭️  Pulando {arquivo_txt.name} - já existe no destino"
        
        # Extrair ano do nome do arquivo
        # Procura por padrão de 4 dígitos que representam o ano no nome do arquivo
        import re
        nome_arquivo = arquivo_txt.stem
        
        # Melhorar detecção de ano para incluir anos de 1985-1999
        match_ano = re.search(r'(19|20)\d{2}', nome_arquivo)
        ano_arquivo = match_ano.group() if match_ano else None
        
        if not ano_arquivo:
            # Tenta extrair da pasta pai
            pasta_pai = arquivo_txt.parent.name
            match_ano = re.search(r'(19|20)\d{2}', pasta_pai)
            ano_arquivo = match_ano.group() if match_ano else str(ano)
        
        # Identificar se é um arquivo de ano antigo (1985-1999)
        is_arquivo_antigo = ano_arquivo and int(ano_arquivo) < 2000
        
        logger.debug(f"Ano extraído para {arquivo_txt.name}: {ano_arquivo} (arquivo antigo: {is_arquivo_antigo})")
        
        # Obter tamanho do arquivo
        tamanho_arquivo = arquivo_txt.stat().st_size
        tamanho_mb = tamanho_arquivo / (1024*1024)
        logger.info(f"Iniciando conversão de {arquivo_txt.name} ({tamanho_mb:.1f}MB) - Ano: {ano_arquivo}")
        
        inicio_conversao = time.time()
        
        with tqdm(total=100, desc=f"🔄 Convertendo {arquivo_txt.name} ({tamanho_mb:.1f}MB)", 
                 leave=False, unit="%") as pbar:
            
            # Detectar o separador
            pbar.set_postfix_str("Detectando separador...")
            with open(arquivo_txt, 'r', encoding='latin1', errors='ignore') as f:
                primeira_linha = f.readline().strip()
            separador = ';' if ';' in primeira_linha else ','
            logger.debug(f"Separador detectado para {arquivo_txt.name}: '{separador}'")
            pbar.update(10)
            
            # ESTRATÉGIA INTELIGENTE: LIMPAR VALORES PROBLEMÁTICOS E USAR TIPOS NATIVOS
            # Primeiro carrega como string, limpa valores problemáticos, depois converte para tipos apropriados
            df = None
            estrategia_usada = "desconhecida"
            
            # ETAPA 1: Carregar como string para poder limpar valores problemáticos
            pbar.set_postfix_str("Carregando como string para limpeza...")
            logger.info(f"Carregando {arquivo_txt.name} como string para limpeza de valores problemáticos")
            
            try:
                # Configurar Dask para evitar problemas de recursão
                import dask.config
                with dask.config.set({
                    'dataframe.query-planning': False,  # Desabilitar query planning que pode causar recursão
                    'array.slicing.split_large_chunks': False,  # Evitar divisão automática de chunks
                    'optimization.fuse': {},  # Reduzir fusão de operações
                    'dataframe.shuffle.method': 'tasks'  # Usar método de shuffle mais simples
                }):
                    df_string = dd.read_csv(
                        arquivo_txt,
                        sep=separador,
                        encoding='latin1',
                        dtype=str,  # Primeiro como string para limpeza
                        blocksize="32MB" if is_arquivo_antigo else "64MB",  # Chunks menores para evitar recursão
                        assume_missing=True,
                        on_bad_lines='skip',
                        na_filter=False,
                        keep_default_na=False,
                        na_values=[]
                    )
                
                # ETAPA 2: Limpar valores problemáticos
                pbar.set_postfix_str("Limpando valores problemáticos...")
                logger.info(f"Limpando valores problemáticos em {arquivo_txt.name}")
                
                # Definir padrões de valores problemáticos para converter em NULL
                valores_problematicos = [
                    '{ñ',      # Caractere problemático comum em arquivos antigos
                    '{ñ c',    # Variação do caractere problemático
                    '000-1',   # Código problemático (manter como string se for código)
                    '',        # Strings vazias
                    ' ',       # Espaços únicos
                    'nan',     # Strings 'nan'
                    'NaN',     # Strings 'NaN'
                    'null',    # Strings 'null'
                    'NULL',    # Strings 'NULL'
                ]
                
                # Aplicar limpeza robusta com tratamento de recursão e fallback
                try:
                    # MÉTODO 1: Limpeza otimizada para evitar recursão
                    logger.debug(f"Aplicando limpeza otimizada em {arquivo_txt.name}")
                    
                    # Função para limpeza segura usando pandas (evita recursão do Dask)
                    def limpar_coluna_pandas(serie_pandas, valores_problematicos):
                        """
                        Limpa uma série pandas de forma segura, evitando recursão.
                        """
                        import pandas as pd
                        
                        # Converter para string se necessário
                        if serie_pandas.dtype != 'object':
                            serie_pandas = serie_pandas.astype(str)
                        
                        # Substituir valores problemáticos por None de uma vez
                        serie_pandas = serie_pandas.replace(valores_problematicos, None)
                        
                        # Limpar espaços em branco
                        serie_pandas = serie_pandas.str.strip()
                        
                        # Converter strings vazias para None
                        serie_pandas = serie_pandas.replace(['', ' '], None)
                        
                        return serie_pandas
                    
                    # Aplicar limpeza coluna por coluna usando map_partitions (mais seguro)
                    for coluna in df_string.columns:
                        try:
                            logger.debug(f"Limpando coluna {coluna} em {arquivo_txt.name}")
                            
                            # Usar map_partitions para aplicar limpeza em cada partição
                            df_string[coluna] = df_string[coluna].map_partitions(
                                limpar_coluna_pandas,
                                valores_problematicos,
                                meta=('x', 'object')
                            )
                            
                        except RecursionError as re:
                            logger.error(f"Erro de recursão na limpeza da coluna {coluna} em {arquivo_txt.name}: {str(re)}")
                            registrar_erro_completo(ano, 'conversao', arquivo_txt.name, f"Recursão na limpeza da coluna {coluna}", re)
                            
                            # Fallback: tentar limpeza mais simples
                            try:
                                logger.info(f"Tentando limpeza simples para coluna {coluna}")
                                # Apenas substituir valores mais problemáticos
                                df_string[coluna] = df_string[coluna].replace(['{ñ', '{ñ c'], None)
                            except Exception as fe:
                                logger.warning(f"Fallback de limpeza falhou para coluna {coluna}: {str(fe)}")
                                # Se falhar completamente, deixar a coluna como está
                                continue
                                
                        except Exception as ce:
                            logger.warning(f"Erro na limpeza da coluna {coluna} em {arquivo_txt.name}: {str(ce)}")
                            # Registrar erro mas continuar com outras colunas
                            registrar_erro_completo(ano, 'conversao', arquivo_txt.name, f"Erro na limpeza da coluna {coluna}", ce)
                            continue
                            
                except RecursionError as re:
                    logger.error(f"Erro de recursão geral na limpeza de {arquivo_txt.name}: {str(re)}")
                    registrar_erro_completo(ano, 'conversao', arquivo_txt.name, "Recursão geral na limpeza", re)
                    
                    # MÉTODO 2: Fallback sem limpeza detalhada
                    logger.info(f"Aplicando fallback sem limpeza detalhada para {arquivo_txt.name}")
                    try:
                        # Apenas aplicar limpeza básica usando pandas diretamente
                        def limpeza_basica_pandas(partition):
                            """Limpeza básica usando pandas puro para evitar recursão"""
                            import pandas as pd
                            
                            # Substituir apenas os valores mais problemáticos
                            partition = partition.replace(['{ñ', '{ñ c', '000-1'], None)
                            
                            return partition
                        
                        # Aplicar limpeza básica em todas as colunas de uma vez
                        df_string = df_string.map_partitions(limpeza_basica_pandas, meta=df_string)
                        logger.info(f"Limpeza básica aplicada com sucesso em {arquivo_txt.name}")
                        
                    except Exception as fe:
                        logger.warning(f"Fallback de limpeza básica falhou para {arquivo_txt.name}: {str(fe)}")
                        # Se falhar, usar DataFrame original sem limpeza
                        logger.info(f"Usando DataFrame original sem limpeza para {arquivo_txt.name}")
                        pass
                        
                except Exception as ge:
                    logger.error(f"Erro geral na limpeza de {arquivo_txt.name}: {str(ge)}")
                    registrar_erro_completo(ano, 'conversao', arquivo_txt.name, "Erro geral na limpeza", ge)
                    # Em caso de erro geral, continuar sem limpeza
                    pass
                
                # ETAPA 3: Conversão inteligente de tipos com fallback para string
                pbar.set_postfix_str("Convertendo tipos de dados...")
                logger.info(f"Convertendo tipos de dados para {arquivo_txt.name}")
                
                # NOVA ESTRATÉGIA: Converter coluna por coluna, mantendo string para problemáticas
                df_otimizado = df_string.copy()
                colunas_convertidas = []
                colunas_mantidas_string = []
                
                # Colunas que sabemos que devem ser mantidas como string (códigos, identificadores, etc.)
                colunas_sempre_string = {
                    'CBO_OCUPACAO', 'CBO_OCUPACAO_2002', 'CNAE_CLASSE', 'CNAE_95_CLASSE',
                    'CNAE_SUBCLASSE', 'MUNICIPIO', 'MUN_TRAB', 'NACIONALIDADE', 
                    'NATUREZA_JURIDICA', 'RACA_COR', 'SEXO', 'ESCOLARIDADE_APOS_2005',
                    'VINCULO_ATIVO_FIM_ANO', 'FAIXA_ETARIA', 'FAIXA_HORA_CONTRATO',
                    'FAIXA_REMUN_DEZEM_SM', 'FAIXA_REMUN_MEDIA_SM', 'FAIXA_TEMPO_EMPREGO',
                    'TAMANHO_ESTABELECIMENTO', 'IND_PORTADOR_DEFIC', 'IND_CEI_VINCULADO',
                    'IND_SIMPLES', 'MOTIVO_DESLIGAMENTO', 'CAUSA_AFASTAMENTO_1',
                    'CAUSA_AFASTAMENTO_2', 'CAUSA_AFASTAMENTO_3', 'BAIRROS_SP',
                    'BAIRROS_FORTALEZA', 'BAIRROS_RJ', 'DISTRITOS_SP', 'REGIOES_ADM_DF',
                    'fonte_arquivo', 'ANO_RAIS', 'estrategia_leitura'
                }
                
                # Para arquivos antigos, ser mais conservador com tipos
                if is_arquivo_antigo:
                    # Manter como object para maior compatibilidade
                    df = df_string
                    estrategia_usada = "conservadora_string"
                    logger.info(f"Arquivo antigo {arquivo_txt.name}: mantendo todas as colunas como string")
                else:
                    # Para arquivos recentes, tentar otimizar tipos coluna por coluna
                    try:
                        # Identificar colunas que podem ser numéricas
                        for coluna in df_string.columns:
                            nome_coluna_limpo = coluna.upper().replace(' ', '_')
                            
                            # Verificar se é uma coluna que deve ser mantida como string
                            if nome_coluna_limpo in colunas_sempre_string:
                                colunas_mantidas_string.append(coluna)
                                logger.debug(f"Coluna {coluna} mantida como string: coluna identificada como código/texto")
                                continue
                            
                            try:
                                # Função segura para conversão numérica usando pandas
                                def converter_para_numerico_pandas(serie_pandas):
                                    """
                                    Converte série pandas para numérico de forma segura.
                                    """
                                    import pandas as pd
                                    return pd.to_numeric(serie_pandas, errors='coerce')
                                
                                # Verificar qualidade da conversão usando amostra primeiro
                                try:
                                    # Usar amostra para verificar qualidade da conversão
                                    amostra = df_string[coluna].head(1000).compute()
                                    import pandas as pd
                                    amostra_convertida = pd.to_numeric(amostra, errors='coerce')
                                    
                                    total_amostra = len(amostra)
                                    null_amostra = np.sum(pd.isna(amostra_convertida))
                                    
                                    # Se mais de 30% dos valores da amostra viraram NaN, manter como string
                                    if null_amostra / total_amostra > 0.3:
                                        logger.debug(f"Coluna {coluna} mantida como string: {null_amostra}/{total_amostra} valores da amostra seriam NaN")
                                        colunas_mantidas_string.append(coluna)
                                    else:
                                        # Aplicar conversão usando map_partitions para evitar recursão
                                        try:
                                            coluna_convertida = df_string[coluna].map_partitions(
                                                converter_para_numerico_pandas,
                                                meta=('x', 'float64')
                                            )
                                            df_otimizado[coluna] = coluna_convertida
                                            colunas_convertidas.append(coluna)
                                            logger.debug(f"Coluna {coluna} convertida para numérico")
                                        except RecursionError as re:
                                            logger.warning(f"Erro de recursão na conversão da coluna {coluna}: {str(re)}")
                                            registrar_erro_completo(ano, 'conversao', arquivo_txt.name, f"Recursão na conversão da coluna {coluna}", re)
                                            colunas_mantidas_string.append(coluna)
                                        except Exception as ec:
                                            logger.warning(f"Erro na conversão da coluna {coluna}: {str(ec)}")
                                            colunas_mantidas_string.append(coluna)
                                        
                                except Exception as e_stat:
                                    # Se não conseguir calcular estatísticas, manter como string
                                    logger.debug(f"Coluna {coluna} mantida como string: erro ao calcular estatísticas - {str(e_stat)}")
                                    colunas_mantidas_string.append(coluna)
                                    
                            except RecursionError as re:
                                # Capturar recursão específica na conversão
                                logger.error(f"Erro de recursão na conversão da coluna {coluna} em {arquivo_txt.name}: {str(re)}")
                                registrar_erro_completo(ano, 'conversao', arquivo_txt.name, f"Recursão na conversão da coluna {coluna}", re)
                                colunas_mantidas_string.append(coluna)
                            except Exception as e:
                                # Se falhar completamente, manter como string
                                logger.debug(f"Coluna {coluna} mantida como string: erro na conversão - {str(e)}")
                                colunas_mantidas_string.append(coluna)
                        
                        df = df_otimizado
                        estrategia_usada = f"otimizada_{len(colunas_convertidas)}num_{len(colunas_mantidas_string)}str"
                        logger.info(f"Conversão de tipos para {arquivo_txt.name}: {len(colunas_convertidas)} colunas numéricas, {len(colunas_mantidas_string)} mantidas como string")
                        
                        # Log das colunas convertidas para DEBUG
                        if colunas_convertidas:
                            logger.debug(f"Colunas convertidas para numérico em {arquivo_txt.name}: {colunas_convertidas}")
                        if colunas_mantidas_string:
                            logger.debug(f"Colunas mantidas como string em {arquivo_txt.name}: {colunas_mantidas_string}")
                        
                    except Exception as e:
                        logger.warning(f"Otimização de tipos falhou para {arquivo_txt.name}: {str(e)}")
                        logger.info(f"Usando fallback: todas as colunas como string para {arquivo_txt.name}")
                        df = df_string
                        estrategia_usada = "fallback_string_completo"
                
                logger.info(f"Sucesso na limpeza e otimização para {arquivo_txt.name}")
                
            except Exception as e:
                logger.warning(f"Erro na estratégia de limpeza para {arquivo_txt.name}: {str(e)}")
                
                # FALLBACK: Carregar como string puro (estratégia anterior)
                pbar.set_postfix_str("Fallback: carregamento como string...")
                logger.info(f"Usando fallback de string puro para {arquivo_txt.name}")
                
                try:
                    df = dd.read_csv(
                        arquivo_txt,
                        sep=separador,
                        encoding='latin1',
                        dtype=str,
                        blocksize="32MB",
                        assume_missing=True,
                        on_bad_lines='skip',
                        na_filter=False,
                        keep_default_na=False,
                        na_values=[],
                        low_memory=False,
                        engine='python'
                    )
                    estrategia_usada = "fallback_string_puro"
                    logger.info(f"Sucesso no fallback string puro para {arquivo_txt.name}")
                except Exception as e2:
                    logger.error(f"Todas as estratégias falharam para {arquivo_txt.name}: {str(e2)}")
                    raise e2
            
            pbar.update(40)
            
            # Obter colunas após leitura bem-sucedida
            colunas_originais = df.columns.tolist()
            
            # Limpar nomes das colunas
            pbar.set_postfix_str("Limpando nomes das colunas...")
            colunas_limpas = [limpar_nome_coluna(col) for col in colunas_originais]
            mapeamento_colunas = dict(zip(colunas_originais, colunas_limpas))
            logger.debug(f"Arquivo {arquivo_txt.name} possui {len(colunas_originais)} colunas")
            pbar.update(10)
            
            # Renomear colunas
            pbar.set_postfix_str("Processando colunas...")
            df = df.rename(columns=mapeamento_colunas)
            
            # Adicionar colunas de metadados
            df['fonte_arquivo'] = arquivo_txt.stem
            df['ANO_RAIS'] = ano_arquivo
            df['estrategia_leitura'] = estrategia_usada  # Para debugging
            
            # Reparticionar se especificado
            if npartitions:
                df = df.repartition(npartitions=npartitions)
                logger.debug(f"Reparticionado {arquivo_txt.name} para {npartitions} partições")
            pbar.update(10)
            
            # Salvar como Parquet
            pbar.set_postfix_str("Salvando Parquet...")
            df.to_parquet(
                arquivo_parquet,
                compression='snappy',
                write_index=False,
                engine='pyarrow'
            )
            pbar.update(30)
            pbar.set_postfix_str("Concluído!")
        
        # Obter informações finais
        num_colunas = len(colunas_limpas) + 3  # +3 para fonte_arquivo, ANO_RAIS e estrategia_leitura
        tamanho_parquet = arquivo_parquet.stat().st_size / (1024*1024) if arquivo_parquet.exists() else 0
        tempo_conversao = time.time() - inicio_conversao
        
        logger.info(f"Conversão concluída: {arquivo_txt.name} → {arquivo_parquet.name} ({num_colunas} colunas, {tamanho_parquet:.1f}MB em {tempo_conversao:.1f}s) - Ano: {ano_arquivo} - Estratégia: {estrategia_usada}")
        
        return f"✅ {arquivo_txt.name} → Parquet ({num_colunas} colunas, {tamanho_parquet:.1f}MB) - Ano: {ano_arquivo} - {estrategia_usada}"
        
    except Exception as e:
        # Capturar informações detalhadas do erro
        import traceback
        erro_detalhado = traceback.format_exc()
        
        # Registrar erro no controlador com informações detalhadas
        controlador_erros.registrar_erro(ano, 'conversao', arquivo_txt.name, str(e))
        
        # Log detalhado do erro
        logger.error(f"Erro durante conversão de {arquivo_txt.name}: {str(e)}")
        logger.error(f"Traceback completo para {arquivo_txt.name}:")
        logger.error(erro_detalhado)
        
        # Verificar se é erro de recursão específico
        if "maximum recursion depth exceeded" in str(e):
            logger.error(f"ERRO DE RECURSÃO DETECTADO em {arquivo_txt.name}")
            logger.error("Possíveis causas: dados circulares, limpeza infinita, ou problema no Dask")
            logger.error("Recomendação: Verificar estrutura dos dados ou reduzir complexidade da limpeza")
            
            # Tentar estratégia de emergência para arquivos com recursão
            try:
                logger.info(f"Tentando estratégia de emergência para {arquivo_txt.name}")
                
                # Estratégia ultra-simples: apenas pandas puro sem Dask
                import pandas as pd
                
                # Ler com pandas puro em chunks pequenos
                chunk_size = 10000
                chunks = []
                
                for chunk in pd.read_csv(
                    arquivo_txt,
                    sep=separador,
                    encoding='latin1',
                    dtype=str,
                    chunksize=chunk_size,
                    on_bad_lines='skip',
                    na_filter=False,
                    keep_default_na=False
                ):
                    # Limpeza básica no chunk
                    chunk = chunk.replace(['{ñ', '{ñ c', '000-1'], None)
                    chunks.append(chunk)
                
                # Concatenar chunks
                df_pandas = pd.concat(chunks, ignore_index=True)
                
                # Converter para Dask com configuração mínima
                df_emergencia = dd.from_pandas(df_pandas, npartitions=1)
                
                # Limpar nomes das colunas
                colunas_originais = df_emergencia.columns.tolist()
                colunas_limpas = [limpar_nome_coluna(col) for col in colunas_originais]
                mapeamento_colunas = dict(zip(colunas_originais, colunas_limpas))
                df_emergencia = df_emergencia.rename(columns=mapeamento_colunas)
                
                # Adicionar metadados
                df_emergencia['fonte_arquivo'] = arquivo_txt.stem
                df_emergencia['ANO_RAIS'] = ano_arquivo
                df_emergencia['estrategia_leitura'] = "emergencia_recursao"
                
                # Salvar
                df_emergencia.to_parquet(
                    arquivo_parquet,
                    compression='snappy',
                    write_index=False,
                    engine='pyarrow'
                )
                
                tamanho_parquet = arquivo_parquet.stat().st_size / (1024*1024) if arquivo_parquet.exists() else 0
                num_colunas = len(colunas_limpas) + 3
                
                logger.info(f"Estratégia de emergência bem-sucedida para {arquivo_txt.name}")
                return f"🔄 {arquivo_txt.name} → Parquet ({num_colunas} colunas, {tamanho_parquet:.1f}MB) - EMERGÊNCIA RECURSÃO"
                
            except Exception as e_emergencia:
                logger.error(f"Estratégia de emergência falhou para {arquivo_txt.name}: {str(e_emergencia)}")
                # Se a estratégia de emergência falhar, pular o arquivo
                pass
        
        return f"⏭️  PULANDO {arquivo_txt.name}: Erro na conversão - {str(e)}"

def remover_acentos(texto):
    """
    Remove acentos de um texto.
    """
    if not isinstance(texto, str):
        return texto
        
    # Normaliza o texto para separar caracteres base de seus acentos
    texto_normalizado = unicodedata.normalize('NFD', texto)
    # Remove todos os caracteres de acentuação
    return ''.join([c for c in texto_normalizado if not unicodedata.combining(c)])

def limpar_nome_coluna(nome):
    """
    Limpa o nome da coluna removendo caracteres especiais, espaços extras e acentos.
    """
    if not isinstance(nome, str):
        return nome
        
    # Remover acentos
    nome_sem_acentos = remover_acentos(nome)
    
    # Remover caracteres especiais e substituir espaços por underscore
    nome_limpo = re.sub(r'[^\w\s]', '', nome_sem_acentos).strip()
    nome_limpo = re.sub(r'\s+', '_', nome_limpo)
    return nome_limpo.upper()

def converter_para_parquet(anos=None, chunksize=100000, sobrescrever=False, max_arquivos=None, npartitions=None, max_workers=2, anos_validos=None):
    """
    Converte arquivos TXT da pasta dados-abertos para o formato Parquet na pasta parquet.
    
    Parâmetros:
    - anos: Lista de anos para processar (ex: [2015, 2016]). Se None, processa todos.
    - chunksize: Tamanho dos chunks para processamento (padrão: 100.000 linhas)
    - sobrescrever: Se True, sobrescreve arquivos Parquet existentes.
    - max_arquivos: Número máximo de arquivos a processar (None para todos).
    - npartitions: Número de partições para os arquivos Parquet (None para automático)
    - max_workers: Número máximo de workers para processamento paralelo.
    - anos_validos: Lista de anos que não tiveram erros na descompactação (usado internamente)
    """
    iniciar_tempo("Conversão TXT→Parquet")
    
    logger = logging.getLogger(__name__)
    diretorio_origem = Path('dados-abertos')
    diretorio_destino = Path('parquet')
    
    # Criar o diretório de destino se não existir
    if not diretorio_destino.exists():
        diretorio_destino.mkdir(parents=True)
    
    # Filtrar as pastas de anos se especificado
    diretorios_anos = [d for d in diretorio_origem.iterdir() if d.is_dir()]
    
    # Converter anos para strings para comparação
    anos_str = None
    if anos is not None:
        anos_str = [str(ano) for ano in anos]
    
    if anos_str:
        # Filtrar apenas os anos solicitados
        diretorios_anos = [d for d in diretorios_anos if d.name in anos_str]
        print(f"Filtrando apenas os anos: {', '.join(anos_str)}")
    
    # Filtrar anos que tiveram erros na descompactação
    if anos_validos is not None:
        anos_validos_str = [str(ano) for ano in anos_validos]
        diretorios_anos_filtrados = [d for d in diretorios_anos if d.name in anos_validos_str]
        anos_excluidos = [d.name for d in diretorios_anos if d.name not in anos_validos_str]
        
        if anos_excluidos:
            print(f"🚫 Excluindo anos com erros de descompactação: {sorted(anos_excluidos)}")
        
        diretorios_anos = diretorios_anos_filtrados
        
    if not diretorios_anos:
        print("Nenhuma pasta de ano encontrada para processar (ou todos os anos tiveram erros).")
        finalizar_tempo("Conversão TXT→Parquet")
        return
        
    print(f"Encontradas {len(diretorios_anos)} pastas de anos para processar: {[d.name for d in diretorios_anos]}")
    
    # Coletar todas as tarefas de conversão
    tarefas_conversao = []
    
    for ano_dir in diretorios_anos:
        # Criar a pasta correspondente no diretório de destino
        pasta_destino = diretorio_destino / ano_dir.name
        if not pasta_destino.exists():
            pasta_destino.mkdir(parents=True)
        
        print(f"\nColetando arquivos da pasta: {ano_dir.name}")
        
        # Listar todos os arquivos TXT na pasta do ano
        arquivos_txt = list(ano_dir.glob('*.txt'))
        
        # Filtrar arquivos que não contenham "_EST", "ESTB", "IGN" ou "NI" no nome
        arquivos_txt = [arq for arq in arquivos_txt if "_EST" not in arq.stem.upper() and "ESTB" not in arq.stem.upper() 
            and "IGN" not in arq.stem.upper() and "NI" not in arq.stem.upper()]
        
        if not arquivos_txt:
            print(f"  Nenhum arquivo TXT válido encontrado em {ano_dir.name}")
            continue
            
        print(f"  Encontrados {len(arquivos_txt)} arquivos para converter")
        
        # Adicionar às tarefas (limitar se especificado)
        for arquivo_txt in arquivos_txt:
            if max_arquivos is not None and len(tarefas_conversao) >= max_arquivos:
                break
            tarefas_conversao.append((arquivo_txt, pasta_destino, sobrescrever, npartitions, int(ano_dir.name)))
        
        if max_arquivos is not None and len(tarefas_conversao) >= max_arquivos:
            break
    
    if not tarefas_conversao:
        print("Nenhum arquivo para converter")
        finalizar_tempo("Conversão TXT→Parquet")
        return
    
    print(f"\nIniciando conversão de {len(tarefas_conversao)} arquivos...")
    
    # Conversão em paralelo (usar menos workers devido ao uso intensivo de memória)
    total_processados = 0
    total_erros = 0
    anos_processados = set()
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Submeter todas as tarefas
        future_to_task = {executor.submit(converter_arquivo_worker, task): task 
                         for task in tarefas_conversao}
        
        # Processar resultados com barra de progresso
        with tqdm(total=len(tarefas_conversao), desc="🔄 TXT→Parquet", unit="arquivo",
                 bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]') as pbar:
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                try:
                    resultado = future.result()
                    _, _, _, _, ano = task
                    anos_processados.add(ano)
                    
                    if "✅" in resultado:
                        total_processados += 1
                        pbar.set_postfix_str(f"✅{total_processados} ❌{total_erros}")
                    elif "❌" in resultado:
                        total_erros += 1
                        pbar.set_postfix_str(f"✅{total_processados} ❌{total_erros}")
                    
                    tqdm.write(resultado)
                    pbar.update(1)
                        
                except Exception as e:
                    arquivo_txt, _, _, _, ano = task
                    total_erros += 1
                    
                    # Registrar erro inesperado com detalhes completos
                    import traceback
                    erro_detalhado = traceback.format_exc()
                    controlador_erros.registrar_erro(ano, 'conversao', arquivo_txt.name, f"Erro inesperado: {str(e)}")
                    
                    # Log detalhado do erro
                    logger.error(f"Erro inesperado ao converter {arquivo_txt.name}: {str(e)}")
                    logger.error(f"Traceback completo para {arquivo_txt.name}:")
                    logger.error(erro_detalhado)
                    
                    erro_msg = f"❌ Erro inesperado ao converter {arquivo_txt.name}: {str(e)}"
                    tqdm.write(erro_msg)
                    pbar.set_postfix_str(f"✅{total_processados} ❌{total_erros}")
                    pbar.update(1)
    
    # Verificar erros por ano e gerar relatório
    anos_com_erros = []
    for ano in anos_processados:
        if controlador_erros.ano_tem_erros(ano):
            anos_com_erros.append(ano)
    
    print(f"\nConversão concluída. Total de {total_processados} arquivos convertidos com sucesso.")
    
    if anos_com_erros:
        print(f"⚠️  ATENÇÃO: {len(anos_com_erros)} ano(s) com erros de conversão: {sorted(anos_com_erros)}")
        print("🚫 Estes anos serão EXCLUÍDOS do processamento subsequente.")
        
        # Salvar relatório de erros atualizado
        arquivos_relatorio = controlador_erros.salvar_relatorio_erros()
        if arquivos_relatorio:
            print(f"📋 Relatório de erros atualizado em: {arquivos_relatorio[1]}")
    
    finalizar_tempo("Conversão TXT→Parquet")
    
    # Retornar lista de anos que podem prosseguir (sem erros)
    anos_validos_final = [ano for ano in anos_processados if not controlador_erros.ano_tem_erros(ano)]
    return anos_validos_final

def consolidar_parquets(anos=None, sobrescrever=False, preservar_arquivos=False, preservar_descompactados=False):
    """
    Consolida todos os arquivos Parquet de um ano em um único arquivo Parquet.
    
    Parâmetros:
    - anos: Lista de anos para processar (ex: [2015, 2016]). Se None, processa todos.
    - sobrescrever: Se True, sobrescreve arquivos Parquet consolidados existentes.
    - preservar_arquivos: Se True, mantém os arquivos intermediários (TXT e Parquet individuais).
    - preservar_descompactados: Se True, preserva apenas arquivos TXT, removendo Parquets intermediários.
    
    Retorna:
    - Lista de arquivos Parquet consolidados criados
    """
    iniciar_tempo("Consolidação Parquet")
    
    logger = logging.getLogger(__name__)
    diretorio_origem = Path('parquet')
    
    # Filtrar as pastas de anos se especificado
    diretorios_anos = [d for d in diretorio_origem.iterdir() if d.is_dir()]
    
    # Converter anos para strings para comparação
    anos_str = None
    if anos is not None:
        anos_str = [str(ano) for ano in anos]
    
    if anos_str:
        # Filtrar apenas os anos solicitados
        diretorios_anos = [d for d in diretorios_anos if d.name in anos_str]
        print(f"Filtrando apenas os anos: {', '.join(anos_str)}")
        
    if not diretorios_anos:
        print("Nenhuma pasta de ano encontrada para processar.")
        finalizar_tempo("Consolidação Parquet")
        return []
        
    print(f"Encontradas {len(diretorios_anos)} pastas de anos para consolidar: {[d.name for d in diretorios_anos]}")
    
    arquivos_consolidados = []
    
    # Processar cada pasta de ano
    for ano_dir in diretorios_anos:
        ano = ano_dir.name
        print(f"\nConsolidando arquivos do ano {ano}")
        
        # Nome do arquivo consolidado - sempre como diretório (sem consolidado_unico)
        arquivo_consolidado = ano_dir / f"RAIS_{ano}_consolidado"
        
        # Verificar se o arquivo já foi consolidado
        if arquivo_consolidado.exists() and not sobrescrever:
            print(f"  Pulando {arquivo_consolidado.name} - já existe (use --sobrescrever para forçar)")
            continue
        
        # Listar todos os diretórios Parquet na pasta do ano
        diretorios_parquet = [d for d in ano_dir.glob('*.parquet') if d.is_dir() and "consolidado" not in d.name]
        
        # Filtrar diretórios que não contenham "_EST", "ESTB", "IGN" ou "NI" no nome
        diretorios_parquet = [d for d in diretorios_parquet if "_EST" not in d.stem.upper() and "ESTB" not in d.stem.upper() 
            and "IGN" not in d.stem.upper() and "NI" not in d.stem.upper()]
        
        if not diretorios_parquet:
            print(f"  Nenhum diretório Parquet válido encontrado em {ano}")
            continue
            
        print(f"  Encontrados {len(diretorios_parquet)} arquivos Parquet para consolidar")
        
        try:
            # Lista para armazenar os DataFrames
            dfs = []
            
            print(f"  📊 Iniciando consolidação de {len(diretorios_parquet)} arquivos...")
            
            # Processar todos os diretórios Parquet com barra de progresso
            for dir_parquet in tqdm(diretorios_parquet, desc=f"  📖 Lendo Parquets {ano}", unit="arquivo", leave=False):
                try:
                    # Ler o arquivo Parquet com Dask
                    df = dd.read_parquet(dir_parquet)
                    
                    # Obter informações do DataFrame
                    try:
                        nrows = len(df)
                        ncols = len(df.columns)
                        msg = f"    ✅ {dir_parquet.stem}: {nrows:,} linhas, {ncols} colunas"
                        tqdm.write(msg)
                        logger.debug(msg)
                    except:
                        msg = f"    ✅ {dir_parquet.stem}: Carregado com sucesso"
                        tqdm.write(msg)
                        logger.debug(msg)
                    
                    # Adicionar coluna com o nome do arquivo para identificação (se não existir)
                    if 'fonte_arquivo' not in df.columns:
                        df['fonte_arquivo'] = dir_parquet.stem
                    
                    # Adicionar coluna do ano se não existir
                    if 'ANO_RAIS' not in df.columns:
                        df['ANO_RAIS'] = ano
                    
                    # Adicionar à lista
                    dfs.append(df)
                except Exception as e:
                    erro_msg = f"    ❌ Erro ao ler {dir_parquet.name}: {str(e)}"
                    tqdm.write(erro_msg)
                    logger.error(f"Erro ao ler {dir_parquet.name}: {str(e)}")
            
            if not dfs:
                print(f"  ❌ Nenhum arquivo Parquet válido encontrado para o ano {ano}")
                continue
                
            # Concatenar todos os DataFrames
            print(f"  🔗 Concatenando {len(dfs)} DataFrames...")
            with tqdm(desc=f"  🔗 Consolidando {ano}", unit="dataframe", total=len(dfs), leave=False) as pbar_concat:
                df_consolidado = dd.concat(dfs)
                pbar_concat.update(len(dfs))
            
            # Informações sobre o DataFrame consolidado
            try:
                total_linhas = len(df_consolidado)
                total_colunas = len(df_consolidado.columns)
                print(f"  📈 Resultado: {total_linhas:,} linhas, {total_colunas} colunas")
            except:
                print(f"  📈 DataFrame consolidado criado com sucesso")
            
            # Salvar o DataFrame consolidado (sempre como diretório)
            print(f"  💾 Salvando como diretório...")
            with tqdm(desc=f"  💾 Salvando {ano}", unit="partição", leave=False) as pbar_save:
                df_consolidado.to_parquet(
                    arquivo_consolidado,
                    compression='snappy',
                    write_index=False,
                    engine='pyarrow'
                )
                pbar_save.update(1)
            
            # Informações finais do arquivo salvo
            # Calcular tamanho total do diretório
            tamanho_final = sum(f.stat().st_size for f in arquivo_consolidado.rglob('*.parquet')) / (1024*1024*1024)
            
            print(f"  ✅ Arquivo consolidado criado: {tamanho_final:.2f} GB")
            arquivos_consolidados.append(arquivo_consolidado)
            
            # Gerenciar arquivos intermediários baseado nos novos parâmetros
            if preservar_arquivos:
                # Preservar tudo
                print(f"  🔒 Preservando todos os arquivos intermediários")
            elif preservar_descompactados:
                # Preservar apenas TXT, remover Parquets intermediários
                print(f"  🗂️ Preservando arquivos TXT, removendo Parquets intermediários...")
                for dir_parquet in diretorios_parquet:
                    try:
                        shutil.rmtree(dir_parquet)
                        print(f"    ✅ Removido: {dir_parquet.name}")
                    except Exception as e:
                        print(f"    ❌ Erro ao remover {dir_parquet}: {str(e)}")
            else:
                # Comportamento padrão: remover TXT e Parquets intermediários
                print(f"  🗑️ Removendo arquivos intermediários...")
                diretorio_txt = Path('dados-abertos') / ano
                if diretorio_txt.exists():
                    arquivos_txt = list(diretorio_txt.glob('*.txt'))
                    arquivos_txt = [arq for arq in arquivos_txt if "_EST" not in arq.stem.upper() and "ESTB" not in arq.stem.upper() 
                        and "IGN" not in arq.stem.upper() and "NI" not in arq.stem.upper()]
                    for arquivo in arquivos_txt:
                        try:
                            arquivo.unlink()
                        except Exception as e:
                            print(f"    ❌ Erro ao remover {arquivo}: {str(e)}")
                    
                    # Remover a pasta do ano se estiver vazia
                    try:
                        if not any(diretorio_txt.iterdir()):  # Verificar se a pasta está vazia
                            diretorio_txt.rmdir()
                            print(f"    ✅ Pasta vazia removida: {diretorio_txt}")
                    except Exception as e:
                        print(f"    ❌ Erro ao remover pasta {diretorio_txt}: {str(e)}")
                
                # Remover diretórios Parquet individuais
                for dir_parquet in diretorios_parquet:
                    try:
                        shutil.rmtree(dir_parquet)
                    except Exception as e:
                        print(f"    ❌ Erro ao remover {dir_parquet}: {str(e)}")
            
        except Exception as e:
            print(f"  ❌ Erro ao consolidar arquivos do ano {ano}: {str(e)}")
            logger.error(f"Erro ao consolidar arquivos do ano {ano}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            print(traceback.format_exc())
    
    finalizar_tempo("Consolidação Parquet")
    return arquivos_consolidados

def consolidar_geral(sobrescrever=False, incremental=False, remover_anos_individuais=False):
    """
    Consolida todos os arquivos consolidados de todos os anos em uma única pasta.
    
    ORIENTAÇÕES TÉCNICAS BASEADAS NA ESTRUTURA OFICIAL DA RAIS:
    
    ✅ CONSOLIDAÇÃO APROVADA - Justificativas técnicas:
    
    1. COMPATIBILIDADE DE SCHEMA: Todos os arquivos de vínculos RAIS possuem
       estrutura idêntica (60+ campos padronizados desde 2006)
       
    2. FILTRAGEM AUTOMÁTICA: Exclusão obrigatória de:
       - Arquivos EST/ESTB (estabelecimentos - estrutura diferente)
       - Arquivos NI (não identificados - dados incompletos)
       
    3. RASTREABILIDADE: Adição automática de metadados:
       - ANO_RAIS: Identificação temporal única
       - fonte_arquivo: Origem geográfica (estado) de cada registro
       
    4. INTEGRIDADE DOS DADOS: Preservação completa da informação original
       com enriquecimento de metadados para análises temporais
       
    5. CONFORMIDADE: Respeita orientações do layout oficial RAIS
       mantendo todos os campos obrigatórios e opcionais
    
    Esta consolidação facilita:
    - Análises temporais de séries históricas
    - Comparações inter-regionais
    - Estudos longitudinais de mercado de trabalho
    - Processamento analítico otimizado (OLAP)
    
    Parâmetros:
    - sobrescrever: Se True, sobrescreve o arquivo consolidado geral existente.
    - incremental: Se True, adiciona apenas anos novos ao arquivo consolidado existente.
    - remover_anos_individuais: Se True, remove pastas de anos individuais após consolidação bem-sucedida.
    
    Retorna:
    - Caminho do arquivo/diretório consolidado geral criado
    """
    iniciar_tempo("Consolidação Geral")
    
    logger = logging.getLogger(__name__)
    diretorio_origem = Path('parquet')
    
    # Nome do arquivo/diretório consolidado geral (sempre como diretório)
    arquivo_consolidado_geral = diretorio_origem / "RAIS_TODOS_ANOS_consolidado"
    
    # Verificar se o arquivo já foi consolidado
    if arquivo_consolidado_geral.exists() and not sobrescrever and not incremental:
        print(f"Arquivo consolidado geral já existe: {arquivo_consolidado_geral.name}")
        print("Use --sobrescrever para forçar a recriação ou --incremental para adicionar novos anos")
        finalizar_tempo("Consolidação Geral")
        return arquivo_consolidado_geral
    
    # Encontrar todas as pastas de anos com arquivos consolidados
    diretorios_anos = []
    arquivos_consolidados = []
    anos_existentes_consolidado = set()
    
    # Se modo incremental, verificar anos já presentes
    if incremental and arquivo_consolidado_geral.exists():
        try:
            print("🔄 Modo incremental: verificando anos já consolidados...")
            df_existente = dd.read_parquet(arquivo_consolidado_geral)
            if 'ANO_RAIS' in df_existente.columns:
                anos_existentes_consolidado = set(df_existente['ANO_RAIS'].unique().compute())
                print(f"   Anos já consolidados: {sorted(anos_existentes_consolidado)}")
            else:
                print("   ⚠️ Coluna ANO_RAIS não encontrada no arquivo existente, processando todos os anos")
                incremental = False
        except Exception as e:
            print(f"   ❌ Erro ao ler arquivo existente para modo incremental: {str(e)}")
            print("   Processando todos os anos...")
            incremental = False
    
    for ano_dir in diretorio_origem.iterdir():
        if ano_dir.is_dir() and ano_dir.name.isdigit():
            # Se modo incremental, pular anos já processados
            if incremental and ano_dir.name in anos_existentes_consolidado:
                print(f"   ⏭️ Pulando ano {ano_dir.name} (já consolidado)")
                continue
                
            # Procurar arquivo consolidado nesta pasta
            arquivo_consolidado = ano_dir / f"RAIS_{ano_dir.name}_consolidado"
            arquivo_consolidado_unico = ano_dir / f"RAIS_{ano_dir.name}_consolidado.parquet"
            
            if arquivo_consolidado.exists():
                arquivos_consolidados.append((ano_dir.name, arquivo_consolidado))
            elif arquivo_consolidado_unico.exists():
                arquivos_consolidados.append((ano_dir.name, arquivo_consolidado_unico))
    
    if not arquivos_consolidados:
        if incremental:
            print("✅ Nenhum ano novo encontrado para consolidação incremental.")
        else:
            print("❌ Nenhum arquivo consolidado encontrado para consolidação geral.")
            print("   Execute primeiro: python main.py --modo consolidar")
        finalizar_tempo("Consolidação Geral")
        return None
    
    # Ordenar por ano
    arquivos_consolidados.sort(key=lambda x: x[0])
    
    if incremental:
        print(f"\n📊 Consolidação Incremental - Adicionando {len(arquivos_consolidados)} anos novos:")
    else:
        print(f"\n📊 Consolidação Geral - Unindo {len(arquivos_consolidados)} anos:")
    
    for ano, caminho in arquivos_consolidados:
        print(f"  📅 {ano}: {caminho.name}")
    
    try:
        # Lista para armazenar os DataFrames
        dfs = []
        total_linhas = 0
        total_colunas = 0
        
        # Se modo incremental, adicionar o DataFrame existente
        if incremental and arquivo_consolidado_geral.exists():
            print(f"\n🔗 Carregando dados existentes...")
            try:
                df_existente = dd.read_parquet(arquivo_consolidado_geral)
                dfs.append(df_existente)
                print(f"  ✅ Dados existentes carregados com sucesso")
            except Exception as e:
                print(f"  ❌ Erro ao carregar dados existentes: {str(e)}")
                return None
        
        print(f"\n🔗 Carregando dados de {len(arquivos_consolidados)} anos...")
        
        # Processar todos os arquivos consolidados com barra de progresso
        for ano, arquivo_consolidado in tqdm(arquivos_consolidados, desc="📖 Carregando anos", unit="ano"):
            try:
                # Ler o arquivo consolidado com Dask
                df = dd.read_parquet(arquivo_consolidado)
                
                # Garantir que existe a coluna ANO_RAIS
                if 'ANO_RAIS' not in df.columns:
                    df['ANO_RAIS'] = ano
                
                # Obter informações do DataFrame
                try:
                    nrows = len(df)
                    ncols = len(df.columns)
                    total_linhas += nrows
                    total_colunas = max(total_colunas, ncols)
                    msg = f"  ✅ Ano {ano}: {nrows:,} linhas, {ncols} colunas"
                    tqdm.write(msg)
                    logger.debug(msg)
                except:
                    msg = f"  ✅ Ano {ano}: Carregado com sucesso"
                    tqdm.write(msg)
                    logger.debug(msg)
                
                # Adicionar à lista
                dfs.append(df)
                
            except Exception as e:
                erro_msg = f"  ❌ Erro ao carregar ano {ano}: {str(e)}"
                tqdm.write(erro_msg)
                logger.error(f"Erro ao carregar ano {ano}: {str(e)}")
        
        if not dfs:
            print("❌ Nenhum arquivo válido encontrado para consolidação geral")
            finalizar_tempo("Consolidação Geral")
            return None
        
        # Concatenar todos os DataFrames com verificação de compatibilidade
        print(f"\n🔗 Concatenando {len(dfs)} DataFrames de diferentes anos...")
        print("🔍 Verificando compatibilidade de schemas entre anos...")
        
        # Verificar se todos os DataFrames têm tipos compatíveis
        schemas_info = []
        for i, df in enumerate(dfs):
            try:
                # Obter informações do schema
                dtypes = df.dtypes.to_dict()
                colunas = set(df.columns)
                schemas_info.append({
                    'index': i,
                    'dtypes': dtypes,
                    'colunas': colunas,
                    'num_colunas': len(colunas)
                })
            except Exception as e:
                logger.warning(f"Erro ao verificar schema do DataFrame {i}: {str(e)}")
        
        # Verificar compatibilidade
        if len(schemas_info) > 1:
            schema_base = schemas_info[0]
            incompatibilidades = []
            
            for info in schemas_info[1:]:
                # Verificar diferenças de colunas
                colunas_faltantes = schema_base['colunas'] - info['colunas']
                colunas_extras = info['colunas'] - schema_base['colunas']
                
                if colunas_faltantes or colunas_extras:
                    incompatibilidades.append({
                        'tipo': 'colunas',
                        'index': info['index'],
                        'faltantes': colunas_faltantes,
                        'extras': colunas_extras
                    })
                
                # Verificar diferenças de tipos (apenas para colunas comuns)
                colunas_comuns = schema_base['colunas'] & info['colunas']
                tipos_diferentes = []
                
                for coluna in colunas_comuns:
                    if coluna in schema_base['dtypes'] and coluna in info['dtypes']:
                        if schema_base['dtypes'][coluna] != info['dtypes'][coluna]:
                            tipos_diferentes.append({
                                'coluna': coluna,
                                'tipo_base': schema_base['dtypes'][coluna],
                                'tipo_atual': info['dtypes'][coluna]
                            })
                
                if tipos_diferentes:
                    incompatibilidades.append({
                        'tipo': 'tipos',
                        'index': info['index'],
                        'diferencas': tipos_diferentes
                    })
            
            if incompatibilidades:
                print("⚠️  Incompatibilidades de schema detectadas:")
                for incomp in incompatibilidades:
                    if incomp['tipo'] == 'colunas':
                        if incomp['faltantes']:
                            print(f"   DataFrame {incomp['index']}: Colunas faltantes: {incomp['faltantes']}")
                        if incomp['extras']:
                            print(f"   DataFrame {incomp['index']}: Colunas extras: {incomp['extras']}")
                    elif incomp['tipo'] == 'tipos':
                        print(f"   DataFrame {incomp['index']}: Tipos diferentes:")
                        for diff in incomp['diferencas']:
                            print(f"     {diff['coluna']}: {diff['tipo_base']} → {diff['tipo_atual']}")
                
                print("💡 Tentando concatenação com ignore_index=True para resolver incompatibilidades...")
            else:
                print("✅ Todos os schemas são compatíveis!")
        
        with tqdm(desc="🔗 Consolidando geral", unit="ano", total=len(dfs)) as pbar_concat:
            try:
                df_consolidado = dd.concat(dfs, ignore_index=True)
                pbar_concat.update(len(dfs))
                print("✅ Concatenação bem-sucedida!")
            except Exception as e:
                logger.error(f"Erro na concatenação: {str(e)}")
                print(f"❌ Erro na concatenação: {str(e)}")
                print("🔧 Tentando concatenação com alinhamento de colunas...")
                
                # Fallback: Alinhar colunas manualmente
                try:
                    # Obter todas as colunas únicas
                    todas_colunas = set()
                    for df in dfs:
                        todas_colunas.update(df.columns)
                    
                    # Alinhar DataFrames
                    dfs_alinhados = []
                    for df in dfs:
                        # Adicionar colunas faltantes com valores vazios
                        for coluna in todas_colunas:
                            if coluna not in df.columns:
                                df[coluna] = ""
                        
                        # Reordenar colunas
                        df = df[sorted(todas_colunas)]
                        dfs_alinhados.append(df)
                    
                    df_consolidado = dd.concat(dfs_alinhados, ignore_index=True)
                    pbar_concat.update(len(dfs))
                    print("✅ Concatenação com alinhamento bem-sucedida!")
                except Exception as e2:
                    logger.error(f"Falha no alinhamento de colunas: {str(e2)}")
                    raise e2
        
        # Informações sobre o DataFrame consolidado
        try:
            linhas_finais = len(df_consolidado)
            colunas_finais = len(df_consolidado.columns)
            print(f"📈 Resultado final: {linhas_finais:,} linhas, {colunas_finais} colunas")
        except:
            print(f"📈 DataFrame consolidado geral criado com sucesso")
        
        # Salvar o DataFrame consolidado (sempre como diretório)
        print(f"\n💾 Salvando consolidação geral...")
        print(f"💾 Salvando como diretório: {arquivo_consolidado_geral.name}/")
        with tqdm(desc="💾 Salvando diretório", unit="partição") as pbar_save:
            df_consolidado.to_parquet(
                arquivo_consolidado_geral,
                compression='snappy',
                write_index=False,
                engine='pyarrow'
            )
            pbar_save.update(1)
        
        # Informações finais do arquivo salvo
        if arquivo_consolidado_geral.exists():
            # Calcular tamanho total do diretório
            tamanho_final = sum(f.stat().st_size for f in arquivo_consolidado_geral.rglob('*.parquet')) / (1024*1024*1024)
            
            print(f"✅ Consolidação geral concluída!")
            print(f"📁 Local: {arquivo_consolidado_geral}")
            print(f"📊 Dados: {total_linhas:,} linhas de {len(arquivos_consolidados)} anos ({min(x[0] for x in arquivos_consolidados)}-{max(x[0] for x in arquivos_consolidados)})")
            print(f"💾 Tamanho: {tamanho_final:.2f} GB")
            
            # Remover anos individuais se solicitado
            if remover_anos_individuais:
                print(f"\n🗑️ Removendo pastas de anos individuais após consolidação bem-sucedida...")
                anos_removidos = 0
                for ano, arquivo_original in arquivos_consolidados:
                    try:
                        pasta_ano = arquivo_original.parent
                        if pasta_ano.name.isdigit() and pasta_ano != diretorio_origem:
                            shutil.rmtree(pasta_ano)
                            anos_removidos += 1
                            print(f"    ✅ Removido: {pasta_ano.name}/")
                    except Exception as e:
                        print(f"    ❌ Erro ao remover pasta do ano {ano}: {str(e)}")
                
                if anos_removidos > 0:
                    print(f"🗑️ Total de {anos_removidos} pastas de anos removidas para economizar espaço")
                    
                    # Calcular espaço economizado
                    espaco_economizado = anos_removidos * (tamanho_final / len(arquivos_consolidados))
                    print(f"💾 Espaço em disco economizado: ~{espaco_economizado:.2f} GB")
            
        finalizar_tempo("Consolidação Geral")
        return arquivo_consolidado_geral
        
    except Exception as e:
        print(f"❌ Erro durante consolidação geral: {str(e)}")
        import traceback
        print(traceback.format_exc())
        finalizar_tempo("Consolidação Geral")
        return None



def processar_ano_completo(ano, sobrescrever=False, max_arquivos=None, 
                          pausar=0, npartitions=None, preservar_arquivos=False, 
                          preservar_descompactados=False, max_workers_extract=4, 
                          max_workers_convert=2):
    """
    Processa um ano completo: descompactar → converter → consolidar → limpeza.
    
    Esta função implementa processamento otimizado por ano, evitando ter que
    esperar todos os anos serem descompactados antes de iniciar a conversão.
    
    Parâmetros:
    - ano: Ano para processar
    - sobrescrever: Se True, sobrescreve arquivos existentes
    - max_arquivos: Número máximo de arquivos por etapa (None para todos)
    - pausar: Segundos de pausa entre arquivos
    - npartitions: Número de partições para arquivos Parquet
    - preservar_arquivos: Se True, mantém todos os arquivos intermediários
    - preservar_descompactados: Se True, preserva apenas arquivos TXT
    - max_workers_extract: Workers para descompactação
    - max_workers_convert: Workers para conversão
    
    Retorna:
    - Tuple (sucesso: bool, arquivo_consolidado: Path, estatisticas: dict)
    """
    logger = logging.getLogger(__name__)
    logger.info(f"🚀 Iniciando processamento completo do ano {ano}")
    
    iniciar_tempo(f"Processamento Ano {ano}")
    
    try:
        # Estruturas de diretórios
        diretorio_zip = Path('dados-abertos-zip') / str(ano)
        diretorio_txt = Path('dados-abertos') / str(ano)
        diretorio_parquet = Path('parquet') / str(ano)
        
        # Criar diretórios se não existirem
        diretorio_txt.mkdir(parents=True, exist_ok=True)
        diretorio_parquet.mkdir(parents=True, exist_ok=True)
        
        estatisticas = {
            'ano': ano,
            'arquivos_zip': 0,
            'arquivos_txt': 0,
            'arquivos_parquet': 0,
            'arquivo_consolidado': None,
            'tempo_descompactacao': 0,
            'tempo_conversao': 0,
            'tempo_consolidacao': 0,
            'espaco_economizado_gb': 0
        }
        
        # Verificar se já está consolidado
        arquivo_consolidado = diretorio_parquet / f"RAIS_{ano}_consolidado"
        if arquivo_consolidado.exists() and not sobrescrever:
            logger.info(f"⏭️ Ano {ano} já processado, pulando")
            return True, arquivo_consolidado, estatisticas
            
        print(f"\n{'='*60}")
        print(f"📅 PROCESSANDO ANO {ano} - Pipeline Completo")
        print(f"{'='*60}")
        
        # ======== ETAPA 1: DESCOMPACTAÇÃO ========
        print(f"\n🔧 ETAPA 1: Descompactando arquivos do ano {ano}")
        iniciar_tempo(f"Descompactação {ano}")
        
        # Listar arquivos ZIP do ano
        if not diretorio_zip.exists():
            logger.warning(f"Pasta de arquivos ZIP não encontrada para o ano {ano}: {diretorio_zip}")
            return False, None, estatisticas
            
        arquivos_7z = list(diretorio_zip.glob('*.7z'))
        arquivos_7z = [arq for arq in arquivos_7z if "_EST" not in arq.stem.upper() 
                      and "ESTB" not in arq.stem.upper() and "IGN" not in arq.stem.upper() 
                      and "NI" not in arq.stem.upper()]
        
        if not arquivos_7z:
            logger.warning(f"Nenhum arquivo 7z válido encontrado para o ano {ano}")
            return False, None, estatisticas
            
        print(f"  📦 Encontrados {len(arquivos_7z)} arquivos 7z para descompactar")
        estatisticas['arquivos_zip'] = len(arquivos_7z)
        
        # Usar função de descompactação com controle de erros
        anos_validos = descompactar_arquivos(anos=[ano], sobrescrever=sobrescrever, 
                                            max_arquivos=max_arquivos, pausar=pausar, 
                                            max_workers=max_workers_extract, auto_redownload=True)
        
        # Verificar se o ano teve erros na descompactação
        if controlador_erros.ano_tem_erros(ano):
            print(f"❌ Ano {ano} teve erros na descompactação. Suspendendo processamento.")
            return False, None, estatisticas
        
        tempo_descompactacao = finalizar_tempo(f"Descompactação {ano}")
        estatisticas['tempo_descompactacao'] = tempo_descompactacao
        print(f"  ✅ Descompactação do ano {ano} concluída")
        
        # ======== ETAPA 2: CONVERSÃO ========
        print(f"\n🔄 ETAPA 2: Convertendo arquivos TXT para Parquet do ano {ano}")
        iniciar_tempo(f"Conversão {ano}")
        
        # Usar função de conversão com controle de erros
        anos_validos_conversao = converter_para_parquet(anos=[ano], sobrescrever=sobrescrever, 
                                                       max_arquivos=max_arquivos, npartitions=npartitions,
                                                       max_workers=max_workers_convert, anos_validos=[ano])
        
        tempo_conversao = finalizar_tempo(f"Conversão {ano}")
        estatisticas['tempo_conversao'] = tempo_conversao
        
        # Verificar se o ano teve erros na conversão
        if controlador_erros.ano_tem_erros(ano):
            print(f"❌ Ano {ano} teve erros na conversão. Suspendendo processamento.")
            return False, None, estatisticas
        
        # ======== ETAPA 3: CONSOLIDAÇÃO ========
        print(f"\n📊 ETAPA 3: Consolidando arquivos Parquet do ano {ano}")
        iniciar_tempo(f"Consolidação {ano}")
        
        # Consolidar arquivos Parquet do ano
        arquivos_consolidados = consolidar_parquets(anos=[ano], sobrescrever=sobrescrever,
                                                   preservar_arquivos=preservar_arquivos, 
                                                   preservar_descompactados=preservar_descompactados)
        
        tempo_consolidacao = finalizar_tempo(f"Consolidação {ano}")
        estatisticas['tempo_consolidacao'] = tempo_consolidacao
        
        if arquivos_consolidados:
            arquivo_consolidado = arquivos_consolidados[0]
            estatisticas['arquivo_consolidado'] = str(arquivo_consolidado)
            
            # Calcular tamanho do arquivo consolidado
            tamanho_consolidado = sum(f.stat().st_size for f in arquivo_consolidado.rglob('*.parquet')) / (1024*1024*1024)
            
            print(f"  ✅ Consolidação do ano {ano}: {tamanho_consolidado:.2f} GB")
            
            # ======== ETAPA 4: LIMPEZA E ESTATÍSTICAS ========
            if not preservar_arquivos:
                print(f"\n🗑️ ETAPA 4: Limpeza de arquivos intermediários do ano {ano}")
                
                # Calcular espaço que será economizado
                if not preservar_descompactados:
                    espaco_txt = sum(f.stat().st_size for f in diretorio_txt.glob('*.txt') 
                                   if "_EST" not in f.stem.upper() and "ESTB" not in f.stem.upper() 
                                   and "IGN" not in f.stem.upper() and "NI" not in f.stem.upper()) / (1024*1024*1024)
                    estatisticas['espaco_economizado_gb'] += espaco_txt
                
                # A limpeza já foi feita durante a consolidação
                print(f"  🗑️ Limpeza concluída para o ano {ano}")
            
            tempo_total = finalizar_tempo(f"Processamento Ano {ano}")
            
            print(f"\n✅ ANO {ano} PROCESSADO COM SUCESSO!")
            print(f"  ⏱️ Tempo total: {formatar_tempo(tempo_total)}")
            print(f"  📊 Arquivo final: {tamanho_consolidado:.2f} GB")
            if estatisticas['espaco_economizado_gb'] > 0:
                print(f"  💾 Espaço economizado: {estatisticas['espaco_economizado_gb']:.2f} GB")
            
            return True, arquivo_consolidado, estatisticas
        else:
            logger.error(f"Falha na consolidação do ano {ano}")
            return False, None, estatisticas
            
    except Exception as e:
        logger.error(f"Erro durante processamento do ano {ano}: {str(e)}")
        print(f"❌ Erro durante processamento do ano {ano}: {str(e)}")
        import traceback
        traceback.print_exc()
        return False, None, estatisticas

def processar_paralelo_por_ano(anos=None, sobrescrever=False, max_arquivos=None, 
                               pausar=0, npartitions=None, preservar_arquivos=False, 
                               preservar_descompactados=False, max_workers_extract=4, 
                               max_workers_convert=2, max_anos_paralelos=2):
    """
    Processa múltiplos anos em paralelo, onde cada ano é processado completamente
    (descompactar → converter → consolidar) antes de passar para o próximo.
    
    Vantagens sobre o pipeline sequencial:
    - Não precisa esperar TODOS os anos serem descompactados
    - Economiza espaço em disco (processa e limpa por ano)
    - Aproveitamento melhor de recursos (I/O + CPU paralelos)
    - Falha de um ano não impacta os outros
    
    Parâmetros:
    - anos: Lista de anos para processar (None para todos disponíveis)
    - sobrescrever: Se True, sobrescreve arquivos existentes
    - max_arquivos: Número máximo de arquivos por etapa por ano
    - pausar: Segundos de pausa entre arquivos
    - npartitions: Número de partições para arquivos Parquet
    - preservar_arquivos: Se True, mantém todos os arquivos intermediários
    - preservar_descompactados: Se True, preserva apenas arquivos TXT
    - max_workers_extract: Workers para descompactação por ano
    - max_workers_convert: Workers para conversão por ano
    - max_anos_paralelos: Número máximo de anos processados simultaneamente
    
    Retorna:
    - Lista de arquivos consolidados criados
    """
    logger = logging.getLogger(__name__)
    logger.info("🚀 Iniciando processamento paralelo por ano")
    
    iniciar_tempo("Processamento Paralelo por Ano")
    
    # Determinar anos para processar
    diretorio_zip = Path('dados-abertos-zip')
    if not diretorio_zip.exists():
        print("❌ Pasta de dados-abertos-zip não encontrada")
        return []
        
    diretorios_anos = [d for d in diretorio_zip.iterdir() if d.is_dir() and d.name.isdigit()]
    
    if anos:
        anos_str = [str(ano) for ano in anos]
        diretorios_anos = [d for d in diretorios_anos if d.name in anos_str]
        
    if not diretorios_anos:
        print("❌ Nenhum ano encontrado para processar")
        return []
        
    anos_para_processar = sorted([int(d.name) for d in diretorios_anos])
    
    print(f"\n🎯 PROCESSAMENTO PARALELO POR ANO")
    print(f"{'='*60}")
    print(f"📅 Anos para processar: {anos_para_processar}")
    print(f"🔧 Processamento por ano: descompactar → converter → consolidar")
    print(f"⚡ Workers por ano: {max_workers_extract} extração, {max_workers_convert} conversão")
    print(f"🔄 Anos simultâneos: {max_anos_paralelos}")
    print(f"{'='*60}")
    
    # Processar anos em paralelo (limitado)
    arquivos_consolidados = []
    estatisticas_totais = {
        'anos_processados': 0,
        'anos_falhados': 0,
        'total_arquivos_zip': 0,
        'total_arquivos_txt': 0,
        'total_arquivos_parquet': 0,
        'tempo_total_descompactacao': 0,
        'tempo_total_conversao': 0,
        'tempo_total_consolidacao': 0,
        'espaco_total_economizado_gb': 0
    }
    
    # Usar ThreadPoolExecutor para processar anos em paralelo
    with ThreadPoolExecutor(max_workers=max_anos_paralelos) as executor:
        # Submeter tarefas de processamento por ano
        future_to_ano = {
            executor.submit(
                processar_ano_completo, 
                ano, sobrescrever, max_arquivos, pausar, npartitions,
                preservar_arquivos, preservar_descompactados, 
                max_workers_extract, max_workers_convert
            ): ano for ano in anos_para_processar
        }
        
        # Processar resultados conforme completam
        with tqdm(total=len(anos_para_processar), desc="📅 Processando anos", unit="ano") as pbar:
            for future in as_completed(future_to_ano):
                ano = future_to_ano[future]
                try:
                    sucesso, arquivo_consolidado, estatisticas = future.result()
                    
                    if sucesso and arquivo_consolidado:
                        arquivos_consolidados.append(arquivo_consolidado)
                        estatisticas_totais['anos_processados'] += 1
                        msg = f"✅ Ano {ano} processado com sucesso"
                        tqdm.write(msg)
                        logger.info(msg)
                    else:
                        estatisticas_totais['anos_falhados'] += 1
                        msg = f"❌ Falha no processamento do ano {ano}"
                        tqdm.write(msg)
                        logger.error(msg)
                    
                    # Acumular estatísticas
                    estatisticas_totais['total_arquivos_zip'] += estatisticas.get('arquivos_zip', 0)
                    estatisticas_totais['total_arquivos_txt'] += estatisticas.get('arquivos_txt', 0)
                    estatisticas_totais['total_arquivos_parquet'] += estatisticas.get('arquivos_parquet', 0)
                    estatisticas_totais['tempo_total_descompactacao'] += estatisticas.get('tempo_descompactacao', 0)
                    estatisticas_totais['tempo_total_conversao'] += estatisticas.get('tempo_conversao', 0)
                    estatisticas_totais['tempo_total_consolidacao'] += estatisticas.get('tempo_consolidacao', 0)
                    estatisticas_totais['espaco_total_economizado_gb'] += estatisticas.get('espaco_economizado_gb', 0)
                    
                    pbar.set_postfix_str(f"✅{estatisticas_totais['anos_processados']} ❌{estatisticas_totais['anos_falhados']}")
                    pbar.update(1)
                    
                except Exception as e:
                    estatisticas_totais['anos_falhados'] += 1
                    erro_msg = f"❌ Erro inesperado no ano {ano}: {str(e)}"
                    tqdm.write(erro_msg)
                    logger.error(f"Erro inesperado no ano {ano}: {str(e)}")
                    # Log traceback completo
                    import traceback
                    logger.error(traceback.format_exc())
                    pbar.set_postfix_str(f"✅{estatisticas_totais['anos_processados']} ❌{estatisticas_totais['anos_falhados']}")
                    pbar.update(1)
    
    # Resumo final
    tempo_total = finalizar_tempo("Processamento Paralelo por Ano")
    
    print(f"\n🎉 PROCESSAMENTO PARALELO POR ANO CONCLUÍDO!")
    print(f"{'='*60}")
    print(f"✅ Anos processados com sucesso: {estatisticas_totais['anos_processados']}")
    print(f"❌ Anos com falha: {estatisticas_totais['anos_falhados']}")
    print(f"📊 Total de arquivos processados:")
    print(f"   📦 ZIP: {estatisticas_totais['total_arquivos_zip']}")
    print(f"   📄 TXT: {estatisticas_totais['total_arquivos_txt']}")
    print(f"   📊 Parquet: {estatisticas_totais['total_arquivos_parquet']}")
    print(f"⏱️ Tempo total: {formatar_tempo(tempo_total)}")
    if estatisticas_totais['espaco_total_economizado_gb'] > 0:
        print(f"💾 Espaço total economizado: {estatisticas_totais['espaco_total_economizado_gb']:.2f} GB")
    print(f"📁 Arquivos consolidados criados: {len(arquivos_consolidados)}")
    for arquivo in arquivos_consolidados:
        print(f"   📁 {arquivo}")
    print(f"{'='*60}")
    
    logger.info(f"Processamento paralelo concluído: {estatisticas_totais['anos_processados']} sucessos, {estatisticas_totais['anos_falhados']} falhas")
    
    return arquivos_consolidados

def processar_sequencial_otimizado(anos=None, sobrescrever=False, max_arquivos=None, 
                                   pausar=0, npartitions=None, preservar_arquivos=False, 
                                   preservar_descompactados=False, max_workers_extract=4, 
                                   max_workers_convert=2):
    """
    Processa anos de forma sequencial otimizada: um ano por vez, completamente.
    
    FLUXO IDEAL POR ANO:
    1. Download dos arquivos .7z do ano (paralelo entre arquivos)
    2. Descompactação dos arquivos .7z (paralelo entre arquivos)
    3. Conversão dos arquivos TXT para Parquet (paralelo entre arquivos)
    4. Consolidação dos arquivos Parquet do ano
    5. Limpeza: Remove arquivos TXT (se não preservar_descompactados)
    6. Limpeza: Remove arquivos Parquet individuais (se não preservar_arquivos)
    7. Limpeza: Remove arquivos .7z (opcional)
    8. Próximo ano
    
    DIFERENÇA CHAVE:
    - ANTES: Baixava TODOS os anos → Descompactava TODOS → Convertia TODOS → Consolidava TODOS
    - AGORA: Para cada ano: Baixa → Descompacta → Converte → Consolida → Limpa
    
    Vantagens:
    - Economia máxima de espaço em disco (não acumula arquivos de vários anos)
    - Processamento resiliente (falha em um ano não afeta outros)
    - Melhor controle de recursos
    - Evita erro "No space left on device"
    - Mantém paralelismo onde é eficiente (dentro de cada etapa)
    """
    logger = logging.getLogger(__name__)
    logger.info("🚀 Iniciando processamento sequencial otimizado")
    
    iniciar_tempo("Processamento Sequencial Otimizado")
    
    # Determinar anos para processar
    if anos is None:
        anos_disponiveis = listar_anos_disponiveis_ftp()
        if not anos_disponiveis:
            print("❌ Nenhum ano disponível no FTP")
            return []
        anos = anos_disponiveis
    
    anos_para_processar = sorted(anos)
    
    print(f"\n🎯 PROCESSAMENTO SEQUENCIAL OTIMIZADO")
    print(f"{'='*60}")
    print(f"📅 Anos para processar: {anos_para_processar}")
    print(f"🔧 Fluxo por ano: download → descompactar → converter → consolidar → limpar")
    print(f"💾 Economia de espaço: processamento um ano por vez")
    print(f"{'='*60}")
    
    arquivos_consolidados = []
    estatisticas_totais = {
        'anos_processados': 0,
        'anos_falhados': 0,
        'anos_pulados': 0,
        'total_arquivos_processados': 0,
        'espaco_total_economizado_gb': 0.0
    }
    
    # Processar cada ano sequencialmente
    for i, ano in enumerate(anos_para_processar, 1):
        print(f"\n{'='*60}")
        print(f"📅 PROCESSANDO ANO {ano} ({i}/{len(anos_para_processar)})")
        print(f"{'='*60}")
        
        try:
            # Verificar se já está consolidado
            diretorio_parquet = Path('parquet') / str(ano)
            arquivo_consolidado = diretorio_parquet / f"RAIS_{ano}_consolidado"
            
            if arquivo_consolidado.exists() and not sobrescrever:
                print(f"⏭️ Ano {ano} já processado, pulando")
                arquivos_consolidados.append(arquivo_consolidado)
                estatisticas_totais['anos_pulados'] += 1
                continue
            
            # ETAPA 1: DOWNLOAD DOS ARQUIVOS DO ANO
            print(f"\n🌐 ETAPA 1: Download dos arquivos do ano {ano}")
            iniciar_tempo(f"Download {ano}")
            
            pasta_zip_ano = Path('dados-abertos-zip') / str(ano)
            pasta_zip_ano.mkdir(parents=True, exist_ok=True)
            
            # Baixar apenas os arquivos deste ano
            arquivos_baixados = baixar_dados_ftp(anos=[ano], sobrescrever=sobrescrever, 
                                                max_workers=4)
            
            finalizar_tempo(f"Download {ano}")
            
            if not arquivos_baixados:
                print(f"❌ Nenhum arquivo baixado para o ano {ano}")
                estatisticas_totais['anos_falhados'] += 1
                continue
            
            # ETAPA 2: DESCOMPACTAÇÃO
            print(f"\n📦 ETAPA 2: Descompactação dos arquivos do ano {ano}")
            iniciar_tempo(f"Descompactação {ano}")
            
            # Criar pasta de destino para arquivos TXT
            pasta_txt_ano = Path('dados-abertos') / str(ano)
            pasta_txt_ano.mkdir(parents=True, exist_ok=True)
            
            # Descompactar apenas os arquivos deste ano
            anos_descompactados = descompactar_arquivos(anos=[ano], sobrescrever=sobrescrever, 
                                                       max_arquivos=max_arquivos, pausar=pausar, 
                                                       max_workers=max_workers_extract, auto_redownload=True)
            
            finalizar_tempo(f"Descompactação {ano}")
            
            # Verificar se houve erros na descompactação
            if controlador_erros.ano_tem_erros(ano):
                print(f"❌ Ano {ano} teve erros na descompactação. Pulando para o próximo ano.")
                estatisticas_totais['anos_falhados'] += 1
                continue
            
            # ETAPA 3: CONVERSÃO TXT → PARQUET
            print(f"\n🔄 ETAPA 3: Conversão TXT para Parquet do ano {ano}")
            iniciar_tempo(f"Conversão {ano}")
            
            # Criar pasta de destino para arquivos Parquet
            diretorio_parquet.mkdir(parents=True, exist_ok=True)
            
            # Converter apenas os arquivos deste ano
            anos_convertidos = converter_para_parquet(anos=[ano], sobrescrever=sobrescrever, 
                                                     max_arquivos=max_arquivos, npartitions=npartitions,
                                                     max_workers=max_workers_convert, anos_validos=[ano])
            
            finalizar_tempo(f"Conversão {ano}")
            
            # Verificar se houve erros na conversão
            if controlador_erros.ano_tem_erros(ano):
                print(f"❌ Ano {ano} teve erros na conversão. Pulando para o próximo ano.")
                estatisticas_totais['anos_falhados'] += 1
                continue
            
            # ETAPA 4: CONSOLIDAÇÃO
            print(f"\n📊 ETAPA 4: Consolidação dos arquivos Parquet do ano {ano}")
            iniciar_tempo(f"Consolidação {ano}")
            
            # Consolidar arquivos Parquet do ano
            arquivos_consolidados_ano = consolidar_parquets(anos=[ano], sobrescrever=sobrescrever,
                                                           preservar_arquivos=preservar_arquivos, 
                                                           preservar_descompactados=preservar_descompactados)
            
            finalizar_tempo(f"Consolidação {ano}")
            
            if not arquivos_consolidados_ano:
                print(f"❌ Falha na consolidação do ano {ano}")
                estatisticas_totais['anos_falhados'] += 1
                continue
            
            arquivo_consolidado = arquivos_consolidados_ano[0]
            arquivos_consolidados.append(arquivo_consolidado)
            
            # ETAPA 5: LIMPEZA OTIMIZADA
            print(f"\n🗑️ ETAPA 5: Limpeza de arquivos intermediários do ano {ano}")
            iniciar_tempo(f"Limpeza {ano}")
            
            espaco_economizado = 0
            
            # 5.1: Remover arquivos TXT (se não preservar_descompactados)
            if not preservar_descompactados and not preservar_arquivos:
                arquivos_txt = list(pasta_txt_ano.glob('*.txt'))
                for arquivo_txt in arquivos_txt:
                    if "_EST" not in arquivo_txt.stem.upper() and "ESTB" not in arquivo_txt.stem.upper():
                        tamanho = arquivo_txt.stat().st_size
                        arquivo_txt.unlink()
                        espaco_economizado += tamanho
                
                # Remover pasta se vazia
                if pasta_txt_ano.exists() and not list(pasta_txt_ano.iterdir()):
                    pasta_txt_ano.rmdir()
                
                print(f"  🗑️ Arquivos TXT removidos: {len(arquivos_txt)}")
            
            # 5.2: Remover arquivos Parquet individuais (se não preservar_arquivos)
            if not preservar_arquivos:
                arquivos_parquet_individuais = [f for f in diretorio_parquet.glob('*.parquet') 
                                              if not f.name.startswith('RAIS_') or '_consolidado' not in f.name]
                for arquivo_parquet in arquivos_parquet_individuais:
                    tamanho = arquivo_parquet.stat().st_size
                    arquivo_parquet.unlink()
                    espaco_economizado += tamanho
                
                print(f"  🗑️ Arquivos Parquet individuais removidos: {len(arquivos_parquet_individuais)}")
            
            # 5.3: Remover arquivos .7z (opcional - pode ser configurado)
            # Por enquanto, vamos manter os arquivos .7z para possível reprocessamento
            
            finalizar_tempo(f"Limpeza {ano}")
            
            espaco_economizado_gb = espaco_economizado / (1024*1024*1024)
            estatisticas_totais['espaco_total_economizado_gb'] += espaco_economizado_gb
            estatisticas_totais['anos_processados'] += 1
            
            # Calcular tamanho do arquivo consolidado
            tamanho_consolidado = 0
            if arquivo_consolidado.exists():
                if arquivo_consolidado.is_file():
                    tamanho_consolidado = arquivo_consolidado.stat().st_size
                else:
                    tamanho_consolidado = sum(f.stat().st_size for f in arquivo_consolidado.rglob('*.parquet'))
            
            tamanho_consolidado_gb = tamanho_consolidado / (1024*1024*1024)
            
            print(f"\n✅ ANO {ano} PROCESSADO COM SUCESSO!")
            print(f"  📊 Arquivo consolidado: {tamanho_consolidado_gb:.2f} GB")
            print(f"  💾 Espaço economizado: {espaco_economizado_gb:.2f} GB")
            print(f"  📁 Localização: {arquivo_consolidado}")
            
        except Exception as e:
            logger.error(f"Erro durante processamento do ano {ano}: {str(e)}")
            print(f"❌ Erro durante processamento do ano {ano}: {str(e)}")
            estatisticas_totais['anos_falhados'] += 1
            continue
    
    # Resumo final
    tempo_total = finalizar_tempo("Processamento Sequencial Otimizado")
    
    print(f"\n🎉 PROCESSAMENTO SEQUENCIAL OTIMIZADO CONCLUÍDO!")
    print(f"{'='*60}")
    print(f"✅ Anos processados com sucesso: {estatisticas_totais['anos_processados']}")
    print(f"⏭️ Anos já existentes (pulados): {estatisticas_totais['anos_pulados']}")
    print(f"❌ Anos com falha: {estatisticas_totais['anos_falhados']}")
    print(f"⏱️ Tempo total: {formatar_tempo(tempo_total)}")
    print(f"💾 Espaço total economizado: {estatisticas_totais['espaco_total_economizado_gb']:.2f} GB")
    print(f"📁 Arquivos consolidados criados: {len(arquivos_consolidados)}")
    for arquivo in arquivos_consolidados:
        print(f"   📁 {arquivo}")
    print(f"{'='*60}")
    
    logger.info(f"Processamento sequencial concluído: {estatisticas_totais['anos_processados']} sucessos, {estatisticas_totais['anos_falhados']} falhas")
    
    return arquivos_consolidados

def processar_arquivo_pipeline(arquivo_info):
    """
    Processa um arquivo em pipeline: download → descompactar → converter
    
    Parâmetros:
    - arquivo_info: dict com informações do arquivo
    
    Retorna:
    - dict com resultado do processamento
    """
    ano, nome_arquivo, pasta_zip, pasta_txt, pasta_parquet, sobrescrever = arquivo_info
    logger = logging.getLogger(__name__)
    
    resultado = {
        'ano': ano,
        'arquivo': nome_arquivo,
        'sucesso': False,
        'etapa_falha': None,
        'erro': None,
        'arquivos_gerados': []
    }
    
    try:
        # ETAPA 1: Download
        logger.debug(f"Pipeline {ano}/{nome_arquivo}: Iniciando download")
        arquivo_7z = pasta_zip / nome_arquivo
        
        if not arquivo_7z.exists() or sobrescrever:
            download_resultado = baixar_arquivo_ftp((ano, nome_arquivo, pasta_zip, sobrescrever))
            if "❌" in download_resultado:
                resultado['etapa_falha'] = 'download'
                resultado['erro'] = download_resultado
                return resultado
        
        # ETAPA 2: Descompactação (imediata após download)
        logger.debug(f"Pipeline {ano}/{nome_arquivo}: Iniciando descompactação")
        descompactar_resultado = descompactar_arquivo_worker((arquivo_7z, pasta_txt, sobrescrever, ano, True))
        
        if "❌" in descompactar_resultado:
            resultado['etapa_falha'] = 'descompactacao'
            resultado['erro'] = descompactar_resultado
            return resultado
        
        # ETAPA 3: Conversão (imediata após descompactação)
        logger.debug(f"Pipeline {ano}/{nome_arquivo}: Iniciando conversão")
        
        # Encontrar arquivos TXT gerados pela descompactação
        nome_base = arquivo_7z.stem
        arquivos_txt = list(pasta_txt.glob(f"{nome_base}*"))
        
        if not arquivos_txt:
            resultado['etapa_falha'] = 'conversao'
            resultado['erro'] = f"Nenhum arquivo TXT encontrado após descompactação de {nome_arquivo}"
            return resultado
        
        # Marcar arquivos TXT como prontos para conversão posterior
        # A conversão será feita em batch após todos os arquivos serem descompactados
        arquivos_parquet_gerados = arquivos_txt  # Usar arquivos TXT como base
        
        if not arquivos_parquet_gerados:
            resultado['etapa_falha'] = 'conversao'
            resultado['erro'] = f"Nenhum arquivo TXT encontrado para conversão"
            return resultado
        
        resultado['sucesso'] = True
        resultado['arquivos_gerados'] = arquivos_parquet_gerados
        
        logger.debug(f"Pipeline {ano}/{nome_arquivo}: Processamento concluído com sucesso")
        return resultado
        
    except Exception as e:
        resultado['etapa_falha'] = 'erro_geral'
        resultado['erro'] = str(e)
        logger.error(f"Erro no pipeline {ano}/{nome_arquivo}: {str(e)}")
        return resultado

def processar_ano_pipeline_assincrono(ano, sobrescrever=False, max_arquivos=None, 
                                     pausar=0, npartitions=None, preservar_arquivos=False, 
                                     preservar_descompactados=False, max_workers_pipeline=4):
    """
    Processa um ano usando pipeline assíncrono otimizado.
    
    FLUXO PIPELINE POR ARQUIVO:
    1. Download arquivo.7z
    2. ↓ Imediatamente descompacta → arquivo.txt
    3. ↓ Imediatamente converte → arquivo.parquet
    4. Quando todos terminam → consolida todos .parquet
    5. Limpeza
    
    Vantagens:
    - Máximo aproveitamento de recursos (I/O + CPU simultâneos)
    - Não espera todos os downloads terminarem
    - Processamento em streaming
    - Menor uso de espaço temporário
    """
    logger = logging.getLogger(__name__)
    logger.info(f"🚀 Iniciando processamento pipeline assíncrono do ano {ano}")
    
    iniciar_tempo(f"Pipeline Assíncrono {ano}")
    
    try:
        # Estruturas de diretórios
        pasta_zip = Path('dados-abertos-zip') / str(ano)
        pasta_txt = Path('dados-abertos') / str(ano)
        pasta_parquet = Path('parquet') / str(ano)
        
        # Criar diretórios se não existirem
        pasta_zip.mkdir(parents=True, exist_ok=True)
        pasta_txt.mkdir(parents=True, exist_ok=True)
        pasta_parquet.mkdir(parents=True, exist_ok=True)
        
        # Verificar se já está consolidado
        arquivo_consolidado = pasta_parquet / f"RAIS_{ano}_consolidado"
        if arquivo_consolidado.exists() and not sobrescrever:
            logger.info(f"⏭️ Ano {ano} já processado, pulando")
            return True, arquivo_consolidado, {'ano': ano}
        
        print(f"\n{'='*60}")
        print(f"📅 PROCESSAMENTO PIPELINE ASSÍNCRONO - ANO {ano}")
        print(f"🔄 Fluxo: download → descompactar → converter (em pipeline)")
        print(f"{'='*60}")
        
        # Obter lista de arquivos para processar
        arquivos_ano = listar_arquivos_ftp_ano(ano)
        if not arquivos_ano:
            logger.warning(f"Nenhum arquivo encontrado para o ano {ano}")
            return False, None, {'ano': ano}
        
        # Filtrar arquivos
        arquivos_filtrados = [arq for arq in arquivos_ano if "_EST" not in arq.upper() 
                             and "ESTB" not in arq.upper() and "IGN" not in arq.upper() 
                             and "NI" not in arq.upper()]
        
        if max_arquivos:
            arquivos_filtrados = arquivos_filtrados[:max_arquivos]
        
        print(f"📦 Arquivos para processar: {len(arquivos_filtrados)}")
        
        # Preparar informações para pipeline
        arquivos_pipeline = []
        for nome_arquivo in arquivos_filtrados:
            arquivo_info = (ano, nome_arquivo, pasta_zip, pasta_txt, pasta_parquet, sobrescrever)
            arquivos_pipeline.append(arquivo_info)
        
        # PROCESSAMENTO EM PIPELINE ASSÍNCRONO (Download + Descompactação)
        arquivos_processados = []
        arquivos_falhados = []
        
        with ThreadPoolExecutor(max_workers=max_workers_pipeline) as executor:
            # Submeter todos os arquivos para processamento em pipeline
            future_to_arquivo = {
                executor.submit(processar_arquivo_pipeline, arquivo_info): arquivo_info[1]
                for arquivo_info in arquivos_pipeline
            }
            
            # Processar resultados conforme completam
            with tqdm(total=len(arquivos_pipeline), desc=f"🔄 Pipeline {ano} (Download+Descompactar)", unit="arquivo") as pbar:
                for future in as_completed(future_to_arquivo):
                    nome_arquivo = future_to_arquivo[future]
                    try:
                        resultado = future.result()
                        
                        if resultado['sucesso']:
                            arquivos_processados.append(resultado)
                            msg = f"✅ {nome_arquivo}: Download+Descompactação concluídos"
                            tqdm.write(msg)
                            logger.debug(msg)
                        else:
                            arquivos_falhados.append(resultado)
                            msg = f"❌ {nome_arquivo}: Falha na {resultado['etapa_falha']} - {resultado['erro']}"
                            tqdm.write(msg)
                            logger.error(f"Falha no pipeline {nome_arquivo}: {resultado['etapa_falha']} - {resultado['erro']}")
                        
                        pbar.set_postfix_str(f"✅{len(arquivos_processados)} ❌{len(arquivos_falhados)}")
                        pbar.update(1)
                        
                        if pausar > 0:
                            time.sleep(pausar)
                            
                    except Exception as e:
                        arquivos_falhados.append({
                            'arquivo': nome_arquivo,
                            'erro': str(e),
                            'etapa_falha': 'erro_executor'
                        })
                        erro_msg = f"❌ {nome_arquivo}: Erro no executor - {str(e)}"
                        tqdm.write(erro_msg)
                        logger.error(f"Erro no executor para {nome_arquivo}: {str(e)}")
                        # Log traceback completo
                        import traceback
                        logger.error(traceback.format_exc())
                        pbar.set_postfix_str(f"✅{len(arquivos_processados)} ❌{len(arquivos_falhados)}")
                        pbar.update(1)
        
        print(f"\n📊 RESULTADO DO PIPELINE:")
        print(f"  ✅ Arquivos processados: {len(arquivos_processados)}")
        print(f"  ❌ Arquivos falhados: {len(arquivos_falhados)}")
        
        if not arquivos_processados:
            logger.error(f"Nenhum arquivo foi processado com sucesso para o ano {ano}")
            return False, None, {'ano': ano}
        
        # ETAPA 3: CONVERSÃO EM BATCH (após todos os downloads/descompactações)
        print(f"\n🔄 ETAPA 3: Conversão TXT para Parquet do ano {ano}")
        iniciar_tempo(f"Conversão {ano}")
        
        # Usar função de conversão existente com controle de erros aprimorado
        anos_convertidos = converter_para_parquet(anos=[ano], sobrescrever=sobrescrever, 
                                                 max_arquivos=max_arquivos, npartitions=npartitions,
                                                 max_workers=max_workers_pipeline, anos_validos=[ano])
        
        finalizar_tempo(f"Conversão {ano}")
        
        # Verificar se houve erros na conversão
        if controlador_erros.ano_tem_erros(ano):
            print(f"❌ Ano {ano} teve erros na conversão. Verificando se há arquivos suficientes...")
            # Continuar se pelo menos alguns arquivos foram convertidos
            arquivos_parquet_existentes = list(pasta_parquet.glob('*.parquet'))
            if not arquivos_parquet_existentes:
                print(f"❌ Nenhum arquivo Parquet gerado para o ano {ano}")
                return False, None, {'ano': ano}
            else:
                print(f"⚠️ Continuando com {len(arquivos_parquet_existentes)} arquivos Parquet gerados")
        
        # ETAPA 4: CONSOLIDAÇÃO
        print(f"\n📊 ETAPA 4: Consolidação dos arquivos Parquet do ano {ano}")
        iniciar_tempo(f"Consolidação {ano}")
        
        # Consolidar arquivos Parquet do ano
        arquivos_consolidados = consolidar_parquets(anos=[ano], sobrescrever=sobrescrever,
                                                   preservar_arquivos=preservar_arquivos, 
                                                   preservar_descompactados=preservar_descompactados)
        
        finalizar_tempo(f"Consolidação {ano}")
        
        if not arquivos_consolidados:
            logger.error(f"Falha na consolidação do ano {ano}")
            return False, None, {'ano': ano}
        
        arquivo_consolidado = arquivos_consolidados[0]
        
        # ETAPA 5: LIMPEZA
        print(f"\n🗑️ ETAPA 5: Limpeza de arquivos intermediários do ano {ano}")
        iniciar_tempo(f"Limpeza {ano}")
        
        espaco_economizado = 0
        
        # Remover arquivos TXT (se não preservar_descompactados)
        if not preservar_descompactados and not preservar_arquivos:
            arquivos_txt = list(pasta_txt.glob('*.txt'))
            for arquivo_txt in arquivos_txt:
                tamanho = arquivo_txt.stat().st_size
                arquivo_txt.unlink()
                espaco_economizado += tamanho
            
            if pasta_txt.exists() and not list(pasta_txt.iterdir()):
                pasta_txt.rmdir()
            
            print(f"  🗑️ Arquivos TXT removidos: {len(arquivos_txt)}")
        
        # Remover arquivos Parquet individuais (se não preservar_arquivos)
        if not preservar_arquivos:
            arquivos_parquet_individuais = [f for f in pasta_parquet.glob('*.parquet') 
                                          if not f.name.startswith('RAIS_') or '_consolidado' not in f.name]
            for arquivo_parquet in arquivos_parquet_individuais:
                tamanho = arquivo_parquet.stat().st_size
                arquivo_parquet.unlink()
                espaco_economizado += tamanho
            
            print(f"  🗑️ Arquivos Parquet individuais removidos: {len(arquivos_parquet_individuais)}")
        
        finalizar_tempo(f"Limpeza {ano}")
        
        # Estatísticas finais
        tempo_total = finalizar_tempo(f"Pipeline Assíncrono {ano}")
        espaco_economizado_gb = espaco_economizado / (1024*1024*1024)
        
        # Calcular tamanho do arquivo consolidado
        tamanho_consolidado = 0
        if arquivo_consolidado.exists():
            if arquivo_consolidado.is_file():
                tamanho_consolidado = arquivo_consolidado.stat().st_size
            else:
                tamanho_consolidado = sum(f.stat().st_size for f in arquivo_consolidado.rglob('*.parquet'))
        
        tamanho_consolidado_gb = tamanho_consolidado / (1024*1024*1024)
        
        print(f"\n✅ ANO {ano} PROCESSADO COM PIPELINE ASSÍNCRONO!")
        print(f"  ⏱️ Tempo total: {formatar_tempo(tempo_total)}")
        print(f"  📊 Arquivo consolidado: {tamanho_consolidado_gb:.2f} GB")
        print(f"  💾 Espaço economizado: {espaco_economizado_gb:.2f} GB")
        print(f"  🔄 Arquivos processados em pipeline: {len(arquivos_processados)}")
        print(f"  📁 Localização: {arquivo_consolidado}")
        
        estatisticas = {
            'ano': ano,
            'arquivos_processados': len(arquivos_processados),
            'arquivos_falhados': len(arquivos_falhados),
            'tempo_total': tempo_total,
            'espaco_economizado_gb': espaco_economizado_gb,
            'tamanho_consolidado_gb': tamanho_consolidado_gb
        }
        
        return True, arquivo_consolidado, estatisticas
        
    except Exception as e:
        logger.error(f"Erro durante processamento pipeline do ano {ano}: {str(e)}")
        print(f"❌ Erro durante processamento pipeline do ano {ano}: {str(e)}")
        return False, None, {'ano': ano, 'erro': str(e)}

def processar_sequencial_pipeline_otimizado(anos=None, sobrescrever=False, max_arquivos=None, 
                                           pausar=0, npartitions=None, preservar_arquivos=False, 
                                           preservar_descompactados=False, max_workers_pipeline=4):
    """
    Processa anos usando pipeline assíncrono otimizado: um ano por vez, mas com pipeline interno.
    
    FLUXO PIPELINE POR ANO:
    - Para cada arquivo do ano: download → descompactar → converter (em pipeline)
    - Quando todos terminam: consolidar
    - Limpeza
    - Próximo ano
    
    VANTAGENS SOBRE O FLUXO ANTERIOR:
    - Não espera todos os downloads terminarem
    - Processamento em streaming por arquivo
    - Máximo aproveitamento de recursos I/O + CPU
    - Menor uso de espaço temporário
    """
    logger = logging.getLogger(__name__)
    logger.info("🚀 Iniciando processamento sequencial com pipeline assíncrono")
    
    iniciar_tempo("Processamento Sequencial Pipeline")
    
    # Determinar anos para processar
    if anos is None:
        anos_disponiveis = listar_anos_disponiveis_ftp()
        if not anos_disponiveis:
            print("❌ Nenhum ano disponível no FTP")
            return []
        anos = anos_disponiveis
    
    anos_para_processar = sorted(anos)
    
    print(f"\n🎯 PROCESSAMENTO SEQUENCIAL COM PIPELINE ASSÍNCRONO")
    print(f"{'='*60}")
    print(f"📅 Anos para processar: {anos_para_processar}")
    print(f"🔄 Fluxo por arquivo: download → descompactar → converter (pipeline)")
    print(f"💾 Economia de espaço: processamento um ano por vez")
    print(f"⚡ Pipeline assíncrono: máximo aproveitamento de recursos")
    print(f"{'='*60}")
    
    arquivos_consolidados = []
    estatisticas_totais = {
        'anos_processados': 0,
        'anos_falhados': 0,
        'anos_pulados': 0,
        'total_arquivos_processados': 0,
        'espaco_total_economizado_gb': 0.0,
        'tempo_total_pipeline': 0.0
    }
    
    # Processar cada ano sequencialmente com pipeline interno
    for i, ano in enumerate(anos_para_processar, 1):
        print(f"\n{'='*60}")
        print(f"📅 PROCESSANDO ANO {ano} ({i}/{len(anos_para_processar)}) - PIPELINE")
        print(f"{'='*60}")
        
        try:
            sucesso, arquivo_consolidado, estatisticas = processar_ano_pipeline_assincrono(
                ano, sobrescrever, max_arquivos, pausar, npartitions,
                preservar_arquivos, preservar_descompactados, max_workers_pipeline
            )
            
            if sucesso and arquivo_consolidado:
                arquivos_consolidados.append(arquivo_consolidado)
                estatisticas_totais['anos_processados'] += 1
                estatisticas_totais['total_arquivos_processados'] += estatisticas.get('arquivos_processados', 0)
                estatisticas_totais['espaco_total_economizado_gb'] += estatisticas.get('espaco_economizado_gb', 0.0)
                estatisticas_totais['tempo_total_pipeline'] += estatisticas.get('tempo_total', 0.0)
            else:
                estatisticas_totais['anos_falhados'] += 1
                
        except Exception as e:
            logger.error(f"Erro durante processamento pipeline do ano {ano}: {str(e)}")
            print(f"❌ Erro durante processamento pipeline do ano {ano}: {str(e)}")
            estatisticas_totais['anos_falhados'] += 1
            continue
    
    # Resumo final
    tempo_total = finalizar_tempo("Processamento Sequencial Pipeline")
    
    print(f"\n🎉 PROCESSAMENTO SEQUENCIAL COM PIPELINE CONCLUÍDO!")
    print(f"{'='*60}")
    print(f"✅ Anos processados com sucesso: {estatisticas_totais['anos_processados']}")
    print(f"❌ Anos com falha: {estatisticas_totais['anos_falhados']}")
    print(f"📊 Total de arquivos processados: {estatisticas_totais['total_arquivos_processados']}")
    print(f"⏱️ Tempo total: {formatar_tempo(tempo_total)}")
    print(f"💾 Espaço total economizado: {estatisticas_totais['espaco_total_economizado_gb']:.2f} GB")
    print(f"🔄 Eficiência pipeline: {formatar_tempo(estatisticas_totais['tempo_total_pipeline'])}")
    print(f"📁 Arquivos consolidados criados: {len(arquivos_consolidados)}")
    for arquivo in arquivos_consolidados:
        print(f"   📁 {arquivo}")
    print(f"{'='*60}")
    
    logger.info(f"Processamento sequencial pipeline concluído: {estatisticas_totais['anos_processados']} sucessos, {estatisticas_totais['anos_falhados']} falhas")
    
    return arquivos_consolidados

def main():
    # Parse inicial apenas para obter o nível de log
    import argparse
    parser_temp = argparse.ArgumentParser(add_help=False)
    parser_temp.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                            default='INFO')
    args_temp, _ = parser_temp.parse_known_args()
    
    # Configurar logging antes de tudo
    logger = configurar_logging(nivel_console=args_temp.log_level)
    
    # Calcular workers otimizados baseado nos recursos da máquina
    workers_padrao = calcular_workers_otimizados()
    
    parser = argparse.ArgumentParser(
        description='Processamento de arquivos da RAIS',
        epilog="""
Exemplos de uso:
  %(prog)s --modo completo --anos 2015 2017 2020          # Anos específicos
  %(prog)s --modo completo --faixa-anos 2010 2019         # Intervalo de anos (2010 a 2019)
  %(prog)s --modo baixar --faixa-anos 2015 2020           # Baixar intervalo específico
  %(prog)s --verificar-7z                                 # Verificar integridade dos arquivos
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--anos', type=int, nargs='+', help='Anos específicos para processar (ex: 2015 2016). Use --faixa-anos para intervalos contínuos')
    parser.add_argument('--sobrescrever', action='store_true', help='Sobrescreve arquivos existentes')
    parser.add_argument('--max', type=int, help='Número máximo de arquivos a processar')
    parser.add_argument('--pausar', type=int, default=0, help='Segundos de pausa entre cada arquivo (padrão: 0)')
    parser.add_argument('--listar', action='store_true', help='Apenas lista os anos disponíveis sem processar')
    parser.add_argument('--modo', choices=['baixar', 'descompactar', 'converter', 'consolidar', 'consolidar-geral', 'completo', 'extrair-converter'], 
                       default='descompactar',
                       help='Modo de operação: baixar, descompactar, converter, consolidar, consolidar-geral, completo (sequencial por ano) ou extrair-converter (descompactar+converter)')
    parser.add_argument('--chunksize', type=int, default=100000, 
                       help='Tamanho dos chunks para processamento de arquivos grandes (padrão: 100.000)')
    parser.add_argument('--npartitions', type=int, 
                       help='Número de partições para os arquivos Parquet (padrão: automático)')
    parser.add_argument('--preservar', action='store_true',
                       help='Preserva todos os arquivos intermediários (TXT e Parquet individuais)')
    parser.add_argument('--preservar-descompactados', action='store_true',
                       help='Preserva apenas arquivos TXT descompactados, removendo Parquets intermediários para economizar espaço')
    parser.add_argument('--incremental', action='store_true',
                       help='Modo incremental para consolidação geral: adiciona apenas anos novos ao arquivo consolidado existente')
    parser.add_argument('--remover-anos-pos-consolidacao', action='store_true',
                       help='Remove pastas de anos individuais após consolidação geral bem-sucedida para economizar espaço em disco')
    parser.add_argument('--max-anos-paralelos', type=int, default=2,
                       help='Número máximo de anos processados simultaneamente no modo completo (padrão: 2)')
    parser.add_argument('--workers-download', type=int, default=workers_padrao[0],
                       help=f'Número de workers para download (padrão: {workers_padrao[0]} - otimizado automaticamente)')
    parser.add_argument('--workers-extract', type=int, default=workers_padrao[1],
                       help=f'Número de workers para descompactação (padrão: {workers_padrao[1]} - otimizado automaticamente)')
    parser.add_argument('--workers-convert', type=int, default=workers_padrao[2],
                       help=f'Número de workers para conversão (padrão: {workers_padrao[2]} - otimizado automaticamente)')
    parser.add_argument('--baixar-opcao', choices=['atual', 'todos', 'faltantes'], default='faltantes',
                       help='Opção de download: atual (ano mais recente), todos (todos os anos), faltantes (apenas não baixados)')
    parser.add_argument('--faixa-anos', type=int, nargs=2, metavar=('ANO_INICIO', 'ANO_FIM'),
                       help='Faixa de anos para processar (ex: --faixa-anos 2010 2019)')
    parser.add_argument('--pular-download', action='store_true',
                       help='Pula a etapa de download no modo completo (usa arquivos já baixados)')
    parser.add_argument('--auto-workers', action='store_true', default=True,
                       help='Calcula automaticamente o número de workers baseado nos recursos da máquina (padrão: ativo)')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                       default='INFO', help='Nível de log para o console (padrão: INFO)')
    parser.add_argument('--relatorio-problemas', action='store_true',
                       help='Gera relatório dos arquivos com problemas a partir dos logs existentes')
    parser.add_argument('--verificar-7z', action='store_true',
                       help='Verifica a integridade de todos os arquivos 7z antes do processamento')
    parser.add_argument('--auto-redownload', action='store_true', default=True,
                       help='Ativa re-download automático de arquivos corrompidos durante descompactação (padrão: ativo)')
    parser.add_argument('--max-tentativas-download', type=int, default=3,
                       help='Número máximo de tentativas para download de arquivos (padrão: 3)')
    parser.add_argument('--tempo-espera-download', type=int, default=5,
                       help='Tempo base de espera entre tentativas de download em segundos (padrão: 5)')

    args = parser.parse_args()
    
    # Log dos argumentos recebidos
    logger.info(f"Argumentos da linha de comando: {vars(args)}")
    
    # Atualizar configurações globais de tolerância a erros
    config_tolerancia['max_tentativas'] = args.max_tentativas_download
    config_tolerancia['tempo_espera_base'] = args.tempo_espera_download
    
    # Se a opção --relatorio-problemas foi especificada
    if args.relatorio_problemas:
        gerar_relatorio_problemas_logs()
        return
    
    # Se a opção --verificar-7z foi especificada
    if args.verificar_7z:
        verificar_integridade_arquivos_7z()
        return
    
    # Se a opção --listar foi especificada
    if args.listar:
        if args.modo == 'baixar':
            anos_ftp = listar_anos_disponiveis_ftp()
            print(f"Anos disponíveis no FTP: {', '.join(map(str, sorted(anos_ftp)))}")
        else:
            diretorio_origem = Path('dados-abertos-zip')
            anos_disponiveis = [d.name for d in diretorio_origem.iterdir() if d.is_dir()]
            print(f"Anos disponíveis localmente: {', '.join(sorted(anos_disponiveis))}")
        return
    
    # Validar argumentos mutuamente exclusivos
    if args.faixa_anos and args.anos:
        print(f"❌ Erro: Não é possível usar --anos e --faixa-anos simultaneamente")
        print(f"   Use --anos para anos específicos: --anos 2015 2017 2020")
        print(f"   Use --faixa-anos para intervalos: --faixa-anos 2015 2020")
        return
    
    # Determinar anos para processar baseado nos argumentos
    anos_processar = None
    
    if args.faixa_anos:
        # Gerar lista de anos da faixa especificada
        ano_inicio, ano_fim = args.faixa_anos
        ano_atual = datetime.now().year
        
        # Validações
        if ano_inicio > ano_fim:
            print(f"❌ Erro: Ano inicial ({ano_inicio}) não pode ser maior que ano final ({ano_fim})")
            return
        
        if ano_inicio < 1985:
            print(f"❌ Erro: Ano inicial ({ano_inicio}) muito antigo. A RAIS está disponível a partir de 1985.")
            return
        
        if ano_fim > ano_atual:
            print(f"❌ Erro: Ano final ({ano_fim}) é futuro. Dados disponíveis até {ano_atual}.")
            return
        
        if ano_fim - ano_inicio > 20:
            print(f"⚠️  Aviso: Faixa muito ampla ({ano_fim - ano_inicio + 1} anos). Isso pode demorar muito.")
            resposta = input("Deseja continuar? (s/N): ").lower().strip()
            if resposta not in ['s', 'sim', 'y', 'yes']:
                print("Operação cancelada pelo usuário.")
                return
        
        anos_processar = list(range(ano_inicio, ano_fim + 1))
        print(f"📅 Faixa de anos especificada: {ano_inicio} a {ano_fim} ({len(anos_processar)} anos)")
        print(f"   Anos que serão processados: {anos_processar}")
    elif args.anos:
        anos_processar = args.anos
        print(f"📅 Anos específicos: {anos_processar}")
    
    # Determinar anos para download se necessário
    anos_download = None
    if args.modo in ['baixar', 'completo'] and not args.pular_download:
        if anos_processar:
            anos_download = anos_processar
        else:
            anos_download = args.baixar_opcao
    
    # Iniciar tempo total de execução
    iniciar_tempo("TEMPO TOTAL")
    
    # Executar o modo selecionado
    logger.info(f"EXECUTANDO MODO: {args.modo.upper()}")
    
    if args.modo == 'baixar':
        logger.info("Iniciando modo BAIXAR")
        baixar_dados_ftp(anos=anos_download, sobrescrever=args.sobrescrever, 
                        max_workers=args.workers_download)
        logger.info("Modo BAIXAR concluído")
    
    elif args.modo in ['descompactar', 'extrair-converter']:
        logger.info("Iniciando etapa de DESCOMPACTAÇÃO")
        anos_validos_descompactacao = descompactar_arquivos(anos=anos_processar, sobrescrever=args.sobrescrever, 
                                                           max_arquivos=args.max, pausar=args.pausar, 
                                                           max_workers=args.workers_extract, auto_redownload=args.auto_redownload)
        logger.info("Etapa de DESCOMPACTAÇÃO concluída")
    
    if args.modo in ['converter', 'extrair-converter']:
        logger.info("Iniciando etapa de CONVERSÃO")
        # Usar anos válidos da descompactação se disponível
        anos_para_converter = anos_validos_descompactacao if 'anos_validos_descompactacao' in locals() else None
        anos_validos_conversao = converter_para_parquet(anos=anos_processar, chunksize=args.chunksize,
                                                       sobrescrever=args.sobrescrever, max_arquivos=args.max,
                                                       npartitions=args.npartitions, max_workers=args.workers_convert,
                                                       anos_validos=anos_para_converter)
        logger.info("Etapa de CONVERSÃO concluída")
    
    if args.modo == 'consolidar':
        logger.info("Iniciando etapa de CONSOLIDAÇÃO")
        consolidar_parquets(anos=anos_processar, sobrescrever=args.sobrescrever,
                           preservar_arquivos=args.preservar, 
                           preservar_descompactados=getattr(args, 'preservar_descompactados', False))
        logger.info("Etapa de CONSOLIDAÇÃO concluída")
    
    if args.modo == 'completo':
        logger.info("Iniciando modo COMPLETO - utilizando processamento sequencial com pipeline assíncrono")
        
        # PROCESSAMENTO SEQUENCIAL COM PIPELINE ASSÍNCRONO
        # Processa um ano por vez, mas com pipeline interno: download+descompactar → converter → consolidar → limpar
        logger.info("Iniciando processamento sequencial com pipeline assíncrono")
        processar_sequencial_pipeline_otimizado(anos=anos_processar, sobrescrever=args.sobrescrever,
                                               max_arquivos=args.max, pausar=args.pausar,
                                               npartitions=args.npartitions, 
                                               preservar_arquivos=args.preservar,
                                               preservar_descompactados=getattr(args, 'preservar_descompactados', False),
                                               max_workers_pipeline=args.workers_extract)
        logger.info("Modo COMPLETO concluído")
    
    if args.modo == 'consolidar-geral':
        logger.info("Iniciando modo CONSOLIDAÇÃO GERAL")
        consolidar_geral(sobrescrever=args.sobrescrever, 
                        incremental=getattr(args, 'incremental', False),
                        remover_anos_individuais=getattr(args, 'remover_anos_pos_consolidacao', False))
        logger.info("Modo CONSOLIDAÇÃO GERAL concluído")
    

    
    # Finalizar tempo total
    finalizar_tempo("TEMPO TOTAL")
    
    # Mostrar resumo dos tempos
    mostrar_resumo_tempos()
    
    # Gerar relatório final detalhado de arquivos pulados
    gerar_relatorio_final_arquivos_pulados()
    
    # Gerar relatório técnico de erros nos logs
    if controlador_erros.anos_com_falhas:
        # Salvar relatório técnico nos logs
        controlador_erros.salvar_relatorio_erros()
    
    # Log final
    logger.info("PROCESSAMENTO FINALIZADO COM SUCESSO")
    logger.info("="*80)

if __name__ == "__main__":
    # Proteção para multiprocessing no Windows
    import multiprocessing
    multiprocessing.freeze_support()
    main()
