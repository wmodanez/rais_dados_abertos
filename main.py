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
        - etapa: Etapa do processamento (descompactacao, conversao, consolidacao)
        - arquivo: Nome do arquivo que causou o erro
        - erro: Descrição do erro
        """
        with self.lock:
            if ano not in self.erros_por_ano:
                self.erros_por_ano[ano] = {
                    'descompactacao': [],
                    'conversao': [],
                    'consolidacao': []
                }
            
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
        Salva um relatório detalhado dos erros na pasta logs.
        """
        if not self.erros_por_ano:
            return None
            
        # Criar pasta logs se não existir
        pasta_logs = Path('logs')
        pasta_logs.mkdir(exist_ok=True)
        
        # Nome do arquivo com timestamp
        timestamp = datetime.now().strftime('%Y_%m_%d_%H%M%S')
        arquivo_relatorio = pasta_logs / f'relatorio_erros_{timestamp}.json'
        
        # Preparar dados para salvar
        relatorio = {
            'timestamp_relatorio': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'total_anos_com_erros': len(self.anos_com_falhas),
            'anos_com_falhas': sorted(list(self.anos_com_falhas)),
            'erros_detalhados': self.erros_por_ano
        }
        
        # Salvar JSON
        with open(arquivo_relatorio, 'w', encoding='utf-8') as f:
            json.dump(relatorio, f, indent=2, ensure_ascii=False)
        
        # Criar também um relatório texto legível
        arquivo_txt = pasta_logs / f'relatorio_erros_{timestamp}.txt'
        with open(arquivo_txt, 'w', encoding='utf-8') as f:
            f.write("="*80 + "\n")
            f.write("RELATÓRIO DE ERROS - PROCESSAMENTO RAIS\n")
            f.write("="*80 + "\n")
            f.write(f"Data/Hora: {relatorio['timestamp_relatorio']}\n")
            f.write(f"Total de anos com erros: {relatorio['total_anos_com_erros']}\n")
            f.write(f"Anos afetados: {', '.join(map(str, relatorio['anos_com_falhas']))}\n")
            f.write("="*80 + "\n\n")
            
            for ano in sorted(self.anos_com_falhas):
                f.write(f"ANO {ano}:\n")
                f.write("-" * 40 + "\n")
                
                for etapa, erros in self.erros_por_ano[ano].items():
                    if erros:
                        f.write(f"\n{etapa.upper()}:\n")
                        for erro in erros:
                            f.write(f"  • {erro['timestamp']} - {erro['arquivo']}\n")
                            f.write(f"    Erro: {erro['erro']}\n")
                
                f.write("\n" + "="*80 + "\n\n")
        
        logger = logging.getLogger(__name__)
        logger.info(f"Relatório de erros salvo: {arquivo_relatorio}")
        logger.info(f"Relatório texto salvo: {arquivo_txt}")
        
        return arquivo_relatorio, arquivo_txt

def gerar_relatorio_problemas_logs():
    """
    Gera um relatório consolidado dos problemas a partir dos logs existentes.
    """
    pasta_logs = Path('logs')
    
    if not pasta_logs.exists():
        print("❌ Pasta de logs não encontrada. Execute algum processamento primeiro.")
        return
    
    # Procurar por arquivos de relatório de erros existentes
    arquivos_relatorio = list(pasta_logs.glob('relatorio_erros_*.json'))
    
    if not arquivos_relatorio:
        print("❌ Nenhum relatório de erros encontrado na pasta logs.")
        return
    
    print(f"📊 Encontrados {len(arquivos_relatorio)} relatórios de erro:")
    
    # Consolidar todos os erros
    todos_erros = {}
    anos_com_problemas = set()
    
    for arquivo in sorted(arquivos_relatorio):
        print(f"   • {arquivo.name}")
        
        try:
            with open(arquivo, 'r', encoding='utf-8') as f:
                relatorio = json.load(f)
            
            # Consolidar erros
            for ano, erros_ano in relatorio.get('erros_detalhados', {}).items():
                if ano not in todos_erros:
                    todos_erros[ano] = {
                        'descompactacao': [],
                        'conversao': [],
                        'consolidacao': []
                    }
                
                for etapa, lista_erros in erros_ano.items():
                    todos_erros[ano][etapa].extend(lista_erros)
                    if lista_erros:
                        anos_com_problemas.add(int(ano))
        
        except Exception as e:
            print(f"   ⚠️  Erro ao ler {arquivo.name}: {e}")
    
    if not anos_com_problemas:
        print("✅ Nenhum problema encontrado nos relatórios!")
        return
    
    # Gerar relatório consolidado
    timestamp = datetime.now().strftime('%Y_%m_%d_%H%M%S')
    arquivo_consolidado = pasta_logs / f'relatorio_consolidado_{timestamp}.txt'
    
    with open(arquivo_consolidado, 'w', encoding='utf-8') as f:
        f.write("="*80 + "\n")
        f.write("RELATÓRIO CONSOLIDADO DE PROBLEMAS - RAIS\n")
        f.write("="*80 + "\n")
        f.write(f"Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Total de anos com problemas: {len(anos_com_problemas)}\n")
        f.write(f"Anos afetados: {', '.join(map(str, sorted(anos_com_problemas)))}\n")
        f.write("="*80 + "\n\n")
        
        # Estatísticas por etapa
        stats_etapas = {
            'descompactacao': 0,
            'conversao': 0,
            'consolidacao': 0
        }
        
        for ano in sorted(anos_com_problemas):
            f.write(f"ANO {ano}:\n")
            f.write("-" * 40 + "\n")
            
            for etapa, erros in todos_erros[str(ano)].items():
                if erros:
                    stats_etapas[etapa] += len(erros)
                    f.write(f"\n{etapa.upper()} ({len(erros)} erros):\n")
                    
                    # Agrupar por tipo de erro para evitar repetição
                    erros_agrupados = {}
                    for erro in erros:
                        tipo_erro = erro['erro'][:100] + "..." if len(erro['erro']) > 100 else erro['erro']
                        if tipo_erro not in erros_agrupados:
                            erros_agrupados[tipo_erro] = []
                        erros_agrupados[tipo_erro].append(erro)
                    
                    for tipo_erro, lista_erros in erros_agrupados.items():
                        f.write(f"  • {tipo_erro}\n")
                        if len(lista_erros) > 3:
                            f.write(f"    Arquivos afetados: {len(lista_erros)} arquivos\n")
                            f.write(f"    Exemplos: {', '.join([e['arquivo'] for e in lista_erros[:3]])}\n")
                        else:
                            for erro in lista_erros:
                                f.write(f"    - {erro['arquivo']} ({erro['timestamp']})\n")
            
            f.write("\n" + "="*80 + "\n\n")
        
        # Resumo final
        f.write("RESUMO FINAL:\n")
        f.write("-" * 40 + "\n")
        for etapa, count in stats_etapas.items():
            if count > 0:
                f.write(f"{etapa.capitalize()}: {count} erros\n")
        
        total_erros = sum(stats_etapas.values())
        f.write(f"Total de erros: {total_erros}\n")
    
    # Mostrar resumo no console
    print(f"\n📋 RESUMO DOS PROBLEMAS:")
    print(f"   • Anos com problemas: {len(anos_com_problemas)}")
    print(f"   • Anos afetados: {', '.join(map(str, sorted(anos_com_problemas)))}")
    
    total_erros = sum(len(erros) for ano_erros in todos_erros.values() 
                     for erros in ano_erros.values())
    print(f"   • Total de erros: {total_erros}")
    
    for etapa, count in {etapa: sum(len(todos_erros[str(ano)][etapa]) 
                                   for ano in anos_com_problemas 
                                   if str(ano) in todos_erros) 
                        for etapa in ['descompactacao', 'conversao', 'consolidacao']}.items():
        if count > 0:
            print(f"   • {etapa.capitalize()}: {count} erros")
    
    print(f"\n💾 Relatório consolidado salvo em: {arquivo_consolidado}")

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
    
    # Nome do arquivo baseado na data atual
    data_hoje = datetime.now().strftime('%Y_%m_%d')
    arquivo_log = pasta_logs / f'rais_{data_hoje}.log'
    
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
                return f"❌ Erro ao baixar {nome_arquivo} após {max_tentativas} tentativas: {error_msg}"
    
    # Não deveria chegar aqui, mas por segurança
    return f"❌ Erro inesperado ao baixar {nome_arquivo}"

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
                    tqdm.write(f"❌ Erro no download {ano}/{nome_arquivo}: {str(e)}")
                    pbar.set_postfix_str(f"✅{sucesso_count} ❌{erro_count}")
                    pbar.update(1)
    
    logger.info(f"DOWNLOAD CONCLUÍDO: {len(arquivos_baixados)} arquivos baixados, {sucesso_count} sucessos, {erro_count} erros")
    print(f"\nDownload concluído. {len(arquivos_baixados)} arquivos baixados com sucesso.")
    
    finalizar_tempo("Download FTP")
    return arquivos_baixados

def descompactar_arquivo_worker(args):
    """
    Worker para descompactar um arquivo específico.
    Inclui verificação de integridade e re-download automático se necessário.
    
    Parâmetros:
    - args: tupla contendo (arquivo_7z, pasta_destino, sobrescrever, ano)
    
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
        
        # NOVA FUNCIONALIDADE: Verificar integridade antes de descompactar
        if auto_redownload:
            logger.debug(f"🔍 Verificando integridade de {arquivo_7z.name}")
            is_valid, error_message = verificar_integridade_arquivo_7z(arquivo_7z)
            
            if not is_valid:
                logger.warning(f"⚠️  Arquivo {arquivo_7z.name} com problema: {error_message}")
                logger.info(f"🔄 Iniciando re-download IMEDIATO de {arquivo_7z.name}")
                
                # Tentar baixar novamente do FTP IMEDIATAMENTE
                pasta_ano = arquivo_7z.parent
                sucesso_download = baixar_arquivo_individual(ano, arquivo_7z.name, pasta_ano, sobrescrever=True)
                
                if not sucesso_download:
                    error_msg = f"Arquivo corrompido e falha no re-download: {error_message}"
                    controlador_erros.registrar_erro(ano, 'descompactacao', arquivo_7z.name, error_msg)
                    return f"❌ {arquivo_7z.name}: {error_msg}"
                
                # Verificar novamente após o re-download
                logger.debug(f"🔍 Verificando integridade após re-download de {arquivo_7z.name}")
                is_valid_after_download, error_after_download = verificar_integridade_arquivo_7z(arquivo_7z)
                
                if not is_valid_after_download:
                    error_msg = f"Arquivo ainda corrompido após re-download: {error_after_download}"
                    controlador_erros.registrar_erro(ano, 'descompactacao', arquivo_7z.name, error_msg)
                    return f"❌ {arquivo_7z.name}: {error_msg}"
                
                logger.info(f"✅ Arquivo {arquivo_7z.name} re-baixado e verificado com sucesso")
        
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
        return f"❌ Erro ao descompactar {arquivo_7z.name}: {str(e)}"

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
                    # Registrar erro inesperado
                    controlador_erros.registrar_erro(ano, 'descompactacao', arquivo_7z.name, f"Erro inesperado: {str(e)}")
                    tqdm.write(f"❌ Erro inesperado ao descompactar {arquivo_7z.name}: {str(e)}")
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
        match_ano = re.search(r'20\d{2}', nome_arquivo)
        ano_arquivo = match_ano.group() if match_ano else None
        
        if not ano_arquivo:
            # Tenta extrair da pasta pai
            pasta_pai = arquivo_txt.parent.name
            match_ano = re.search(r'20\d{2}', pasta_pai)
            ano_arquivo = match_ano.group() if match_ano else "UNKNOWN"
        
        logger.debug(f"Ano extraído para {arquivo_txt.name}: {ano_arquivo}")
        
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
            
            # Ler amostra para obter nomes das colunas
            pbar.set_postfix_str("Analisando colunas...")
            amostra = dd.read_csv(
                arquivo_txt, 
                sep=separador, 
                encoding='latin1', 
                blocksize="1MB",  # Lê apenas um pequeno bloco
                assume_missing=True,
                on_bad_lines='skip'
            ).head(1)  # Obter apenas a primeira linha
            pbar.update(10)
            
            # Limpar nomes das colunas
            pbar.set_postfix_str("Limpando nomes das colunas...")
            colunas_originais = amostra.columns.tolist()
            colunas_limpas = [limpar_nome_coluna(col) for col in colunas_originais]
            mapeamento_colunas = dict(zip(colunas_originais, colunas_limpas))
            logger.debug(f"Arquivo {arquivo_txt.name} possui {len(colunas_originais)} colunas")
            
            # Definir tipos como string
            tipos = {col: 'object' for col in colunas_originais}
            pbar.update(10)
            
            # Ler com Dask
            pbar.set_postfix_str("Carregando dados...")
            df = dd.read_csv(
                arquivo_txt,
                sep=separador,
                encoding='latin1',
                dtype=tipos,
                blocksize="128MB",
                assume_missing=True,
                on_bad_lines='skip'
            )
            pbar.update(30)
            
            # Renomear colunas
            pbar.set_postfix_str("Processando colunas...")
            df = df.rename(columns=mapeamento_colunas)
            
            # Adicionar colunas de metadados
            df['fonte_arquivo'] = arquivo_txt.stem
            df['ANO_RAIS'] = ano_arquivo
            
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
        num_colunas = len(colunas_limpas) + 2  # +2 para fonte_arquivo e ANO_RAIS
        tamanho_parquet = arquivo_parquet.stat().st_size / (1024*1024) if arquivo_parquet.exists() else 0
        tempo_conversao = time.time() - inicio_conversao
        
        logger.info(f"Conversão concluída: {arquivo_txt.name} → {arquivo_parquet.name} ({num_colunas} colunas, {tamanho_parquet:.1f}MB em {tempo_conversao:.1f}s) - Ano: {ano_arquivo}")
        
        return f"✅ {arquivo_txt.name} → Parquet ({num_colunas} colunas, {tamanho_parquet:.1f}MB) - Ano: {ano_arquivo}"
        
    except Exception as e:
        # Registrar erro no controlador
        controlador_erros.registrar_erro(ano, 'conversao', arquivo_txt.name, str(e))
        logger.error(f"Erro durante conversão de {arquivo_txt.name}: {str(e)}")
        return f"❌ Erro ao converter {arquivo_txt.name}: {str(e)}"

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
                    # Registrar erro inesperado
                    controlador_erros.registrar_erro(ano, 'conversao', arquivo_txt.name, f"Erro inesperado: {str(e)}")
                    tqdm.write(f"❌ Erro inesperado ao converter {arquivo_txt.name}: {str(e)}")
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
                        tqdm.write(f"    ✅ {dir_parquet.stem}: {nrows:,} linhas, {ncols} colunas")
                    except:
                        tqdm.write(f"    ✅ {dir_parquet.stem}: Carregado com sucesso")
                    
                    # Adicionar coluna com o nome do arquivo para identificação (se não existir)
                    if 'fonte_arquivo' not in df.columns:
                        df['fonte_arquivo'] = dir_parquet.stem
                    
                    # Adicionar coluna do ano se não existir
                    if 'ANO_RAIS' not in df.columns:
                        df['ANO_RAIS'] = ano
                    
                    # Adicionar à lista
                    dfs.append(df)
                except Exception as e:
                    tqdm.write(f"    ❌ Erro ao ler {dir_parquet.name}: {str(e)}")
            
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
            import traceback
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
                    tqdm.write(f"  ✅ Ano {ano}: {nrows:,} linhas, {ncols} colunas")
                except:
                    tqdm.write(f"  ✅ Ano {ano}: Carregado com sucesso")
                
                # Adicionar à lista
                dfs.append(df)
                
            except Exception as e:
                tqdm.write(f"  ❌ Erro ao carregar ano {ano}: {str(e)}")
        
        if not dfs:
            print("❌ Nenhum arquivo válido encontrado para consolidação geral")
            finalizar_tempo("Consolidação Geral")
            return None
        
        # Concatenar todos os DataFrames
        print(f"\n🔗 Concatenando {len(dfs)} DataFrames de diferentes anos...")
        with tqdm(desc="🔗 Consolidando geral", unit="ano", total=len(dfs)) as pbar_concat:
            df_consolidado = dd.concat(dfs, ignore_index=True)
            pbar_concat.update(len(dfs))
        
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
                        tqdm.write(f"✅ Ano {ano} processado com sucesso")
                    else:
                        estatisticas_totais['anos_falhados'] += 1
                        tqdm.write(f"❌ Falha no processamento do ano {ano}")
                    
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
                    tqdm.write(f"❌ Erro inesperado no ano {ano}: {str(e)}")
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
    
    parser = argparse.ArgumentParser(description='Processamento de arquivos da RAIS')
    parser.add_argument('--anos', type=int, nargs='+', help='Anos específicos para processar (ex: 2015 2016)')
    parser.add_argument('--sobrescrever', action='store_true', help='Sobrescreve arquivos existentes')
    parser.add_argument('--max', type=int, help='Número máximo de arquivos a processar')
    parser.add_argument('--pausar', type=int, default=0, help='Segundos de pausa entre cada arquivo (padrão: 0)')
    parser.add_argument('--listar', action='store_true', help='Apenas lista os anos disponíveis sem processar')
    parser.add_argument('--modo', choices=['baixar', 'descompactar', 'converter', 'consolidar', 'consolidar-geral', 'completo', 'extrair-converter'], 
                       default='descompactar',
                       help='Modo de operação: baixar, descompactar, converter, consolidar, consolidar-geral, completo ou extrair-converter (descompactar+converter)')
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
                       help='Faixa de anos para baixar (ex: --faixa-anos 2015 2020)')
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
    
    # Determinar anos para download se necessário
    anos_download = None
    if args.modo in ['baixar', 'completo'] and not args.pular_download:
        if args.faixa_anos:
            anos_download = tuple(args.faixa_anos)
        elif args.anos:
            anos_download = args.anos
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
        anos_validos_descompactacao = descompactar_arquivos(anos=args.anos, sobrescrever=args.sobrescrever, 
                                                           max_arquivos=args.max, pausar=args.pausar, 
                                                           max_workers=args.workers_extract, auto_redownload=args.auto_redownload)
        logger.info("Etapa de DESCOMPACTAÇÃO concluída")
    
    if args.modo in ['converter', 'extrair-converter']:
        logger.info("Iniciando etapa de CONVERSÃO")
        # Usar anos válidos da descompactação se disponível
        anos_para_converter = anos_validos_descompactacao if 'anos_validos_descompactacao' in locals() else None
        anos_validos_conversao = converter_para_parquet(anos=args.anos, chunksize=args.chunksize,
                                                       sobrescrever=args.sobrescrever, max_arquivos=args.max,
                                                       npartitions=args.npartitions, max_workers=args.workers_convert,
                                                       anos_validos=anos_para_converter)
        logger.info("Etapa de CONVERSÃO concluída")
    
    if args.modo == 'consolidar':
        logger.info("Iniciando etapa de CONSOLIDAÇÃO")
        consolidar_parquets(anos=args.anos, sobrescrever=args.sobrescrever,
                           preservar_arquivos=args.preservar, 
                           preservar_descompactados=getattr(args, 'preservar_descompactados', False))
        logger.info("Etapa de CONSOLIDAÇÃO concluída")
    
    if args.modo == 'completo':
        logger.info("Iniciando modo COMPLETO - utilizando pipeline paralelo otimizado")
        # O modo completo agora usa o pipeline paralelo otimizado
        processar_paralelo_por_ano(anos=args.anos, sobrescrever=args.sobrescrever,
                                  max_arquivos=args.max, pausar=args.pausar,
                                  npartitions=args.npartitions, 
                                  preservar_arquivos=args.preservar,
                                  preservar_descompactados=getattr(args, 'preservar_descompactados', False),
                                  max_workers_extract=args.workers_extract,
                                  max_workers_convert=args.workers_convert,
                                  max_anos_paralelos=getattr(args, 'max_anos_paralelos', 2))
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
    
    # Gerar relatório final de erros se houver
    if controlador_erros.anos_com_falhas:
        print(f"\n⚠️  RELATÓRIO FINAL DE ERROS:")
        print(f"   📊 Total de anos com problemas: {len(controlador_erros.anos_com_falhas)}")
        print(f"   📝 Anos afetados: {sorted(controlador_erros.anos_com_falhas)}")
        
        # Salvar relatório final
        arquivos_relatorio = controlador_erros.salvar_relatorio_erros()
        if arquivos_relatorio:
            print(f"   📋 Relatório completo salvo em: {arquivos_relatorio[1]}")
            print(f"   💾 Dados técnicos em: {arquivos_relatorio[0]}")
    else:
        print("\n✅ Nenhum erro encontrado durante o processamento!")
    
    # Log final
    logger.info("PROCESSAMENTO FINALIZADO COM SUCESSO")
    logger.info("="*80)

if __name__ == "__main__":
    # Proteção para multiprocessing no Windows
    import multiprocessing
    multiprocessing.freeze_support()
    main()
