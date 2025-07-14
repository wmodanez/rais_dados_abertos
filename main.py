import os
import py7zr
import shutil
from pathlib import Path
import argparse
from tqdm import tqdm
import time
import dask.dataframe as dd
import pandas as pd
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

def calcular_workers_otimizados():
    """
    Calcula valores otimizados de workers baseado nos recursos da máquina.
    
    Retorna:
    - Tupla com (workers_download, workers_extract, workers_convert)
    """
    # Obter informações do sistema
    cpu_count = psutil.cpu_count(logical=True) or 4  # CPUs lógicos (com hyperthreading)
    cpu_physical = psutil.cpu_count(logical=False) or cpu_count // 2  # CPUs físicos
    memoria_gb = psutil.virtual_memory().total / (1024**3)  # RAM em GB
    
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
    Baixa um arquivo específico do FTP.
    
    Parâmetros:
    - args: tupla contendo (ano, nome_arquivo, pasta_destino, sobrescrever)
    
    Retorna:
    - True se sucesso, False caso contrário
    """
    ano, nome_arquivo, pasta_destino, sobrescrever = args
    
    arquivo_local = pasta_destino / nome_arquivo
    
    # Verificar se o arquivo já existe
    if arquivo_local.exists() and not sobrescrever:
        return f"Pulando {nome_arquivo} - já existe localmente"
    
    ftp = conectar_ftp()
    if not ftp:
        return f"Erro ao conectar ao FTP para {nome_arquivo}"
    
    try:
        # Navegar para o diretório do ano
        ftp.cwd(str(ano))
        
        # Baixar o arquivo
        with open(arquivo_local, 'wb') as f:
            ftp.retrbinary(f'RETR {nome_arquivo}', f.write)
        
        ftp.quit()
        return f"✓ {nome_arquivo} baixado com sucesso"
        
    except Exception as e:
        if ftp:
            ftp.quit()
        return f"✗ Erro ao baixar {nome_arquivo}: {str(e)}"

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
    print("=== INICIANDO DOWNLOAD DOS DADOS ===")
    
    # Criar diretório de destino
    pasta_destino = Path('dados-abertos-zip')
    if not pasta_destino.exists():
        pasta_destino.mkdir(parents=True)
    
    # Obter anos disponíveis no FTP
    anos_disponiveis = listar_anos_disponiveis_ftp()
    if not anos_disponiveis:
        print("Nenhum ano disponível no FTP")
        return []
    
    print(f"Anos disponíveis no FTP: {anos_disponiveis}")
    
    # Determinar quais anos baixar
    anos_para_baixar = []
    
    if anos == 'atual':
        anos_para_baixar = [max(anos_disponiveis)]
    elif anos == 'todos':
        anos_para_baixar = anos_disponiveis
    elif isinstance(anos, tuple) and len(anos) == 2:
        ano_inicio, ano_fim = anos
        anos_para_baixar = [a for a in anos_disponiveis if ano_inicio <= a <= ano_fim]
    elif isinstance(anos, list):
        anos_para_baixar = [a for a in anos if a in anos_disponiveis]
    else:
        # Baixar apenas anos que não foram baixados ainda
        anos_existentes = [int(d.name) for d in pasta_destino.iterdir() if d.is_dir() and d.name.isdigit()]
        anos_para_baixar = [a for a in anos_disponiveis if a not in anos_existentes]
    
    if not anos_para_baixar:
        print("Nenhum ano para baixar")
        return []
    
    print(f"Anos que serão baixados: {anos_para_baixar}")
    
    # Preparar lista de downloads
    downloads = []
    for ano in anos_para_baixar:
        # Criar pasta do ano
        pasta_ano = pasta_destino / str(ano)
        if not pasta_ano.exists():
            pasta_ano.mkdir(parents=True)
        
        # Listar arquivos do ano
        arquivos_ano = listar_arquivos_ftp_ano(ano)
        
        # Filtrar arquivos que não contenham "EST" ou "NI" no nome
        arquivos_filtrados = [arq for arq in arquivos_ano 
                             if "EST" not in arq.upper() and "NI" not in arq.upper()]
        
        print(f"Ano {ano}: {len(arquivos_filtrados)} arquivos para baixar")
        
        for arquivo in arquivos_filtrados:
            downloads.append((ano, arquivo, pasta_ano, sobrescrever))
    
    if not downloads:
        print("Nenhum arquivo para baixar")
        return []
    
    print(f"Total de {len(downloads)} arquivos para baixar")
    
    # Download em paralelo
    arquivos_baixados = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submeter todos os downloads
        future_to_download = {executor.submit(baixar_arquivo_ftp, download): download 
                             for download in downloads}
        
        # Processar resultados com barra de progresso
        with tqdm(total=len(downloads), desc="Baixando arquivos", unit="arquivo") as pbar:
            for future in as_completed(future_to_download):
                download = future_to_download[future]
                try:
                    resultado = future.result()
                    ano, nome_arquivo, _, _ = download
                    
                    if "✓" in resultado:
                        arquivos_baixados.append(f"{ano}/{nome_arquivo}")
                    
                    tqdm.write(resultado)
                    pbar.update(1)
                    
                except Exception as e:
                    ano, nome_arquivo, _, _ = download
                    tqdm.write(f"✗ Erro no download {ano}/{nome_arquivo}: {str(e)}")
                    pbar.update(1)
    
    print(f"\nDownload concluído. {len(arquivos_baixados)} arquivos baixados com sucesso.")
    return arquivos_baixados

def descompactar_arquivo_worker(args):
    """
    Worker para descompactar um arquivo específico.
    
    Parâmetros:
    - args: tupla contendo (arquivo_7z, pasta_destino, sobrescrever)
    
    Retorna:
    - Resultado da operação
    """
    arquivo_7z, pasta_destino, sobrescrever = args
    
    try:
        # Verificar se o arquivo já foi descompactado
        if verificar_arquivo_extraido(arquivo_7z, pasta_destino) and not sobrescrever:
            return f"Pulando {arquivo_7z.name} - já existe no destino"
        
        with py7zr.SevenZipFile(arquivo_7z, mode='r') as z:
            z.extractall(path=pasta_destino)
        
        return f"✓ {arquivo_7z.name} descompactado com sucesso"
        
    except Exception as e:
        return f"✗ Erro ao descompactar {arquivo_7z.name}: {str(e)}"

def descompactar_arquivos(anos=None, sobrescrever=False, max_arquivos=None, pausar=0, max_workers=4):
    """
    Descompacta todos os arquivos .7z das subpastas de dados-abertos-zip
    para a pasta dados-abertos, mantendo a mesma estrutura de diretórios.
    
    Parâmetros:
    - anos: Lista de anos para processar (ex: [2015, 2016]). Se None, processa todos.
    - sobrescrever: Se True, sobrescreve arquivos existentes no destino.
    - max_arquivos: Número máximo de arquivos a processar (None para todos).
    - pausar: Segundos de pausa entre cada arquivo (para evitar sobrecarga).
    - max_workers: Número máximo de workers para processamento paralelo.
    """
    diretorio_origem = Path('dados-abertos-zip')
    diretorio_destino = Path('dados-abertos')
    
    # Criar o diretório de destino se não existir
    if not diretorio_destino.exists():
        diretorio_destino.mkdir(parents=True)
    
    # Filtrar as pastas de anos se especificado
    diretorios_anos = [d for d in diretorio_origem.iterdir() if d.is_dir()]
    
    # Converter anos para strings para comparação
    anos_str = [str(ano) for ano in anos] if anos else None
    
    if anos_str:
        # Filtrar apenas os anos solicitados
        diretorios_anos = [d for d in diretorios_anos if d.name in anos_str]
        print(f"Filtrando apenas os anos: {', '.join(anos_str)}")
        
    if not diretorios_anos:
        print("Nenhuma pasta de ano encontrada para processar.")
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
        
        # Filtrar arquivos que não contenham "EST" ou "NI" no nome
        arquivos_7z = [arq for arq in arquivos_7z if "EST" not in arq.stem.upper() and "NI" not in arq.stem.upper()]
        
        if not arquivos_7z:
            print(f"  Nenhum arquivo .7z válido encontrado em {ano_dir.name}")
            continue
            
        print(f"  Encontrados {len(arquivos_7z)} arquivos para descompactar")
        
        # Adicionar às tarefas (limitar se especificado)
        for arquivo_7z in arquivos_7z:
            if max_arquivos is not None and len(tarefas_descompactacao) >= max_arquivos:
                break
            tarefas_descompactacao.append((arquivo_7z, pasta_destino, sobrescrever))
        
        if max_arquivos is not None and len(tarefas_descompactacao) >= max_arquivos:
            break
    
    if not tarefas_descompactacao:
        print("Nenhum arquivo para descompactar")
        return
    
    print(f"\nIniciando descompactação de {len(tarefas_descompactacao)} arquivos...")
    
    # Descompactação em paralelo
    total_processados = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submeter todas as tarefas
        future_to_task = {executor.submit(descompactar_arquivo_worker, task): task 
                         for task in tarefas_descompactacao}
        
        # Processar resultados com barra de progresso
        with tqdm(total=len(tarefas_descompactacao), desc="Descompactando", unit="arquivo") as pbar:
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                try:
                    resultado = future.result()
                    
                    if "✓" in resultado:
                        total_processados += 1
                    
                    tqdm.write(resultado)
                    pbar.update(1)
                    
                    # Pausa se especificada
                    if pausar > 0:
                        time.sleep(pausar)
                        
                except Exception as e:
                    arquivo_7z, _, _ = task
                    tqdm.write(f"✗ Erro inesperado ao descompactar {arquivo_7z.name}: {str(e)}")
                    pbar.update(1)
    
    print(f"\nDescompactação concluída. Total de {total_processados} arquivos processados com sucesso.")

def converter_arquivo_worker(args):
    """
    Worker para converter um arquivo TXT para Parquet.
    
    Parâmetros:
    - args: tupla contendo (arquivo_txt, pasta_destino, sobrescrever, npartitions)
    
    Retorna:
    - Resultado da operação
    """
    arquivo_txt, pasta_destino, sobrescrever, npartitions = args
    
    try:
        # Nome do arquivo de saída
        arquivo_parquet = pasta_destino / f"{arquivo_txt.stem}.parquet"
        
        # Verificar se o arquivo já foi convertido
        if arquivo_parquet.exists() and not sobrescrever:
            return f"Pulando {arquivo_txt.name} - já existe no destino"
        
        # Detectar o separador
        with open(arquivo_txt, 'r', encoding='latin1', errors='ignore') as f:
            primeira_linha = f.readline().strip()
        separador = ';' if ';' in primeira_linha else ','
        
        # Ler amostra para obter nomes das colunas
        amostra = pd.read_csv(
            arquivo_txt, 
            sep=separador, 
            encoding='latin1', 
            nrows=1,
            low_memory=False,
            on_bad_lines='skip'
        )
        
        # Limpar nomes das colunas
        colunas_originais = amostra.columns.tolist()
        colunas_limpas = [limpar_nome_coluna(col) for col in colunas_originais]
        mapeamento_colunas = dict(zip(colunas_originais, colunas_limpas))
        
        # Definir tipos como string
        tipos = {col: 'object' for col in colunas_originais}
        
        # Ler com Dask
        df = dd.read_csv(
            arquivo_txt,
            sep=separador,
            encoding='latin1',
            dtype=tipos,
            blocksize="128MB",
            assume_missing=True,
            on_bad_lines='skip'
        )
        
        # Renomear colunas
        df = df.rename(columns=mapeamento_colunas)
        
        # Reparticionar se especificado
        if npartitions:
            df = df.repartition(npartitions=npartitions)
        
        # Salvar como Parquet
        df.to_parquet(
            arquivo_parquet,
            compression='snappy',
            write_index=False,
            engine='pyarrow'
        )
        
        return f"✓ {arquivo_txt.name} convertido com sucesso para Parquet"
        
    except Exception as e:
        return f"✗ Erro ao converter {arquivo_txt.name}: {str(e)}"

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
    return nome_limpo

def converter_para_parquet(anos=None, chunksize=100000, sobrescrever=False, max_arquivos=None, npartitions=None, max_workers=2):
    """
    Converte arquivos TXT da pasta dados-abertos para o formato Parquet na pasta parquet.
    
    Parâmetros:
    - anos: Lista de anos para processar (ex: [2015, 2016]). Se None, processa todos.
    - chunksize: Tamanho dos chunks para processamento (padrão: 100.000 linhas)
    - sobrescrever: Se True, sobrescreve arquivos Parquet existentes.
    - max_arquivos: Número máximo de arquivos a processar (None para todos).
    - npartitions: Número de partições para os arquivos Parquet (None para automático)
    - max_workers: Número máximo de workers para processamento paralelo.
    """
    diretorio_origem = Path('dados-abertos')
    diretorio_destino = Path('parquet')
    
    # Criar o diretório de destino se não existir
    if not diretorio_destino.exists():
        diretorio_destino.mkdir(parents=True)
    
    # Filtrar as pastas de anos se especificado
    diretorios_anos = [d for d in diretorio_origem.iterdir() if d.is_dir()]
    
    # Converter anos para strings para comparação
    anos_str = [str(ano) for ano in anos] if anos else None
    
    if anos_str:
        # Filtrar apenas os anos solicitados
        diretorios_anos = [d for d in diretorios_anos if d.name in anos_str]
        print(f"Filtrando apenas os anos: {', '.join(anos_str)}")
        
    if not diretorios_anos:
        print("Nenhuma pasta de ano encontrada para processar.")
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
        
        # Filtrar arquivos que não contenham "EST" ou "NI" no nome
        arquivos_txt = [arq for arq in arquivos_txt if "EST" not in arq.stem.upper() and "NI" not in arq.stem.upper()]
        
        if not arquivos_txt:
            print(f"  Nenhum arquivo TXT válido encontrado em {ano_dir.name}")
            continue
            
        print(f"  Encontrados {len(arquivos_txt)} arquivos para converter")
        
        # Adicionar às tarefas (limitar se especificado)
        for arquivo_txt in arquivos_txt:
            if max_arquivos is not None and len(tarefas_conversao) >= max_arquivos:
                break
            tarefas_conversao.append((arquivo_txt, pasta_destino, sobrescrever, npartitions))
        
        if max_arquivos is not None and len(tarefas_conversao) >= max_arquivos:
            break
    
    if not tarefas_conversao:
        print("Nenhum arquivo para converter")
        return
    
    print(f"\nIniciando conversão de {len(tarefas_conversao)} arquivos...")
    
    # Conversão em paralelo (usar menos workers devido ao uso intensivo de memória)
    total_processados = 0
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Submeter todas as tarefas
        future_to_task = {executor.submit(converter_arquivo_worker, task): task 
                         for task in tarefas_conversao}
        
        # Processar resultados com barra de progresso
        with tqdm(total=len(tarefas_conversao), desc="Convertendo", unit="arquivo") as pbar:
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                try:
                    resultado = future.result()
                    
                    if "✓" in resultado:
                        total_processados += 1
                    
                    tqdm.write(resultado)
                    pbar.update(1)
                        
                except Exception as e:
                    arquivo_txt, _, _, _ = task
                    tqdm.write(f"✗ Erro inesperado ao converter {arquivo_txt.name}: {str(e)}")
                    pbar.update(1)
    
    print(f"\nConversão concluída. Total de {total_processados} arquivos convertidos com sucesso.")

def consolidar_parquets(anos=None, sobrescrever=False, preservar_arquivos=False):
    """
    Consolida todos os arquivos Parquet de um ano em um único arquivo Parquet.
    
    Parâmetros:
    - anos: Lista de anos para processar (ex: [2015, 2016]). Se None, processa todos.
    - sobrescrever: Se True, sobrescreve arquivos Parquet consolidados existentes.
    - preservar_arquivos: Se True, mantém os arquivos intermediários (TXT e Parquet individuais).
    
    Retorna:
    - Lista de arquivos Parquet consolidados criados
    """
    diretorio_origem = Path('parquet')
    
    # Filtrar as pastas de anos se especificado
    diretorios_anos = [d for d in diretorio_origem.iterdir() if d.is_dir()]
    
    # Converter anos para strings para comparação
    anos_str = [str(ano) for ano in anos] if anos else None
    
    if anos_str:
        # Filtrar apenas os anos solicitados
        diretorios_anos = [d for d in diretorios_anos if d.name in anos_str]
        print(f"Filtrando apenas os anos: {', '.join(anos_str)}")
        
    if not diretorios_anos:
        print("Nenhuma pasta de ano encontrada para processar.")
        return []
        
    print(f"Encontradas {len(diretorios_anos)} pastas de anos para consolidar: {[d.name for d in diretorios_anos]}")
    
    arquivos_consolidados = []
    
    # Processar cada pasta de ano
    for ano_dir in diretorios_anos:
        ano = ano_dir.name
        print(f"\nConsolidando arquivos do ano {ano}")
        
        # Nome do arquivo consolidado - diretamente na pasta do ano
        arquivo_consolidado = ano_dir / f"RAIS_{ano}_consolidado"
        
        # Verificar se o arquivo já foi consolidado
        if arquivo_consolidado.exists() and not sobrescrever:
            print(f"  Pulando {arquivo_consolidado.name} - já existe (use --sobrescrever para forçar)")
            continue
        
        # Listar todos os diretórios Parquet na pasta do ano
        diretorios_parquet = [d for d in ano_dir.glob('*.parquet') if d.is_dir() and "consolidado" not in d.name]
        
        # Filtrar diretórios que não contenham "EST" ou "NI" no nome
        diretorios_parquet = [d for d in diretorios_parquet if "EST" not in d.stem.upper() and "NI" not in d.stem.upper()]
        
        if not diretorios_parquet:
            print(f"  Nenhum diretório Parquet válido encontrado em {ano}")
            continue
            
        print(f"  Encontrados {len(diretorios_parquet)} arquivos Parquet para consolidar")
        
        try:
            # Lista para armazenar os DataFrames
            dfs = []
            
            # Processar todos os diretórios Parquet com barra de progresso
            for dir_parquet in tqdm(diretorios_parquet, desc=f"  Lendo arquivos Parquet do ano {ano}", unit="arquivo"):
                try:
                    # Ler o arquivo Parquet com Dask
                    df = dd.read_parquet(dir_parquet)
                    
                    # Adicionar coluna com o nome do arquivo para identificação
                    df['fonte_arquivo'] = dir_parquet.stem
                    
                    # Adicionar à lista
                    dfs.append(df)
                except Exception as e:
                    tqdm.write(f"  ✗ Erro ao ler {dir_parquet.name}: {str(e)}")
            
            if not dfs:
                print(f"  Nenhum arquivo Parquet válido encontrado para o ano {ano}")
                continue
                
            # Concatenar todos os DataFrames
            print(f"  Concatenando {len(dfs)} DataFrames...")
            df_consolidado = dd.concat(dfs)
            
            # Salvar o DataFrame consolidado
            print(f"  Salvando arquivo consolidado {arquivo_consolidado}...")
            df_consolidado.to_parquet(
                arquivo_consolidado,
                compression='snappy',
                write_index=False,
                engine='pyarrow'
            )
            
            print(f"  ✓ Arquivo consolidado {arquivo_consolidado.name} criado com sucesso")
            arquivos_consolidados.append(arquivo_consolidado)
            
            # Limpar arquivos intermediários se solicitado
            if not preservar_arquivos:
                # Remover arquivos TXT
                print(f"  Removendo arquivos TXT intermediários...")
                diretorio_txt = Path('dados-abertos') / ano
                if diretorio_txt.exists():
                    arquivos_txt = list(diretorio_txt.glob('*.txt'))
                    arquivos_txt = [arq for arq in arquivos_txt if "EST" not in arq.stem.upper() and "NI" not in arq.stem.upper()]
                    for arquivo in arquivos_txt:
                        try:
                            arquivo.unlink()
                        except Exception as e:
                            print(f"    Erro ao remover {arquivo}: {str(e)}")
                
                # Remover diretórios Parquet individuais
                print(f"  Removendo arquivos Parquet intermediários...")
                for dir_parquet in diretorios_parquet:
                    try:
                        shutil.rmtree(dir_parquet)
                    except Exception as e:
                        print(f"    Erro ao remover {dir_parquet}: {str(e)}")
            
        except Exception as e:
            print(f"  ✗ Erro ao consolidar arquivos do ano {ano}: {str(e)}")
            import traceback
            print(traceback.format_exc())
    
    return arquivos_consolidados

def processar_completo(anos=None, sobrescrever=False, max_arquivos=None, 
                      pausar=0, npartitions=None, preservar_arquivos=False, 
                      max_workers_download=4, max_workers_extract=4, max_workers_convert=2,
                      pular_download=False):
    """
    Realiza o processamento completo: baixa, descompacta, converte para Parquet e consolida.
    
    Parâmetros:
    - anos: Lista de anos para processar (ex: [2015, 2016]). Se None, processa todos.
    - sobrescrever: Se True, sobrescreve arquivos existentes.
    - max_arquivos: Número máximo de arquivos a processar (None para todos).
    - pausar: Segundos de pausa entre cada arquivo (para evitar sobrecarga).
    - npartitions: Número de partições para os arquivos Parquet (None para automático).
    - preservar_arquivos: Se True, mantém os arquivos intermediários.
    - max_workers_download: Workers para download.
    - max_workers_extract: Workers para descompactação.
    - max_workers_convert: Workers para conversão.
    - pular_download: Se True, pula a etapa de download.
    """
    print("=== INICIANDO PROCESSAMENTO COMPLETO ===")
    
    # Etapa 1: Baixar arquivos (se não for para pular)
    if not pular_download:
        print("\n=== ETAPA 1: BAIXANDO ARQUIVOS ===")
        baixar_dados_ftp(anos=anos, sobrescrever=sobrescrever, max_workers=max_workers_download)
    else:
        print("\n=== ETAPA 1: PULANDO DOWNLOAD (conforme solicitado) ===")
    
    # Etapa 2: Descompactar arquivos
    print("\n=== ETAPA 2: DESCOMPACTANDO ARQUIVOS ===")
    descompactar_arquivos(anos=anos, sobrescrever=sobrescrever, 
                         max_arquivos=max_arquivos, pausar=pausar, max_workers=max_workers_extract)
    
    # Etapa 3: Converter para Parquet
    print("\n=== ETAPA 3: CONVERTENDO PARA PARQUET ===")
    converter_para_parquet(anos=anos, sobrescrever=sobrescrever, 
                          max_arquivos=max_arquivos, npartitions=npartitions, max_workers=max_workers_convert)
    
    # Etapa 4: Consolidar arquivos Parquet
    print("\n=== ETAPA 4: CONSOLIDANDO ARQUIVOS PARQUET ===")
    arquivos_consolidados = consolidar_parquets(anos=anos, sobrescrever=sobrescrever,
                                              preservar_arquivos=preservar_arquivos)
    
    # Resumo final
    print("\n=== PROCESSAMENTO COMPLETO FINALIZADO ===")
    print(f"Total de arquivos consolidados: {len(arquivos_consolidados)}")
    for arquivo in arquivos_consolidados:
        print(f"  - {arquivo}")
    
    if preservar_arquivos:
        print("\nArquivos intermediários foram preservados.")
    else:
        print("\nArquivos intermediários foram removidos.")

def main():
    # Calcular workers otimizados baseado nos recursos da máquina
    workers_padrao = calcular_workers_otimizados()
    
    parser = argparse.ArgumentParser(description='Processamento de arquivos da RAIS')
    parser.add_argument('--anos', type=int, nargs='+', help='Anos específicos para processar (ex: 2015 2016)')
    parser.add_argument('--sobrescrever', action='store_true', help='Sobrescreve arquivos existentes')
    parser.add_argument('--max', type=int, help='Número máximo de arquivos a processar')
    parser.add_argument('--pausar', type=int, default=0, help='Segundos de pausa entre cada arquivo (padrão: 0)')
    parser.add_argument('--listar', action='store_true', help='Apenas lista os anos disponíveis sem processar')
    parser.add_argument('--modo', choices=['baixar', 'descompactar', 'converter', 'consolidar', 'completo', 'extrair-converter', 'processar'], 
                       default='descompactar',
                       help='Modo de operação: baixar, descompactar, converter, consolidar, completo, extrair-converter (descompactar+converter) ou processar (descompactar+converter+consolidar)')
    parser.add_argument('--chunksize', type=int, default=100000, 
                       help='Tamanho dos chunks para processamento de arquivos grandes (padrão: 100.000)')
    parser.add_argument('--npartitions', type=int, 
                       help='Número de partições para os arquivos Parquet (padrão: automático)')
    parser.add_argument('--preservar', action='store_true',
                       help='Preserva os arquivos intermediários (TXT e Parquet individuais)')
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
    args = parser.parse_args()
    
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
    
    # Executar o modo selecionado
    if args.modo == 'baixar':
        baixar_dados_ftp(anos=anos_download, sobrescrever=args.sobrescrever, 
                        max_workers=args.workers_download)
    
    elif args.modo in ['descompactar', 'extrair-converter', 'processar']:
        descompactar_arquivos(anos=args.anos, sobrescrever=args.sobrescrever, 
                             max_arquivos=args.max, pausar=args.pausar, 
                             max_workers=args.workers_extract)
    
    if args.modo in ['converter', 'extrair-converter', 'processar']:
        converter_para_parquet(anos=args.anos, chunksize=args.chunksize,
                              sobrescrever=args.sobrescrever, max_arquivos=args.max,
                              npartitions=args.npartitions, max_workers=args.workers_convert)
    
    if args.modo in ['consolidar', 'processar']:
        consolidar_parquets(anos=args.anos, sobrescrever=args.sobrescrever,
                           preservar_arquivos=args.preservar)
    
    if args.modo == 'completo':
        processar_completo(anos=args.anos, sobrescrever=args.sobrescrever,
                          max_arquivos=args.max, pausar=args.pausar,
                          npartitions=args.npartitions, preservar_arquivos=args.preservar,
                          max_workers_download=args.workers_download,
                          max_workers_extract=args.workers_extract,
                          max_workers_convert=args.workers_convert,
                          pular_download=args.pular_download)

if __name__ == "__main__":
    # Proteção para multiprocessing no Windows
    import multiprocessing
    multiprocessing.freeze_support()
    main()
