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

def descompactar_arquivos(anos=None, sobrescrever=False, max_arquivos=None, pausar=0):
    """
    Descompacta todos os arquivos .7z das subpastas de dados-abertos-zip
    para a pasta dados-abertos, mantendo a mesma estrutura de diretórios.
    
    Parâmetros:
    - anos: Lista de anos para processar (ex: [2015, 2016]). Se None, processa todos.
    - sobrescrever: Se True, sobrescreve arquivos existentes no destino.
    - max_arquivos: Número máximo de arquivos a processar (None para todos).
    - pausar: Segundos de pausa entre cada arquivo (para evitar sobrecarga).
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
    
    total_arquivos_processados = 0
    
    # Processar cada pasta de ano
    for ano_dir in diretorios_anos:
        # Criar a pasta correspondente no diretório de destino
        pasta_destino = diretorio_destino / ano_dir.name
        if not pasta_destino.exists():
            pasta_destino.mkdir(parents=True)
        
        print(f"\nProcessando pasta: {ano_dir.name}")
        
        # Listar todos os arquivos .7z na pasta do ano
        arquivos_7z = list(ano_dir.glob('*.7z'))
        
        if not arquivos_7z:
            print(f"  Nenhum arquivo .7z encontrado em {ano_dir.name}")
            continue
            
        print(f"  Encontrados {len(arquivos_7z)} arquivos para descompactar")
        
        # Processar todos os arquivos .7z na pasta do ano com barra de progresso
        for arquivo_7z in tqdm(arquivos_7z, desc=f"  {ano_dir.name}", unit="arquivo"):
            # Verificar se atingiu o limite máximo de arquivos
            if max_arquivos is not None and total_arquivos_processados >= max_arquivos:
                print(f"\nLimite de {max_arquivos} arquivos atingido. Parando o processamento.")
                return
                
            # Verificar se o arquivo já foi descompactado
            if verificar_arquivo_extraido(arquivo_7z, pasta_destino) and not sobrescrever:
                tqdm.write(f"  Pulando {arquivo_7z.name} - já existe no destino (use --sobrescrever para forçar)")
                continue
                
            try:
                with py7zr.SevenZipFile(arquivo_7z, mode='r') as z:
                    z.extractall(path=pasta_destino)
                tqdm.write(f"  ✓ {arquivo_7z.name} descompactado com sucesso")
                total_arquivos_processados += 1
                
                # Pausa entre extrações para evitar sobrecarga
                if pausar > 0:
                    time.sleep(pausar)
            except Exception as e:
                tqdm.write(f"  ✗ Erro ao descompactar {arquivo_7z.name}: {str(e)}")
    
    print(f"\nProcessamento concluído. Total de {total_arquivos_processados} arquivos descompactados.")

def limpar_nome_coluna(nome):
    """
    Limpa o nome da coluna removendo caracteres especiais e espaços extras.
    """
    # Remover caracteres especiais e substituir espaços por underscore
    nome_limpo = re.sub(r'[^\w\s]', '', nome).strip()
    nome_limpo = re.sub(r'\s+', '_', nome_limpo)
    return nome_limpo

def converter_para_parquet(anos=None, chunksize=100000, sobrescrever=False, max_arquivos=None, npartitions=None):
    """
    Converte arquivos TXT da pasta dados-abertos para o formato Parquet na pasta parquet.
    
    Parâmetros:
    - anos: Lista de anos para processar (ex: [2015, 2016]). Se None, processa todos.
    - chunksize: Tamanho dos chunks para processamento (padrão: 100.000 linhas)
    - sobrescrever: Se True, sobrescreve arquivos Parquet existentes.
    - max_arquivos: Número máximo de arquivos a processar (None para todos).
    - npartitions: Número de partições para os arquivos Parquet (None para automático)
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
    
    total_arquivos_processados = 0
    
    # Processar cada pasta de ano
    for ano_dir in diretorios_anos:
        # Criar a pasta correspondente no diretório de destino
        pasta_destino = diretorio_destino / ano_dir.name
        if not pasta_destino.exists():
            pasta_destino.mkdir(parents=True)
        
        print(f"\nProcessando pasta: {ano_dir.name}")
        
        # Listar todos os arquivos TXT na pasta do ano
        arquivos_txt = list(ano_dir.glob('*.txt'))
        
        if not arquivos_txt:
            print(f"  Nenhum arquivo TXT encontrado em {ano_dir.name}")
            continue
            
        print(f"  Encontrados {len(arquivos_txt)} arquivos para converter")
        
        # Processar todos os arquivos TXT na pasta do ano com barra de progresso
        for arquivo_txt in tqdm(arquivos_txt, desc=f"  {ano_dir.name}", unit="arquivo"):
            # Verificar se atingiu o limite máximo de arquivos
            if max_arquivos is not None and total_arquivos_processados >= max_arquivos:
                print(f"\nLimite de {max_arquivos} arquivos atingido. Parando o processamento.")
                return
                
            # Nome do arquivo de saída (mesmo nome, extensão .parquet)
            arquivo_parquet = pasta_destino / f"{arquivo_txt.stem}.parquet"
            
            # Verificar se o arquivo já foi convertido
            if arquivo_parquet.exists() and not sobrescrever:
                tqdm.write(f"  Pulando {arquivo_txt.name} - já existe no destino (use --sobrescrever para forçar)")
                continue
            
            try:
                # Detectar o separador (geralmente é ';' para arquivos da RAIS)
                with open(arquivo_txt, 'r', encoding='latin1', errors='ignore') as f:
                    primeira_linha = f.readline().strip()
                separador = ';' if ';' in primeira_linha else ','
                
                # Usar Dask para processar o arquivo em chunks
                tqdm.write(f"  Convertendo {arquivo_txt.name} para Parquet (separador: '{separador}')")
                
                # Ler as primeiras linhas para obter os nomes das colunas
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
                
                # Definir tipos de dados para todas as colunas como object (string)
                # para evitar problemas de inferência de tipo
                tipos = {col: 'object' for col in colunas_originais}
                
                # Ler o arquivo em chunks com Dask, forçando todos os tipos para string
                tqdm.write(f"  Lendo arquivo em chunks com tipos de dados definidos como string")
                
                # Usar um tamanho de bloco adequado para arquivos grandes
                blocksize = "128MB"  # Ajuste conforme necessário
                
                # Ler o arquivo CSV com Dask
                df = dd.read_csv(
                    arquivo_txt,
                    sep=separador,
                    encoding='latin1',
                    dtype=tipos,
                    blocksize=blocksize,
                    assume_missing=True,
                    on_bad_lines='skip'
                )
                
                # Renomear colunas para nomes limpos
                df = df.rename(columns=mapeamento_colunas)
                
                # Definir número de partições se especificado
                if npartitions:
                    df = df.repartition(npartitions=npartitions)
                
                # Salvar como Parquet
                tqdm.write(f"  Salvando arquivo Parquet (pode demorar para arquivos grandes)")
                df.to_parquet(
                    arquivo_parquet,
                    compression='snappy',
                    write_index=False,
                    engine='pyarrow'
                )
                
                tqdm.write(f"  ✓ {arquivo_txt.name} convertido com sucesso para Parquet")
                total_arquivos_processados += 1
                
            except Exception as e:
                tqdm.write(f"  ✗ Erro ao converter {arquivo_txt.name}: {str(e)}")
                import traceback
                tqdm.write(traceback.format_exc())
    
    print(f"\nProcessamento concluído. Total de {total_arquivos_processados} arquivos convertidos para Parquet.")

def main():
    parser = argparse.ArgumentParser(description='Processamento de arquivos da RAIS')
    parser.add_argument('--anos', type=int, nargs='+', help='Anos específicos para processar (ex: 2015 2016)')
    parser.add_argument('--sobrescrever', action='store_true', help='Sobrescreve arquivos existentes')
    parser.add_argument('--max', type=int, help='Número máximo de arquivos a processar')
    parser.add_argument('--pausar', type=int, default=0, help='Segundos de pausa entre cada arquivo (padrão: 0)')
    parser.add_argument('--listar', action='store_true', help='Apenas lista os anos disponíveis sem processar')
    parser.add_argument('--modo', choices=['descompactar', 'converter', 'ambos'], default='descompactar',
                       help='Modo de operação: descompactar, converter ou ambos (padrão: descompactar)')
    parser.add_argument('--chunksize', type=int, default=100000, 
                       help='Tamanho dos chunks para processamento de arquivos grandes (padrão: 100.000)')
    parser.add_argument('--npartitions', type=int, 
                       help='Número de partições para os arquivos Parquet (padrão: automático)')
    args = parser.parse_args()
    
    # Se a opção --listar foi especificada, apenas mostra os anos disponíveis
    if args.listar:
        diretorio_origem = Path('dados-abertos-zip')
        anos_disponiveis = [d.name for d in diretorio_origem.iterdir() if d.is_dir()]
        print(f"Anos disponíveis: {', '.join(sorted(anos_disponiveis))}")
        return
    
    # Executar o modo selecionado
    if args.modo in ['descompactar', 'ambos']:
        descompactar_arquivos(anos=args.anos, sobrescrever=args.sobrescrever, 
                             max_arquivos=args.max, pausar=args.pausar)
    
    if args.modo in ['converter', 'ambos']:
        converter_para_parquet(anos=args.anos, chunksize=args.chunksize,
                              sobrescrever=args.sobrescrever, max_arquivos=args.max,
                              npartitions=args.npartitions)

if __name__ == "__main__":
    main()
