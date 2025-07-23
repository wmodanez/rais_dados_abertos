#!/usr/bin/env python3
"""
Script para diagnosticar problemas com arquivos RAIS de 2023.
"""

import logging
import sys
from pathlib import Path
import polars as pl
from datetime import datetime

# Importar funções de padronização de colunas
from src.util.utilitarios import padronizar_colunas_dataframe

def configurar_logging() -> logging.Logger:
    """Configura o sistema de logging."""
    # Criar diretório de logs se não existir
    diretorio_logs = Path("logs")
    diretorio_logs.mkdir(parents=True, exist_ok=True)
    
    # Gerar nome do arquivo de log com timestamp
    timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    nome_arquivo = f"diagnostico_arquivo_{timestamp}.log"
    caminho_arquivo_log = diretorio_logs / nome_arquivo
    
    # Configurar logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(caminho_arquivo_log, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logger = logging.getLogger("diagnostico_arquivo")
    logger.info(f"Logging configurado - Arquivo: {caminho_arquivo_log}, Nível: INFO")
    
    return logger

def detectar_encoding(caminho_arquivo: Path) -> str:
    """Detecta o encoding do arquivo."""
    try:
        import chardet
        
        # Ler uma amostra do arquivo para detectar encoding
        with open(caminho_arquivo, 'rb') as f:
            amostra = f.read(10000)  # Primeiros 10KB
        
        resultado = chardet.detect(amostra)
        encoding = resultado['encoding']
        confianca = resultado['confidence']
        
        print(f"Encoding detectado: {encoding} (confiança: {confianca:.2f})")
        return encoding if encoding else 'utf-8'
        
    except ImportError:
        print("chardet não disponível, usando utf-8")
        return 'utf-8'
    except Exception as e:
        print(f"Erro ao detectar encoding: {e}")
        return 'utf-8'

def detectar_separador(caminho_arquivo: Path) -> str:
    """Detecta o separador do arquivo."""
    try:
        # Ler as primeiras linhas para detectar separador
        with open(caminho_arquivo, 'r', encoding='utf-8', errors='ignore') as f:
            primeiras_linhas = [f.readline() for _ in range(5)]
        
        # Contar ocorrências de separadores comuns
        separadores = [';', ',', '\t', '|']
        contadores = {}
        
        for linha in primeiras_linhas:
            if linha.strip():
                for sep in separadores:
                    cont = linha.count(sep)
                    contadores[sep] = contadores.get(sep, 0) + cont
        
        # Encontrar o separador mais frequente
        if contadores:
            separador = max(contadores, key=contadores.get)
            print(f"Separador detectado: '{separador}' (total: {contadores[separador]})")
            return separador
        
        print("Não foi possível detectar separador, usando ';'")
        return ';'
        
    except Exception as e:
        print(f"Erro ao detectar separador: {e}")
        return ';'

def testar_leitura_arquivo(caminho_arquivo: Path):
    """Testa a leitura do arquivo com diferentes configurações."""
    print(f"\n=== DIAGNÓSTICO DO ARQUIVO: {caminho_arquivo} ===")
    
    if not caminho_arquivo.exists():
        print(f"❌ Arquivo não encontrado: {caminho_arquivo}")
        return
    
    # Informações básicas do arquivo
    tamanho_bytes = caminho_arquivo.stat().st_size
    tamanho_mb = tamanho_bytes / (1024 * 1024)
    print(f"📁 Tamanho do arquivo: {tamanho_mb:.1f} MB")
    
    # Detectar encoding e separador
    encoding = detectar_encoding(caminho_arquivo)
    separador = detectar_separador(caminho_arquivo)
    
    # Teste 1: Ler apenas o cabeçalho
    print(f"\n--- Teste 1: Leitura do cabeçalho ---")
    try:
        df_cabecalho = pl.read_csv(
            caminho_arquivo,
            separator=separador,
            n_rows=1,
            encoding=encoding,
            ignore_errors=True,
            truncate_ragged_lines=True
        )
        print(f"✅ Cabeçalho lido com sucesso")
        print(f"   Número de colunas: {len(df_cabecalho.columns)}")
        print(f"   Primeiras 5 colunas: {df_cabecalho.columns[:5]}")
        
        # Testar padronização de colunas
        mapeamento = padronizar_colunas_dataframe(df_cabecalho.columns)
        print(f"   Colunas padronizadas: {len(mapeamento)}")
        
    except Exception as e:
        print(f"❌ Erro ao ler cabeçalho: {e}")
        return
    
    # Teste 2: Ler primeiras 10 linhas
    print(f"\n--- Teste 2: Leitura das primeiras 10 linhas ---")
    try:
        df_amostra = pl.read_csv(
            caminho_arquivo,
            separator=separador,
            n_rows=10,
            encoding=encoding,
            ignore_errors=True,
            truncate_ragged_lines=True
        )
        print(f"✅ Amostra lida com sucesso")
        print(f"   Linhas lidas: {len(df_amostra)}")
        print(f"   Colunas: {len(df_amostra.columns)}")
        
        # Verificar se há linhas com problemas
        linhas_com_problemas = df_amostra.filter(pl.any_horizontal(pl.all().is_null()))
        if len(linhas_com_problemas) > 0:
            print(f"   ⚠️  Linhas com valores nulos: {len(linhas_com_problemas)}")
        
    except Exception as e:
        print(f"❌ Erro ao ler amostra: {e}")
        return
    
    # Teste 3: Contar linhas
    print(f"\n--- Teste 3: Contagem de linhas ---")
    try:
        # Usar wc -l se disponível (mais rápido)
        import subprocess
        resultado = subprocess.run(['wc', '-l', str(caminho_arquivo)], 
                                 capture_output=True, text=True)
        if resultado.returncode == 0:
            linhas = int(resultado.stdout.strip().split()[0])
            print(f"✅ Total de linhas: {linhas:,}")
        else:
            # Fallback: contar manualmente
            with open(caminho_arquivo, 'r', encoding=encoding, errors='ignore') as f:
                linhas = sum(1 for _ in f)
            print(f"✅ Total de linhas: {linhas:,}")
            
    except Exception as e:
        print(f"❌ Erro ao contar linhas: {e}")
    
    # Teste 4: Verificar se há caracteres especiais problemáticos
    print(f"\n--- Teste 4: Verificação de caracteres especiais ---")
    try:
        with open(caminho_arquivo, 'r', encoding=encoding, errors='ignore') as f:
            primeiras_linhas = [f.readline() for _ in range(100)]
        
        caracteres_especiais = []
        for i, linha in enumerate(primeiras_linhas):
            for j, char in enumerate(linha):
                if ord(char) > 127:  # Caracteres não-ASCII
                    caracteres_especiais.append((i, j, char, ord(char)))
                    if len(caracteres_especiais) >= 10:  # Limitar a 10 exemplos
                        break
            if len(caracteres_especiais) >= 10:
                break
        
        if caracteres_especiais:
            print(f"⚠️  Caracteres especiais encontrados:")
            for linha, col, char, codigo in caracteres_especiais[:5]:
                print(f"   Linha {linha}, Col {col}: '{char}' (código {codigo})")
        else:
            print(f"✅ Nenhum caractere especial problemático encontrado")
            
    except Exception as e:
        print(f"❌ Erro ao verificar caracteres especiais: {e}")

def main():
    """Função principal."""
    logger = configurar_logging()
    
    # Arquivos problemáticos de 2023
    arquivos_teste = [
        Path("files-unzip/2023/RAIS_VINC_PUB_SP.txt"),
        Path("files-unzip/2023/RAIS_VINC_PUB_MG_ES_RJ.txt")
    ]
    
    print("🔍 DIAGNÓSTICO DE ARQUIVOS RAIS 2023")
    print("=" * 50)
    
    for arquivo in arquivos_teste:
        testar_leitura_arquivo(arquivo)
        print("\n" + "=" * 50)
    
    print("\n✅ Diagnóstico concluído!")

if __name__ == "__main__":
    main() 