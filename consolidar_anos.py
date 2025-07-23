#!/usr/bin/env python3
"""
Script para consolidar arquivos RAIS dos anos 2021 e 2022.
Este script consolida os chunks em arquivos únicos por ano.
"""

import logging
import sys
from pathlib import Path
from datetime import datetime

# Importar as classes do módulo util
from src.util import PipelineParalelo, MedidorTempo


def configurar_logging() -> logging.Logger:
    """Configura o sistema de logging."""
    # Criar diretório de logs se não existir
    diretorio_logs = Path("logs")
    diretorio_logs.mkdir(parents=True, exist_ok=True)
    
    # Gerar nome do arquivo de log com timestamp
    timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    nome_arquivo = f"consolidacao_{timestamp}.log"
    caminho_arquivo_log = diretorio_logs / nome_arquivo
    
    # Configuração de logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(caminho_arquivo_log, encoding='utf-8'),
            logging.StreamHandler()
        ],
        force=True
    )
    
    logger = logging.getLogger("consolidacao")
    logger.info(f"Logging configurado - Arquivo: {caminho_arquivo_log}")
    
    return logger


def main():
    """Função principal para consolidar os anos 2021 e 2022."""
    logger = configurar_logging()
    medidor = MedidorTempo("Consolidação RAIS 2021-2022")
    
    try:
        # Anos a serem consolidados
        anos = [2021, 2022]
        
        # Criar pipeline paralelo
        pipeline = PipelineParalelo(
            max_workers=4,
            chunk_size=None,
            campos_especificos=['CNAE_2_0_CLASSE', 'VINCULO_ATIVO_31_12', 'MUNICIPIO'],
            limpar_arquivos_descompactados=False,
            filtrar_empregos_verdes=True,
            arquivo_filtro_cnae="db/cnae_classe_emprego_verde.csv",
            nome_filtro_cnae="Emprego Verde",
            situacao_filtro_cnae=1
        )
        
        logger.info("Iniciando consolidação dos anos 2021 e 2022...")
        
        for ano in anos:
            with medidor.etapa(f"Consolidação do ano {ano}"):
                logger.info(f"Consolidando arquivos do ano {ano}...")
                
                # Executar consolidação
                resultado = pipeline.consolidar_arquivos(ano)
                
                if resultado['status'] == 'sucesso':
                    print(f"\n✅ Consolidação do ano {ano} concluída com sucesso")
                    print(f"   Arquivo consolidado: {resultado['arquivo_consolidado']}")
                    logger.info(f"Consolidação do ano {ano} concluída com sucesso")
                else:
                    print(f"\n❌ Erro na consolidação do ano {ano}: {resultado['erro']}")
                    logger.error(f"Erro na consolidação do ano {ano}: {resultado['erro']}")
        
        # Verificar se os arquivos consolidados foram criados
        print("\n" + "="*60)
        print("VERIFICAÇÃO DOS ARQUIVOS CONSOLIDADOS")
        print("="*60)
        
        for ano in anos:
            arquivo_consolidado = Path("parquet") / str(ano) / f"RAIS_{ano}.parquet"
            if arquivo_consolidado.exists():
                tamanho_mb = arquivo_consolidado.stat().st_size / (1024 * 1024)
                print(f"✅ Ano {ano}: {arquivo_consolidado} ({tamanho_mb:.2f} MB)")
            else:
                print(f"❌ Ano {ano}: Arquivo consolidado não encontrado")
        
        print("\n" + "="*60)
        print("CONSOLIDAÇÃO CONCLUÍDA")
        print("="*60)
        
    except KeyboardInterrupt:
        logger.info("Operação interrompida pelo usuário")
        print("\nOperação interrompida pelo usuário")
    except Exception as e:
        logger.error(f"Erro durante a execução: {e}")
        print(f"\nErro: {e}")
        sys.exit(1)
    finally:
        # Finalizar medição de tempo e imprimir resumo
        medidor.finalizar_processo()
        medidor.imprimir_resumo()


if __name__ == "__main__":
    main() 