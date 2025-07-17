import logging
import argparse
import os
import sys
from datetime import datetime
from pathlib import Path
from tqdm import tqdm

# Importar as classes do módulo util
from src.util.descompactador import DescompactadorArquivos
from src.util.conversor_parquet import ConversorParquet


def configurar_logging(nivel_log: str = "INFO") -> logging.Logger:
    """
    Configura o sistema de logging baseado no sistema operacional.
    
    Args:
        nivel_log: Nível de log (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        
    Returns:
        Logger configurado
    """
    # Determinar o diretório de logs baseado no sistema operacional
    if sys.platform.startswith('win'):
        # Windows: pasta logs no diretório atual
        diretorio_logs = Path("logs")
    else:
        # Linux: pasta padrão de logs do sistema
        diretorio_logs = Path("/var/log")
    
    # Criar diretório de logs se não existir
    diretorio_logs.mkdir(parents=True, exist_ok=True)
    
    # Gerar nome do arquivo de log com timestamp
    timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    nome_arquivo = f"rais_{timestamp}.log"
    caminho_arquivo_log = diretorio_logs / nome_arquivo
    
    # Configurar o nível de log
    nivel = getattr(logging, nivel_log.upper(), logging.INFO)
    
    # Configuração de logging
    logging.basicConfig(
        level=nivel,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(caminho_arquivo_log, encoding='utf-8'),
            logging.StreamHandler()
        ],
        force=True  # Força a reconfiguração do logging
    )
    
    logger = logging.getLogger("gerenciador_arquivos")
    logger.info(f"Logging configurado - Arquivo: {caminho_arquivo_log}, Nível: {nivel_log}")
    
    return logger


def criar_parser_argumentos() -> argparse.ArgumentParser:
    """
    Cria e configura o parser de argumentos da linha de comando.
    
    Returns:
        ArgumentParser configurado
    """
    parser = argparse.ArgumentParser(
        description="Gerenciador de arquivos para dados RAIS - Download, descompactação e conversão para Parquet",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  # Trabalhar localmente (sem acessar servidor FTP):
  python main.py --apenas-descompactar
  python main.py --apenas-converter-parquet --ano 2024
  
  # Acessar servidor FTP:
  python main.py --sincronizar --ano 2024
  python main.py --listar-diretorios
  python main.py --listar-arquivos
        """
    )
    
    # Argumentos obrigatórios
    parser.add_argument(
        "--servidor",
        type=str,
        default="ftp.mtps.gov.br",
        help="Endereço do servidor FTP (padrão: ftp.mtps.gov.br)"
    )
    
    parser.add_argument(
        "--diretorio-remoto",
        type=str,
        default="/pdet/microdados/RAIS",
        help="Diretório remoto no servidor FTP (padrão: /pdet/microdados/RAIS)"
    )
    
    # Argumentos opcionais
    parser.add_argument(
        "--max-tentativas",
        type=int,
        default=5,
        help="Número máximo de tentativas de download (padrão: 5)"
    )
    
    parser.add_argument(
        "--tempo-espera",
        type=int,
        default=10,
        help="Tempo de espera entre tentativas em segundos (padrão: 10)"
    )
    
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="Número máximo de workers para download paralelo (padrão: 4)"
    )
    
    parser.add_argument(
        "--nivel-log",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Nível de logging (padrão: INFO)"
    )
    
    # Flags de ação
    parser.add_argument(
        "--listar-diretorios",
        action="store_true",
        help="Apenas listar diretórios remotos sem baixar arquivos"
    )
    
    parser.add_argument(
        "--listar-arquivos",
        action="store_true",
        help="Apenas listar arquivos remotos sem baixar"
    )
    
    parser.add_argument(
        "--sincronizar",
        action="store_true",
        help="Sincronizar arquivos do servidor FTP"
    )
    
    parser.add_argument(
        "--ano",
        type=int,
        help="Ano específico para baixar (ex: 2020)"
    )
    parser.add_argument(
        "--faixa-anos",
        type=int,
        nargs='+',
        metavar='ANO',
        help="Faixa de anos para baixar. Use: --faixa-anos 2020 (do ano 2020 até o último) ou --faixa-anos 2020 2024 (do ano 2020 até 2024)"
    )
    
    parser.add_argument(
        "--descompactar",
        action="store_true",
        help="Descompactar arquivos .7z baixados para files-unzip/"
    )
    
    parser.add_argument(
        "--apenas-descompactar",
        action="store_true",
        help="Apenas descompactar arquivos .7z existentes, sem baixar novos arquivos"
    )
    
    parser.add_argument(
        "--converter-parquet",
        action="store_true",
        help="Converter arquivos TXT descompactados para formato Parquet"
    )
    
    parser.add_argument(
        "--apenas-converter-parquet",
        action="store_true",
        help="Apenas converter arquivos TXT para Parquet, sem baixar ou descompactar"
    )
    
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=100000,
        help="Tamanho do chunk para conversão Parquet (padrão: 100000 linhas)"
    )
    
    return parser


def main():
    """
    Função principal que processa argumentos e executa o gerenciador de arquivos.
    """
    # Criar e configurar parser de argumentos
    parser = criar_parser_argumentos()
    args = parser.parse_args()
    
    # Configurar logging
    logger = configurar_logging(args.nivel_log)
    
    # Registrar o comando executado
    comando_executado = " ".join(sys.argv)
    logger.info(f"Comando executado: {comando_executado}")
    
    try:
        # Verificar se alguma ação foi especificada
        acoes_especificadas = any([
            args.listar_diretorios,
            args.listar_arquivos,
            args.sincronizar,
            args.apenas_descompactar,
            args.apenas_converter_parquet
        ])
        
        if not acoes_especificadas:
            print("Nenhuma ação especificada. Use --help para ver as opções disponíveis.")
            parser.print_help()
            return
        
        # Criar instância do gerenciador apenas se necessário
        gerenciador = None
        if any([args.listar_diretorios, args.listar_arquivos, args.sincronizar]):
            # Importar apenas quando necessário para evitar conexão desnecessária
            from src.util.gerenciador_ftp import GerenciadorArquivosFTP
            
            gerenciador = GerenciadorArquivosFTP(
                servidor_ftp=args.servidor,
                diretorio_remoto=args.diretorio_remoto,
                max_tentativas=args.max_tentativas,
                tempo_espera=args.tempo_espera,
                max_workers=args.max_workers
            )
            
            logger.info(f"Gerenciador configurado:")
            logger.info(f"  Servidor: {args.servidor}")
            logger.info(f"  Diretório remoto: {args.diretorio_remoto}")
            logger.info(f"  Diretório local: files-zip")
            logger.info(f"  Máx tentativas: {args.max_tentativas}")
            logger.info(f"  Tempo espera: {args.tempo_espera}s")
            logger.info(f"  Máx workers: {args.max_workers}")
        
        # Executar ações baseadas nos argumentos
        if args.listar_diretorios:
            logger.info("Listando diretórios remotos...")
            diretorios = gerenciador.listar_diretorios_remotos()
            print(f"\nDiretórios encontrados ({len(diretorios)}):")
            for i, diretorio in enumerate(diretorios, 1):
                print(f"  {i:2d}. {diretorio}")
        
        elif args.listar_arquivos:
            logger.info("Listando arquivos remotos...")
            arquivos = gerenciador.listar_arquivos_remotos()
            print(f"\nArquivos encontrados ({len(arquivos)}):")
            
            # Barra de progresso para listagem
            for i, arquivo in enumerate(tqdm(arquivos, desc="Listando arquivos", unit="arquivo"), 1):
                tamanho_mb = arquivo.get("tamanho", 0) / (1024 * 1024)
                print(f"  {i:3d}. {arquivo['nome']} ({tamanho_mb:.2f} MB)")
        
        elif args.sincronizar:
            logger.info("Iniciando sincronização de arquivos...")
            
            # Processar argumentos de faixa de anos
            ano_inicio = None
            ano_fim = None
            if args.faixa_anos:
                if len(args.faixa_anos) == 1:
                    ano_inicio = args.faixa_anos[0]
                    ano_fim = None  # Até o último disponível
                    logger.info(f"Faixa de anos: do ano {ano_inicio} até o último disponível")
                elif len(args.faixa_anos) == 2:
                    ano_inicio = args.faixa_anos[0]
                    ano_fim = args.faixa_anos[1]
                    logger.info(f"Faixa de anos: do ano {ano_inicio} até {ano_fim}")
                else:
                    logger.error("--faixa-anos deve receber 1 ou 2 valores")
                    sys.exit(1)
            
            logger.info(f"Filtro de ano: ano={args.ano}, ano_inicio={ano_inicio}, ano_fim={ano_fim}")
            total, baixados, falhas = gerenciador.sincronizar_arquivos(
                ano=args.ano,
                ano_inicio=ano_inicio,
                ano_fim=ano_fim
            )
            
            print(f"\nResultado da sincronização:")
            print(f"  Total de arquivos: {total}")
            print(f"  Arquivos baixados: {baixados}")
            print(f"  Falhas: {falhas}")
            
            if falhas > 0:
                logger.warning(f"Sincronização concluída com {falhas} falhas")
            else:
                logger.info("Sincronização concluída com sucesso")
            
            # Descompactar arquivos se solicitado
            if args.descompactar:
                logger.info("Iniciando descompactação de arquivos...")
                descompactador = DescompactadorArquivos(max_workers=args.max_workers)
                total_descompactar, descompactados, falhas_descompactar = descompactador.descompactar_arquivos_paralelo()
                
                print(f"\nResultado da descompactação:")
                print(f"  Total de arquivos: {total_descompactar}")
                print(f"  Arquivos descompactados: {descompactados}")
                print(f"  Falhas: {falhas_descompactar}")
                
                if falhas_descompactar > 0:
                    logger.warning(f"Descompactação concluída com {falhas_descompactar} falhas")
                else:
                    logger.info("Descompactação concluída com sucesso")
            
            # Converter para Parquet se solicitado
            if args.converter_parquet:
                logger.info("Iniciando conversão para Parquet...")
                conversor = ConversorParquet(
                    chunk_size=args.chunk_size,
                    max_workers=args.max_workers
                )
                
                # Determinar ano para conversão
                ano_conversao = args.ano
                if args.faixa_anos and len(args.faixa_anos) >= 1:
                    ano_conversao = args.faixa_anos[0]
                
                resultado_conversao = conversor.converter_diretorio("files-unzip", ano=ano_conversao)
                
                print(f"\nResultado da conversão para Parquet:")
                print(f"  Total de arquivos: {resultado_conversao['total']}")
                print(f"  Arquivos convertidos: {resultado_conversao['convertidos']}")
                print(f"  Falhas: {resultado_conversao['falhas']}")
                
                if resultado_conversao['falhas'] > 0:
                    logger.warning(f"Conversão concluída com {resultado_conversao['falhas']} falhas")
                else:
                    logger.info("Conversão para Parquet concluída com sucesso")
        
        elif args.apenas_descompactar:
            logger.info("Iniciando apenas descompactação de arquivos...")
            descompactador = DescompactadorArquivos(max_workers=args.max_workers)
            total_descompactar, descompactados, falhas_descompactar = descompactador.descompactar_arquivos_paralelo()
            
            print(f"\nResultado da descompactação:")
            print(f"  Total de arquivos: {total_descompactar}")
            print(f"  Arquivos descompactados: {descompactados}")
            print(f"  Falhas: {falhas_descompactar}")
            
            if falhas_descompactar > 0:
                logger.warning(f"Descompactação concluída com {falhas_descompactar} falhas")
            else:
                logger.info("Descompactação concluída com sucesso")
        
        elif args.apenas_converter_parquet:
            logger.info("Iniciando apenas conversão para Parquet...")
            conversor = ConversorParquet(
                chunk_size=args.chunk_size,
                max_workers=args.max_workers
            )
            
            # Determinar ano para conversão
            ano_conversao = args.ano
            if args.faixa_anos and len(args.faixa_anos) >= 1:
                ano_conversao = args.faixa_anos[0]
            
            resultado_conversao = conversor.converter_diretorio("files-unzip", ano=ano_conversao)
            
            print(f"\nResultado da conversão para Parquet:")
            print(f"  Total de arquivos: {resultado_conversao['total']}")
            print(f"  Arquivos convertidos: {resultado_conversao['convertidos']}")
            print(f"  Falhas: {resultado_conversao['falhas']}")
            
            if resultado_conversao['falhas'] > 0:
                logger.warning(f"Conversão concluída com {resultado_conversao['falhas']} falhas")
            else:
                logger.info("Conversão para Parquet concluída com sucesso")
        
    except KeyboardInterrupt:
        logger.info("Operação interrompida pelo usuário")
        print("\nOperação interrompida pelo usuário")
    except Exception as e:
        logger.error(f"Erro durante a execução: {e}")
        print(f"\nErro: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
