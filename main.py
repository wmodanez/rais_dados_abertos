import logging
import argparse
import os
import sys
from datetime import datetime
from pathlib import Path
from tqdm import tqdm

# Importar as classes do módulo util
from src.util import GerenciadorArquivosFTP, DescompactadorArquivos, ConversorParquet, MedidorTempo


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
  # Apenas baixar:
  python main.py --baixar --ano 2024

  # Baixar e descompactar:
  python main.py --descompactar --ano 2024

  # Apenas descompactar (arquivos já baixados):
  python main.py --apenas-descompactar --ano 2024

  # Baixar, descompactar e converter:
  python main.py --converter --ano 2024

  # Apenas converter (arquivos já descompactados):
  python main.py --apenas-converter --ano 2024

  # Descompactar e converter (arquivos já baixados):
  python main.py --descompactar-converter --ano 2024

  # Converter com consolidação (arquivo único por ano):
  python main.py --converter --ano 2024 --consolidacao

  # Apenas converter com consolidação:
  python main.py --apenas-converter --ano 2024 --consolidacao

  # Descompactar e converter com consolidação:
  python main.py --descompactar-converter --ano 2024 --consolidacao

  # Apenas consolidar arquivos já convertidos:
  python main.py --apenas-consolidar --ano 2024
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
        default=None,
        help="Número máximo de workers para processamento paralelo (padrão: detectado automaticamente)"
    )
    
    parser.add_argument(
        "--nivel-log",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Nível de logging (padrão: INFO)"
    )
    
    # Flags de ação exclusivas
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--baixar', action='store_true', help='Apenas baixar arquivos do servidor FTP')
    group.add_argument('--descompactar', action='store_true', help='Baixar e descompactar arquivos')
    group.add_argument('--apenas-descompactar', action='store_true', help='Apenas descompactar arquivos já baixados (não faz download)')
    group.add_argument('--converter', action='store_true', help='Baixar, descompactar e converter para formato final')
    group.add_argument('--apenas-converter', action='store_true', help='Apenas converter arquivos já descompactados (não faz download)')
    group.add_argument('--descompactar-converter', action='store_true', help='Descompactar e converter arquivos já baixados (não faz download)')
    group.add_argument('--apenas-consolidar', action='store_true', help='Apenas consolidar arquivos já convertidos em um único arquivo RAIS_ANO.parquet')
    
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
        "--descompactar-apos-baixar",
        action="store_true",
        help="Descompactar arquivos .7z após baixá-los"
    )
    
    parser.add_argument(
        "--converter-apos-descompactar",
        action="store_true",
        help="Converter arquivos TXT para Parquet após descompactá-los"
    )
    
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=None,
        help="Tamanho do chunk para conversão Parquet (padrão: baseado no tamanho do arquivo)"
    )
    
    parser.add_argument(
        "--consolidacao",
        action="store_true",
        help="Consolidar todos os arquivos de um ano em um único arquivo RAIS_ANO.parquet após a conversão"
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
    
    # Inicializar medidor de tempo
    medidor = MedidorTempo("Processamento RAIS")
    medidor.iniciar_processo()
    
    try:
        # Verificar se alguma ação foi especificada
        acoes_especificadas = any([
            args.baixar,
            args.descompactar,
            args.apenas_descompactar,
            args.converter,
            args.apenas_converter,
            args.descompactar_converter,
            args.apenas_consolidar
        ])
        
        if not acoes_especificadas:
            print("Nenhuma ação válida especificada. Use --help para ver as opções disponíveis.")
            parser.print_help()
            return
        
        # Criar instância do gerenciador se necessário para operações FTP
        gerenciador = None
        if any([args.baixar, args.descompactar, args.converter]):
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
        if args.baixar:
            if gerenciador is None:
                logger.error("Gerenciador FTP não foi inicializado")
                return
            with medidor.etapa("Download"):
                logger.info("Iniciando download de arquivos...")
                
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
            if args.descompactar_apos_baixar:
                logger.info("Iniciando descompactação de arquivos...")
                descompactador = DescompactadorArquivos(max_workers=args.max_workers)
                total_descompactar, descompactados, falhas_descompactar = descompactador.descompactar_arquivos_paralelo(
                    ano=args.ano,
                    ano_inicio=ano_inicio,
                    ano_fim=ano_fim
                )
                
                print(f"\nResultado da descompactação:")
                print(f"  Total de arquivos: {total_descompactar}")
                print(f"  Arquivos descompactados: {descompactados}")
                print(f"  Falhas: {falhas_descompactar}")
                
                if falhas_descompactar > 0:
                    logger.warning(f"Descompactação concluída com {falhas_descompactar} falhas")
                else:
                    logger.info("Descompactação concluída com sucesso")
        
        elif args.descompactar:
            if gerenciador is None:
                logger.error("Gerenciador FTP não foi inicializado")
                return
            with medidor.etapa("Download"):
                logger.info("Iniciando download de arquivos...")
                
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
            
            # Descompactar arquivos após download
            with medidor.etapa("Descompactação"):
                logger.info("Iniciando descompactação de arquivos...")
                logger.info(f"Filtro de ano para descompactação: ano={args.ano}, ano_inicio={ano_inicio}, ano_fim={ano_fim}")
                
                descompactador = DescompactadorArquivos(max_workers=args.max_workers)
                total_descompactar, descompactados, falhas_descompactar = descompactador.descompactar_arquivos_paralelo(
                    ano=args.ano,
                    ano_inicio=ano_inicio,
                    ano_fim=ano_fim
                )
            
            print(f"\nResultado da descompactação:")
            print(f"  Total de arquivos: {total_descompactar}")
            print(f"  Arquivos descompactados: {descompactados}")
            print(f"  Falhas: {falhas_descompactar}")
            
            if falhas_descompactar > 0:
                logger.warning(f"Descompactação concluída com {falhas_descompactar} falhas")
            else:
                logger.info("Descompactação concluída com sucesso")
        
        elif args.apenas_descompactar:
            with medidor.etapa("Descompactação"):
                logger.info("Iniciando apenas descompactação de arquivos...")
                
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
                
                logger.info(f"Filtro de ano para descompactação: ano={args.ano}, ano_inicio={ano_inicio}, ano_fim={ano_fim}")
                
                descompactador = DescompactadorArquivos(max_workers=args.max_workers)
                total_descompactar, descompactados, falhas_descompactar = descompactador.descompactar_arquivos_paralelo(
                    ano=args.ano,
                    ano_inicio=ano_inicio,
                    ano_fim=ano_fim
                )
            
            print(f"\nResultado da descompactação:")
            print(f"  Total de arquivos: {total_descompactar}")
            print(f"  Arquivos descompactados: {descompactados}")
            print(f"  Falhas: {falhas_descompactar}")
            
            if falhas_descompactar > 0:
                logger.warning(f"Descompactação concluída com {falhas_descompactar} falhas")
            else:
                logger.info("Descompactação concluída com sucesso")
        
        elif args.converter:
            if gerenciador is None:
                logger.error("Gerenciador FTP não foi inicializado")
                return
            with medidor.etapa("Download"):
                logger.info("Iniciando download de arquivos...")
                
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
            
            # Descompactar arquivos após download
            with medidor.etapa("Descompactação"):
                logger.info("Iniciando descompactação de arquivos...")
                logger.info(f"Filtro de ano para descompactação: ano={args.ano}, ano_inicio={ano_inicio}, ano_fim={ano_fim}")
                
                descompactador = DescompactadorArquivos(max_workers=args.max_workers)
                total_descompactar, descompactados, falhas_descompactar = descompactador.descompactar_arquivos_paralelo(
                    ano=args.ano,
                    ano_inicio=ano_inicio,
                    ano_fim=ano_fim
                )
            
            print(f"\nResultado da descompactação:")
            print(f"  Total de arquivos: {total_descompactar}")
            print(f"  Arquivos descompactados: {descompactados}")
            print(f"  Falhas: {falhas_descompactar}")
            
            if falhas_descompactar > 0:
                logger.warning(f"Descompactação concluída com {falhas_descompactar} falhas")
            else:
                logger.info("Descompactação concluída com sucesso")
            
            # Converter arquivos após descompactação
            with medidor.etapa("Conversão"):
                logger.info("Iniciando conversão de arquivos para formato final...")
                conversor = ConversorParquet(
                    chunk_size=args.chunk_size,
                    max_workers=args.max_workers
                )
                resultado_conversao = conversor.converter_diretorio("files-unzip", ano=args.ano, consolidar=args.consolidacao)
                
                print(f"\nResultado da conversão:")
                print(f"  Total de arquivos: {resultado_conversao['total']}")
                print(f"  Arquivos convertidos: {resultado_conversao['convertidos']}")
                print(f"  Falhas: {resultado_conversao['falhas']}")
                
                if resultado_conversao['falhas'] > 0:
                    logger.warning(f"Conversão concluída com {resultado_conversao['falhas']} falhas")
                else:
                    logger.info("Conversão concluída com sucesso")
        
        elif args.apenas_converter:
            with medidor.etapa("Conversão para Parquet"):
                logger.info("Iniciando apenas conversão para Parquet...")
                conversor = ConversorParquet(
                    chunk_size=args.chunk_size,
                    max_workers=args.max_workers
                )
                
                # Determinar ano para conversão
                ano_conversao = args.ano
                if args.faixa_anos and len(args.faixa_anos) >= 1:
                    ano_conversao = args.faixa_anos[0]
                
                resultado_conversao = conversor.converter_diretorio("files-unzip", ano=ano_conversao, consolidar=args.consolidacao)
            
            print(f"\nResultado da conversão para Parquet:")
            print(f"  Total de arquivos: {resultado_conversao['total']}")
            print(f"  Arquivos convertidos: {resultado_conversao['convertidos']}")
            print(f"  Falhas: {resultado_conversao['falhas']}")
            
            if resultado_conversao['falhas'] > 0:
                logger.warning(f"Conversão concluída com {resultado_conversao['falhas']} falhas")
            else:
                logger.info("Conversão para Parquet concluída com sucesso")
        
        elif args.descompactar_converter:
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
            
            # Descompactar arquivos
            with medidor.etapa("Descompactação"):
                logger.info("Iniciando descompactação de arquivos...")
                logger.info(f"Filtro de ano para descompactação: ano={args.ano}, ano_inicio={ano_inicio}, ano_fim={ano_fim}")
                
                descompactador = DescompactadorArquivos(max_workers=args.max_workers)
                total_descompactar, descompactados, falhas_descompactar = descompactador.descompactar_arquivos_paralelo(
                    ano=args.ano,
                    ano_inicio=ano_inicio,
                    ano_fim=ano_fim
                )
            
            print(f"\nResultado da descompactação:")
            print(f"  Total de arquivos: {total_descompactar}")
            print(f"  Arquivos descompactados: {descompactados}")
            print(f"  Falhas: {falhas_descompactar}")
            
            if falhas_descompactar > 0:
                logger.warning(f"Descompactação concluída com {falhas_descompactar} falhas")
            else:
                logger.info("Descompactação concluída com sucesso")
            
            # Converter arquivos
            with medidor.etapa("Conversão"):
                logger.info("Iniciando conversão de arquivos para formato final...")
                conversor = ConversorParquet(
                    chunk_size=args.chunk_size,
                    max_workers=args.max_workers
                )
                resultado_conversao = conversor.converter_diretorio("files-unzip", ano=args.ano, consolidar=args.consolidacao)
                
                print(f"\nResultado da conversão:")
                print(f"  Total de arquivos: {resultado_conversao['total']}")
                print(f"  Arquivos convertidos: {resultado_conversao['convertidos']}")
                print(f"  Falhas: {resultado_conversao['falhas']}")
                
                if resultado_conversao['falhas'] > 0:
                    logger.warning(f"Conversão concluída com {resultado_conversao['falhas']} falhas")
                else:
                    logger.info("Conversão concluída com sucesso")
        
        elif args.apenas_consolidar:
            # Verificar se o ano foi especificado
            if not args.ano:
                logger.error("--apenas-consolidar requer que o ano seja especificado com --ano")
                sys.exit(1)
            
            with medidor.etapa("Consolidação"):
                logger.info(f"Iniciando apenas consolidação de arquivos do ano {args.ano}...")
                conversor = ConversorParquet(
                    chunk_size=args.chunk_size,
                    max_workers=args.max_workers
                )
                
                # Executar apenas a consolidação
                conversor.consolidar_arquivos_ano(args.ano)
                
                # Limpar arquivos chunk antigos na raiz
                conversor.limpar_chunks_antigos(args.ano)
            
            print(f"\nConsolidação do ano {args.ano} concluída com sucesso")
            logger.info("Consolidação concluída com sucesso")
        
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
