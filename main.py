import logging
import argparse
import os
import sys
from datetime import datetime
from pathlib import Path
from tqdm import tqdm

# Importar as classes do módulo util
from src.util import GerenciadorArquivosFTP, DescompactadorArquivos, ConversorParquet, MedidorTempo, PipelineParalelo


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
        description="Gerenciador de arquivos para dados RAIS - Download, descompactação, conversão para Parquet e exportação de campos específicos",
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

  # Pipeline paralelo (máxima eficiência):
  python main.py --converter --ano 2024 --consolidacao

  # Converter apenas campos específicos:
  python main.py --converter --ano 2024 --campos CNAE_2_0_CLASSE VINCULO_ATIVO_31_12 MUNICIPIO

  # Converter apenas campos específicos de todos os anos:
  python main.py --converter --campos CNAE_2_0_CLASSE VINCULO_ATIVO_31_12 MUNICIPIO

  # Converter e apagar arquivos descompactados para economizar espaço:
  python main.py --converter --ano 2024 --limpar-descompactados

  # Pipeline completo com limpeza automática:
  python main.py --converter --ano 2024 --consolidacao --limpar-descompactados

  # Consolidar todos os anos em um único arquivo:
  python main.py --consolidar-todos-anos

  # Consolidar todos os anos com nome personalizado:
  python main.py --consolidar-todos-anos --nome-arquivo RAIS_HISTORICO_COMPLETO.parquet
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
    group.add_argument('--apenas-consolidar', action='store_true', help='Apenas consolidar arquivos já convertidos de um ano específico em um único arquivo RAIS_ANO.parquet')
    group.add_argument('--apenas-consolidar-faixa', action='store_true', help='Apenas consolidar arquivos já convertidos de uma faixa de anos em arquivos RAIS_ANO.parquet')
    group.add_argument('--consolidar-todos-anos', action='store_true', help='Consolidar todos os arquivos RAIS_ANO.parquet em um único arquivo RAIS_COMPLETO.parquet')
    
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
    
    parser.add_argument(
        "--limpar-descompactados",
        action="store_true",
        help="Apagar arquivos TXT descompactados após a conversão bem-sucedida para economizar espaço em disco"
    )
    
    parser.add_argument(
        "--consolidar-todos",
        action="store_true",
        help="Consolidar todos os arquivos RAIS_ANO.parquet em um único arquivo RAIS_COMPLETO.parquet"
    )
    
    parser.add_argument(
        "--nome-arquivo",
        type=str,
        default="RAIS_COMPLETO.parquet",
        help="Nome do arquivo final consolidado (padrão: RAIS_COMPLETO.parquet)"
    )
    
    # Argumentos para filtro de campos durante conversão
    parser.add_argument(
        "--campos",
        type=str,
        nargs='+',
        metavar='CAMPO',
        help="Campos específicos a serem incluídos na conversão (ex: CNAE_2_0_CLASSE VINCULO_ATIVO_31_12 MUNICIPIO)"
    )

    parser.add_argument(
        "--arquivo-filtro-cnae",
        type=str,
        help="Caminho para arquivo CSV com classificação CNAE personalizada (deve ter colunas CLASSE_CNAE e SITUACAO)"
    )
    
    parser.add_argument(
        "--nome-filtro-cnae",
        type=str,
        default="CNAE",
        help="Nome descritivo do filtro CNAE personalizado (padrão: CNAE)"
    )
    
    parser.add_argument(
        "--situacao-filtro-cnae",
        type=int,
        default=1,
        help="Valor da coluna SITUACAO para filtrar no arquivo personalizado (padrão: 1)"
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
            args.apenas_consolidar,
            args.apenas_consolidar_faixa,
            args.consolidar_todos_anos
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
            
            with medidor.etapa("Download"):
                logger.info("Iniciando download de arquivos...")
                
                # Criar pipeline paralelo
                pipeline = PipelineParalelo(
                    max_workers=args.max_workers,
                    chunk_size=args.chunk_size,
                    campos_especificos=args.campos,
                    limpar_arquivos_descompactados=args.limpar_descompactados,
                    arquivo_filtro_cnae=args.arquivo_filtro_cnae,
                    nome_filtro_cnae=args.nome_filtro_cnae,
                    situacao_filtro_cnae=args.situacao_filtro_cnae
                )
                
                # Executar apenas download
                resultado = pipeline.processar_apenas_download(
                    servidor_ftp=args.servidor,
                    diretorio_remoto=args.diretorio_remoto,
                    ano=args.ano,
                    ano_inicio=ano_inicio,
                    ano_fim=ano_fim,
                    max_tentativas=args.max_tentativas,
                    tempo_espera=args.tempo_espera
                )
            
            print(f"\nResultado do download:")
            print(f"  Total de arquivos: {resultado['download']['total']}")
            print(f"  Arquivos baixados: {resultado['download']['concluidos']}")
            print(f"  Falhas: {resultado['download']['falhas']}")
            
            if resultado['download']['falhas'] > 0:
                logger.warning(f"Download concluído com {resultado['download']['falhas']} falhas")
            else:
                logger.info("Download concluído com sucesso")
            
            # Descompactar arquivos se solicitado
            if args.descompactar_apos_baixar:
                logger.info("Iniciando descompactação de arquivos...")
                resultado_descompactar = pipeline.processar_apenas_descompactacao(
                    ano=args.ano,
                    ano_inicio=ano_inicio,
                    ano_fim=ano_fim
                )
                
                print(f"\nResultado da descompactação:")
                print(f"  Total de arquivos: {resultado_descompactar['descompactacao']['total']}")
                print(f"  Arquivos descompactados: {resultado_descompactar['descompactacao']['concluidos']}")
                print(f"  Falhas: {resultado_descompactar['descompactacao']['falhas']}")
                
                if resultado_descompactar['descompactacao']['falhas'] > 0:
                    logger.warning(f"Descompactação concluída com {resultado_descompactar['descompactacao']['falhas']} falhas")
                else:
                    logger.info("Descompactação concluída com sucesso")
        
        elif args.descompactar:
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
            
            with medidor.etapa("Pipeline (Download + Descompactação)"):
                logger.info("Iniciando pipeline: download e descompactação...")
                
                # Criar pipeline paralelo
                pipeline = PipelineParalelo(
                    max_workers=args.max_workers,
                    chunk_size=args.chunk_size,
                    campos_especificos=args.campos,
                    limpar_arquivos_descompactados=args.limpar_descompactados,
                    arquivo_filtro_cnae=args.arquivo_filtro_cnae,
                    nome_filtro_cnae=args.nome_filtro_cnae,
                    situacao_filtro_cnae=args.situacao_filtro_cnae
                )
                
                # Executar download e descompactação
                resultado = pipeline.processar_download_descompactacao(
                    servidor_ftp=args.servidor,
                    diretorio_remoto=args.diretorio_remoto,
                    ano=args.ano,
                    ano_inicio=ano_inicio,
                    ano_fim=ano_fim,
                    max_tentativas=args.max_tentativas,
                    tempo_espera=args.tempo_espera
                )
            
            print(f"\nResultado do pipeline:")
            print(f"  Download - Total: {resultado['download']['total']}, Baixados: {resultado['download']['concluidos']}, Falhas: {resultado['download']['falhas']}")
            print(f"  Descompactação - Total: {resultado['descompactacao']['total']}, Descompactados: {resultado['descompactacao']['concluidos']}, Falhas: {resultado['descompactacao']['falhas']}")
            
            # Verificar se houve falhas
            total_falhas = resultado['download']['falhas'] + resultado['descompactacao']['falhas']
            if total_falhas > 0:
                logger.warning(f"Pipeline concluído com {total_falhas} falhas no total")
            else:
                logger.info("Pipeline concluído com sucesso")
        
        elif args.apenas_descompactar:
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
            
            with medidor.etapa("Descompactação"):
                logger.info("Iniciando apenas descompactação de arquivos...")
                
                # Criar pipeline paralelo
                pipeline = PipelineParalelo(
                    max_workers=args.max_workers,
                    chunk_size=args.chunk_size,
                    campos_especificos=args.campos,
                    limpar_arquivos_descompactados=args.limpar_descompactados,
                    arquivo_filtro_cnae=args.arquivo_filtro_cnae,
                    nome_filtro_cnae=args.nome_filtro_cnae,
                    situacao_filtro_cnae=args.situacao_filtro_cnae
                )
                
                # Executar apenas descompactação
                resultado = pipeline.processar_apenas_descompactacao(
                    ano=args.ano,
                    ano_inicio=ano_inicio,
                    ano_fim=ano_fim
                )
            
            print(f"\nResultado da descompactação:")
            print(f"  Total de arquivos: {resultado['descompactacao']['total']}")
            print(f"  Arquivos descompactados: {resultado['descompactacao']['concluidos']}")
            print(f"  Falhas: {resultado['descompactacao']['falhas']}")
            
            if resultado['descompactacao']['falhas'] > 0:
                logger.warning(f"Descompactação concluída com {resultado['descompactacao']['falhas']} falhas")
            else:
                logger.info("Descompactação concluída com sucesso")
        
        elif args.converter:
            if gerenciador is None:
                logger.error("Gerenciador FTP não foi inicializado")
                return
            
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
            
            with medidor.etapa("Pipeline Paralelo (Download + Descompactação + Conversão)"):
                logger.info("Iniciando pipeline paralelo: download, descompactação e conversão...")
                
                # Criar pipeline paralelo
                pipeline = PipelineParalelo(
                    max_workers=args.max_workers,
                    chunk_size=args.chunk_size,
                    campos_especificos=args.campos,
                    limpar_arquivos_descompactados=args.limpar_descompactados,
                    arquivo_filtro_cnae=args.arquivo_filtro_cnae,
                    nome_filtro_cnae=args.nome_filtro_cnae,
                    situacao_filtro_cnae=args.situacao_filtro_cnae
                )
                
                # Executar pipeline
                resultado = pipeline.processar_completo(
                    servidor_ftp=args.servidor,
                    diretorio_remoto=args.diretorio_remoto,
                    ano=args.ano,
                    ano_inicio=ano_inicio,
                    ano_fim=ano_fim,
                    max_tentativas=args.max_tentativas,
                    tempo_espera=args.tempo_espera,
                    consolidar=args.consolidacao
                )
                
                print(f"\nResultado do pipeline paralelo:")
                print(f"  Download - Total: {resultado['download']['total']}, Concluídos: {resultado['download']['concluidos']}, Falhas: {resultado['download']['falhas']}")
                print(f"  Descompactação - Total: {resultado['descompactacao']['total']}, Concluídos: {resultado['descompactacao']['concluidos']}, Falhas: {resultado['descompactacao']['falhas']}")
                print(f"  Conversão - Total: {resultado['conversao']['total']}, Concluídos: {resultado['conversao']['concluidos']}, Falhas: {resultado['conversao']['falhas']}")
                
                # Verificar se houve falhas
                total_falhas = resultado['download']['falhas'] + resultado['descompactacao']['falhas'] + resultado['conversao']['falhas']
                if total_falhas > 0:
                    logger.warning(f"Pipeline concluído com {total_falhas} falhas no total")
                else:
                    logger.info("Pipeline paralelo concluído com sucesso")
        
        elif args.apenas_converter:
            # Processar argumentos de faixa de anos
            ano_inicio = None
            ano_fim = None
            ano_conversao = args.ano
            
            if args.faixa_anos:
                if len(args.faixa_anos) == 1:
                    ano_inicio = args.faixa_anos[0]
                    ano_fim = None  # Até o último disponível
                    ano_conversao = args.faixa_anos[0]
                    logger.info(f"Faixa de anos: do ano {ano_inicio} até o último disponível")
                elif len(args.faixa_anos) == 2:
                    ano_inicio = args.faixa_anos[0]
                    ano_fim = args.faixa_anos[1]
                    ano_conversao = None  # Usar faixa em vez de ano específico
                    logger.info(f"Faixa de anos: do ano {ano_inicio} até {ano_fim}")
                else:
                    logger.error("--faixa-anos deve receber 1 ou 2 valores")
                    sys.exit(1)
            
            with medidor.etapa("Conversão para Parquet"):
                logger.info("Iniciando apenas conversão para Parquet...")
                
                # Criar pipeline paralelo
                pipeline = PipelineParalelo(
                    max_workers=args.max_workers,
                    chunk_size=args.chunk_size,
                    campos_especificos=args.campos,
                    limpar_arquivos_descompactados=args.limpar_descompactados,
                    arquivo_filtro_cnae=args.arquivo_filtro_cnae,
                    nome_filtro_cnae=args.nome_filtro_cnae,
                    situacao_filtro_cnae=args.situacao_filtro_cnae
                )
                
                # Executar apenas conversão
                resultado = pipeline.processar_apenas_conversao(
                    ano=ano_conversao,
                    ano_inicio=ano_inicio,
                    ano_fim=ano_fim,
                    consolidar=args.consolidacao
                )
            
            print(f"\nResultado da conversão para Parquet:")
            print(f"  Total de arquivos: {resultado['conversao']['total']}")
            print(f"  Arquivos convertidos: {resultado['conversao']['concluidos']}")
            print(f"  Falhas: {resultado['conversao']['falhas']}")
            
            if resultado['conversao']['falhas'] > 0:
                logger.warning(f"Conversão concluída com {resultado['conversao']['falhas']} falhas")
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
            
            with medidor.etapa("Pipeline Paralelo (Descompactação + Conversão)"):
                logger.info("Iniciando pipeline paralelo: descompactação e conversão...")
                
                # Criar pipeline paralelo
                pipeline = PipelineParalelo(
                    max_workers=args.max_workers,
                    chunk_size=args.chunk_size,
                    campos_especificos=args.campos,
                    limpar_arquivos_descompactados=args.limpar_descompactados,
                    arquivo_filtro_cnae=args.arquivo_filtro_cnae,
                    nome_filtro_cnae=args.nome_filtro_cnae,
                    situacao_filtro_cnae=args.situacao_filtro_cnae
                )
                
                # Executar pipeline (sem download)
                resultado = pipeline.processar_sem_download(
                    ano=args.ano,
                    ano_inicio=ano_inicio,
                    ano_fim=ano_fim,
                    consolidar=args.consolidacao
                )
                
                print(f"\nResultado do pipeline paralelo:")
                print(f"  Descompactação - Total: {resultado['descompactacao']['total']}, Concluídos: {resultado['descompactacao']['concluidos']}, Falhas: {resultado['descompactacao']['falhas']}")
                print(f"  Conversão - Total: {resultado['conversao']['total']}, Concluídos: {resultado['conversao']['concluidos']}, Falhas: {resultado['conversao']['falhas']}")
                
                # Verificar se houve falhas
                total_falhas = resultado['descompactacao']['falhas'] + resultado['conversao']['falhas']
                if total_falhas > 0:
                    logger.warning(f"Pipeline concluído com {total_falhas} falhas no total")
                else:
                    logger.info("Pipeline paralelo concluído com sucesso")
        
        elif args.apenas_consolidar:
            # Verificar se o ano foi especificado
            if not args.ano:
                logger.error("--apenas-consolidar requer que o ano seja especificado com --ano")
                sys.exit(1)
            
            with medidor.etapa("Consolidação"):
                logger.info(f"Iniciando apenas consolidação de arquivos do ano {args.ano}...")
                
                # Criar pipeline paralelo para usar seu método de consolidação
                pipeline = PipelineParalelo(
                    max_workers=args.max_workers,
                    chunk_size=args.chunk_size,
                    campos_especificos=args.campos,
                    limpar_arquivos_descompactados=args.limpar_descompactados,
                    arquivo_filtro_cnae=args.arquivo_filtro_cnae,
                    nome_filtro_cnae=args.nome_filtro_cnae,
                    situacao_filtro_cnae=args.situacao_filtro_cnae
                )
                
                # Executar apenas a consolidação
                resultado_consolidacao = pipeline.consolidar_arquivos(args.ano)
                
                if resultado_consolidacao['status'] == 'sucesso':
                    print(f"\nConsolidação do ano {args.ano} concluída com sucesso")
                    print(f"  Arquivo consolidado: {resultado_consolidacao['arquivo_consolidado']}")
                    logger.info("Consolidação concluída com sucesso")
                else:
                    print(f"\nErro na consolidação do ano {args.ano}: {resultado_consolidacao['erro']}")
                    logger.error(f"Erro na consolidação: {resultado_consolidacao['erro']}")
        
        elif args.apenas_consolidar_faixa:
            # Verificar se a faixa de anos foi especificada
            if not args.faixa_anos or len(args.faixa_anos) != 2:
                logger.error("--apenas-consolidar-faixa requer que uma faixa de anos seja especificada com --faixa-anos ANO_INICIO ANO_FIM")
                sys.exit(1)
            
            ano_inicio = args.faixa_anos[0]
            ano_fim = args.faixa_anos[1]
            
            with medidor.etapa("Consolidação de Faixa"):
                logger.info(f"Iniciando apenas consolidação de arquivos da faixa {ano_inicio}-{ano_fim}...")
                
                # Criar pipeline paralelo para usar seu método de consolidação
                pipeline = PipelineParalelo(
                    max_workers=args.max_workers,
                    chunk_size=args.chunk_size,
                    campos_especificos=args.campos,
                    limpar_arquivos_descompactados=args.limpar_descompactados,
                    arquivo_filtro_cnae=args.arquivo_filtro_cnae,
                    nome_filtro_cnae=args.nome_filtro_cnae,
                    situacao_filtro_cnae=args.situacao_filtro_cnae
                )
                
                # Executar apenas a consolidação da faixa
                resultado_consolidacao = pipeline.consolidar_faixa_anos(ano_inicio, ano_fim)
                
                if resultado_consolidacao['status'] == 'sucesso':
                    print(f"\nConsolidação da faixa {ano_inicio}-{ano_fim} concluída com sucesso")
                    print(f"  Anos consolidados: {resultado_consolidacao['anos_consolidados']}")
                    print(f"  Total de anos: {resultado_consolidacao['total_anos']}")
                    logger.info("Consolidação de faixa concluída com sucesso")
                elif resultado_consolidacao['status'] == 'parcial':
                    print(f"\nConsolidação parcial da faixa {ano_inicio}-{ano_fim}")
                    print(f"  Anos consolidados com sucesso: {resultado_consolidacao['anos_consolidados']}")
                    print(f"  Anos com erro: {resultado_consolidacao['anos_com_erro']}")
                    print(f"  Sucessos: {resultado_consolidacao['sucessos']}, Falhas: {resultado_consolidacao['falhas']}")
                    logger.warning("Consolidação de faixa concluída parcialmente")
                else:
                    print(f"\nErro na consolidação da faixa {ano_inicio}-{ano_fim}: {resultado_consolidacao['erro']}")
                    logger.error(f"Erro na consolidação: {resultado_consolidacao['erro']}")
        
        elif args.consolidar_todos_anos:
            with medidor.etapa("Consolidação de Todos os Anos"):
                logger.info("Iniciando consolidação de todos os anos...")
                
                # Criar pipeline paralelo para usar seu método de consolidação
                pipeline = PipelineParalelo(
                    max_workers=args.max_workers,
                    chunk_size=args.chunk_size,
                    campos_especificos=args.campos,
                    limpar_arquivos_descompactados=args.limpar_descompactados,
                    arquivo_filtro_cnae=args.arquivo_filtro_cnae,
                    nome_filtro_cnae=args.nome_filtro_cnae,
                    situacao_filtro_cnae=args.situacao_filtro_cnae
                )
                
                # Executar consolidação de todos os anos
                resultado_consolidacao = pipeline.consolidar_todos_anos(args.nome_arquivo)
                
                if resultado_consolidacao['status'] == 'sucesso':
                    print(f"\nConsolidação de todos os anos concluída com sucesso!")
                    print(f"  Arquivo final: {resultado_consolidacao['arquivo_final']}")
                    print(f"  Tamanho: {resultado_consolidacao['tamanho_gb']:.2f} GB")
                    print(f"  Total de linhas: {resultado_consolidacao['total_linhas']:,}")
                    print(f"  Anos incluídos: {resultado_consolidacao['anos_incluidos']}")
                    print(f"  Arquivos processados: {resultado_consolidacao['arquivos_processados']}")
                    logger.info("Consolidação de todos os anos concluída com sucesso")
                else:
                    print(f"\nErro na consolidação de todos os anos: {resultado_consolidacao['erro']}")
                    logger.error(f"Erro na consolidação: {resultado_consolidacao['erro']}")
        
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
