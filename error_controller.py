"""
Módulo para controle e registro de erros durante o processamento de arquivos.
"""

import threading
import logging
from datetime import datetime
from pathlib import Path

class ControladorErros:
    """
    Classe para rastrear e registrar erros durante o processamento.
    """
    def __init__(self):
        """
        Inicializa o controlador de erros.
        """
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
        """
        Verifica se um ano específico teve erros.
        
        Parâmetros:
        - ano: Ano a verificar
        
        Retorna:
        - True se o ano teve erros, False caso contrário
        """
        return ano in self.anos_com_falhas
    
    def get_erros_ano(self, ano):
        """
        Retorna todos os erros de um ano específico.
        
        Parâmetros:
        - ano: Ano a consultar
        
        Retorna:
        - Dicionário com erros por etapa
        """
        return self.erros_por_ano.get(ano, {})
    
    def salvar_relatorio_erros(self):
        """
        Registra um relatório detalhado dos erros apenas no log.
        
        Retorna:
        - None
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
    
    def gerar_relatorio_final_arquivos_pulados(self):
        """
        Gera um relatório final detalhado dos arquivos que foram pulados durante o processamento.
        
        Retorna:
        - None
        """
        logger = logging.getLogger(__name__)
        
        if not self.anos_com_falhas:
            print("\n🎉 RELATÓRIO FINAL: Nenhum arquivo foi pulado durante o processamento!")
            print("✅ Todos os arquivos foram processados com sucesso.")
            return
        
        print("\n" + "="*80)
        print("📋 RELATÓRIO FINAL - ARQUIVOS PULADOS")
        print("="*80)
        
        total_arquivos_pulados = 0
        anos_afetados = sorted(self.anos_com_falhas)
        
        print(f"⚠️  RESUMO GERAL:")
        print(f"   • Anos com arquivos pulados: {len(anos_afetados)}")
        print(f"   • Anos afetados: {', '.join(map(str, anos_afetados))}")
        print()
        
        # Detalhes por ano
        for ano in anos_afetados:
            erros_ano = self.get_erros_ano(ano)
            
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
                    erros_ano = self.get_erros_ano(ano)
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

# Instanciar controlador de erros global
controlador_erros = ControladorErros() 