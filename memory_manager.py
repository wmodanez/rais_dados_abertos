"""
Módulo para gerenciamento de memória durante o processamento de arquivos grandes.
"""

import os
import psutil
import gc
import logging
import time
from pathlib import Path

class GerenciadorMemoria:
    """
    Classe para monitorar e gerenciar o uso de memória durante o processamento de arquivos grandes.
    Implementa estratégias para evitar erros de memória insuficiente.
    """
    
    def __init__(self, limiar_aviso=65, limiar_critico=80, intervalo_verificacao=5):
        """
        Inicializa o gerenciador de memória.
        
        Parâmetros:
        - limiar_aviso: Porcentagem de uso de memória que dispara avisos (padrão: 65%)
        - limiar_critico: Porcentagem de uso de memória que dispara ações críticas (padrão: 80%)
        - intervalo_verificacao: Intervalo em segundos entre verificações de memória (padrão: 5s)
        """
        self.limiar_aviso = limiar_aviso
        self.limiar_critico = limiar_critico
        self.intervalo_verificacao = intervalo_verificacao
        self.ultima_verificacao = 0
        self.logger = logging.getLogger(__name__)
    
    def obter_uso_memoria(self):
        """
        Obtém o uso atual de memória do processo Python.
        
        Retorna:
        - Dicionário com informações de uso de memória
        """
        processo = psutil.Process(os.getpid())
        info_memoria = processo.memory_info()
        
        # Calcular porcentagens
        memoria_total = psutil.virtual_memory().total
        porcentagem_processo = (info_memoria.rss / memoria_total) * 100
        porcentagem_sistema = psutil.virtual_memory().percent
        
        return {
            'rss': info_memoria.rss / (1024 * 1024),  # MB
            'vms': info_memoria.vms / (1024 * 1024),  # MB
            'porcentagem_processo': porcentagem_processo,
            'porcentagem_sistema': porcentagem_sistema,
            'memoria_total': memoria_total / (1024 * 1024 * 1024)  # GB
        }
    
    def verificar_memoria(self, forcar=False):
        """
        Verifica o uso de memória e toma ações se necessário.
        
        Parâmetros:
        - forcar: Se True, força a verificação mesmo que o intervalo não tenha passado
        
        Retorna:
        - Dicionário com informações de uso de memória e status
        """
        agora = time.time()
        
        # Verificar apenas se passou o intervalo ou se forçado
        if not forcar and (agora - self.ultima_verificacao) < self.intervalo_verificacao:
            return None
        
        self.ultima_verificacao = agora
        info = self.obter_uso_memoria()
        
        # Definir status baseado nos limiares
        if info['porcentagem_sistema'] >= self.limiar_critico:
            status = 'CRITICO'
            self.logger.warning(f"⚠️ USO DE MEMÓRIA CRÍTICO: {info['porcentagem_sistema']:.1f}% do sistema, {info['porcentagem_processo']:.1f}% do processo ({info['rss']:.1f} MB)")
            self.limpar_memoria(nivel='agressivo')
        elif info['porcentagem_sistema'] >= self.limiar_aviso:
            status = 'AVISO'
            self.logger.info(f"⚠️ USO DE MEMÓRIA ALTO: {info['porcentagem_sistema']:.1f}% do sistema, {info['porcentagem_processo']:.1f}% do processo ({info['rss']:.1f} MB)")
            self.limpar_memoria(nivel='normal')
        else:
            status = 'OK'
        
        info['status'] = status
        return info
    
    def limpar_memoria(self, nivel='normal'):
        """
        Limpa a memória usando diferentes estratégias dependendo do nível.
        
        Parâmetros:
        - nivel: 'normal' ou 'agressivo'
        
        Retorna:
        - True se a limpeza foi bem-sucedida
        """
        self.logger.info(f"🧹 Iniciando limpeza de memória (nível: {nivel})")
        
        # Coletar lixo
        for _ in range(3 if nivel == 'normal' else 5):
            gc.collect()
        
        # Se for agressivo, tentar liberar mais memória
        if nivel == 'agressivo':
            # Forçar coleta de objetos não referenciados
            gc.collect(0)
            gc.collect(1)
            gc.collect(2)
            
            # Sugerir ao sistema para liberar memória não utilizada
            if hasattr(os, 'posix_fadvise'):  # Linux/Unix
                try:
                    os.posix_fadvise(0, 0, 0, os.POSIX_FADV_DONTNEED)
                except Exception:
                    pass
        
        # Verificar resultado
        info_pos = self.obter_uso_memoria()
        self.logger.info(f"🧹 Limpeza concluída: {info_pos['porcentagem_sistema']:.1f}% do sistema, {info_pos['rss']:.1f} MB do processo")
        
        return True
    
    def verificar_e_salvar_intermediario(self, df, pasta_destino, nome_base, contador, limiar_salvar=75):
        """
        Verifica o uso de memória e salva um DataFrame intermediário se necessário.
        
        Parâmetros:
        - df: DataFrame a ser salvo
        - pasta_destino: Pasta onde salvar o arquivo
        - nome_base: Nome base para o arquivo
        - contador: Contador para identificar o arquivo
        - limiar_salvar: Porcentagem de uso de memória que dispara o salvamento
        
        Retorna:
        - True se salvou, False caso contrário
        """
        info = self.verificar_memoria(forcar=True)
        
        if info['porcentagem_sistema'] >= limiar_salvar:
            # Criar pasta se não existir
            pasta = Path(pasta_destino)
            pasta.mkdir(exist_ok=True, parents=True)
            
            # Nome do arquivo temporário
            arquivo_temp = pasta / f"{nome_base}_temp_{contador}.parquet"
            
            # Salvar DataFrame
            self.logger.info(f"💾 Salvando DataFrame intermediário em {arquivo_temp}")
            
            try:
                df.write_parquet(arquivo_temp, compression="zstd", compression_level=1)
                self.logger.info(f"✅ DataFrame intermediário salvo com sucesso")
                
                # Limpar memória
                self.limpar_memoria(nivel='agressivo')
                return True
            except Exception as e:
                self.logger.error(f"❌ Erro ao salvar DataFrame intermediário: {str(e)}")
                return False
        
        return False

# Instanciar gerenciador de memória global
gerenciador_memoria = GerenciadorMemoria(limiar_aviso=65, limiar_critico=80) 