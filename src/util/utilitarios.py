import re
import unicodedata
import time
from typing import List, Dict, Any
import logging
from contextlib import contextmanager

logger = logging.getLogger("utilitarios")


class MedidorTempo:
    """
    Classe para medir o tempo de processamento de diferentes etapas.
    """
    
    def __init__(self, nome_processo: str = "Processamento"):
        """
        Inicializa o medidor de tempo.
        
        Args:
            nome_processo: Nome do processo principal
        """
        self.nome_processo = nome_processo
        self.tempo_inicio = None
        self.tempo_fim = None
        self.etapas = {}
        self.etapa_atual = None
        self.tempo_etapa_inicio = None
        
        logger.info(f"Iniciando medição de tempo para: {nome_processo}")
    
    def iniciar_processo(self):
        """Inicia a medição do tempo total do processo."""
        self.tempo_inicio = time.time()
        logger.info(f"Processo '{self.nome_processo}' iniciado")
    
    def finalizar_processo(self):
        """Finaliza a medição do tempo total do processo."""
        self.tempo_fim = time.time()
        tempo_total = self.tempo_fim - self.tempo_inicio
        logger.info(f"Processo '{self.nome_processo}' finalizado em {tempo_total:.2f} segundos")
        return tempo_total
    
    def iniciar_etapa(self, nome_etapa: str):
        """
        Inicia a medição de uma etapa específica.
        
        Args:
            nome_etapa: Nome da etapa
        """
        if self.etapa_atual:
            self.finalizar_etapa()
        
        self.etapa_atual = nome_etapa
        self.tempo_etapa_inicio = time.time()
        logger.info(f"Etapa '{nome_etapa}' iniciada")
    
    def finalizar_etapa(self):
        """Finaliza a medição da etapa atual."""
        if self.etapa_atual and self.tempo_etapa_inicio:
            tempo_etapa = time.time() - self.tempo_etapa_inicio
            self.etapas[self.etapa_atual] = tempo_etapa
            logger.info(f"Etapa '{self.etapa_atual}' finalizada em {tempo_etapa:.2f} segundos")
            self.etapa_atual = None
            self.tempo_etapa_inicio = None
    
    @contextmanager
    def etapa(self, nome_etapa: str):
        """
        Context manager para medir o tempo de uma etapa.
        
        Args:
            nome_etapa: Nome da etapa
            
        Usage:
            with medidor.etapa("Download"):
                # código da etapa
        """
        self.iniciar_etapa(nome_etapa)
        try:
            yield
        finally:
            self.finalizar_etapa()
    
    def obter_resumo(self) -> Dict[str, Any]:
        """
        Retorna um resumo completo dos tempos medidos.
        
        Returns:
            Dicionário com resumo dos tempos
        """
        # Finalizar etapa atual se houver
        if self.etapa_atual:
            self.finalizar_etapa()
        
        # Calcular tempo total
        tempo_total = 0
        if self.tempo_inicio and self.tempo_fim:
            tempo_total = self.tempo_fim - self.tempo_inicio
        elif self.tempo_inicio:
            tempo_total = time.time() - self.tempo_inicio
        
        # Calcular tempo das etapas
        tempo_etapas = sum(self.etapas.values())
        tempo_nao_medido = tempo_total - tempo_etapas
        
        resumo = {
            "processo": self.nome_processo,
            "tempo_total": tempo_total,
            "etapas": self.etapas.copy(),
            "tempo_etapas": tempo_etapas,
            "tempo_nao_medido": tempo_nao_medido,
            "percentual_etapas": (tempo_etapas / tempo_total * 100) if tempo_total > 0 else 0
        }
        
        return resumo
    
    def imprimir_resumo(self):
        """Imprime um resumo formatado dos tempos medidos."""
        resumo = self.obter_resumo()
        
        print(f"\n{'='*60}")
        print(f"RESUMO DE TEMPO - {resumo['processo'].upper()}")
        print(f"{'='*60}")
        print(f"Tempo Total: {resumo['tempo_total']:.2f} segundos")
        print(f"Tempo das Etapas: {resumo['tempo_etapas']:.2f} segundos ({resumo['percentual_etapas']:.1f}%)")
        if resumo['tempo_nao_medido'] > 0:
            print(f"Tempo Não Medido: {resumo['tempo_nao_medido']:.2f} segundos")
        
        if resumo['etapas']:
            print(f"\nDetalhamento por Etapa:")
            print(f"{'Etapa':<30} {'Tempo (s)':<12} {'% do Total':<10}")
            print(f"{'-'*30} {'-'*12} {'-'*10}")
            
            for etapa, tempo in resumo['etapas'].items():
                percentual = (tempo / resumo['tempo_total'] * 100) if resumo['tempo_total'] > 0 else 0
                print(f"{etapa:<30} {tempo:<12.2f} {percentual:<10.1f}%")
        
        print(f"{'='*60}")


def padronizar_nome_coluna(nome_coluna: str) -> str:
    """
    Padroniza o nome de uma coluna removendo caracteres especiais, acentos,
    convertendo para maiúsculas e substituindo espaços por underline.
    
    Args:
        nome_coluna: Nome original da coluna
        
    Returns:
        Nome da coluna padronizado
        
    Exemplos:
        >>> padronizar_nome_coluna("Código do Município")
        'CODIGO_DO_MUNICIPIO'
        >>> padronizar_nome_coluna("Valor da Remuneração (R$)")
        'VALOR_DA_REMUNERACAO_R'
        >>> padronizar_nome_coluna("CPF do Trabalhador")
        'CPF_DO_TRABALHADOR'
    """
    if not nome_coluna:
        return ""
    
    # Converter para string se não for
    nome = str(nome_coluna).strip()
    
    # Remover acentos e normalizar caracteres Unicode
    nome = unicodedata.normalize('NFD', nome)
    nome = ''.join(c for c in nome if not unicodedata.combining(c))
    
    # Substituir caracteres especiais por espaços
    # Manter apenas letras, números e espaços
    nome = re.sub(r'[^a-zA-Z0-9\s]', ' ', nome)
    
    # Remover espaços múltiplos e converter para maiúsculas
    nome = re.sub(r'\s+', ' ', nome).strip().upper()
    
    # Substituir espaços por underline
    nome = nome.replace(' ', '_')
    
    # Remover underlines múltiplos
    nome = re.sub(r'_+', '_', nome)
    
    # Remover underlines no início e fim
    nome = nome.strip('_')
    
    # Se ficou vazio após a padronização, retornar um nome padrão
    if not nome:
        nome = "COLUNA_SEM_NOME"
    
    logger.debug(f"Coluna padronizada: '{nome_coluna}' -> '{nome}'")
    
    return nome


def padronizar_colunas_dataframe(df_colunas: List[str]) -> Dict[str, str]:
    """
    Padroniza uma lista de nomes de colunas e retorna um mapeamento
    entre os nomes originais e os padronizados.
    
    Args:
        df_colunas: Lista com os nomes originais das colunas
        
    Returns:
        Dicionário com mapeamento {nome_original: nome_padronizado}
        
    Exemplo:
        >>> colunas = ["Código do Município", "Valor da Remuneração"]
        >>> mapeamento = padronizar_colunas_dataframe(colunas)
        >>> print(mapeamento)
        {'Código do Município': 'CODIGO_DO_MUNICIPIO', 'Valor da Remuneração': 'VALOR_DA_REMUNERACAO'}
    """
    mapeamento = {}
    
    for coluna in df_colunas:
        nome_padronizado = padronizar_nome_coluna(coluna)
        mapeamento[coluna] = nome_padronizado
    
    # Verificar se há colunas duplicadas após padronização
    nomes_padronizados = list(mapeamento.values())
    duplicados = set([nome for nome in nomes_padronizados if nomes_padronizados.count(nome) > 1])
    
    if duplicados:
        logger.warning(f"Colunas duplicadas após padronização: {duplicados}")
        
        # Adicionar sufixo numérico para colunas duplicadas
        contadores = {}
        for coluna_original, nome_padronizado in mapeamento.items():
            if nome_padronizado in duplicados:
                contadores[nome_padronizado] = contadores.get(nome_padronizado, 0) + 1
                if contadores[nome_padronizado] > 1:
                    mapeamento[coluna_original] = f"{nome_padronizado}_{contadores[nome_padronizado]}"
                    logger.info(f"Coluna renomeada para evitar duplicação: '{coluna_original}' -> '{mapeamento[coluna_original]}'")
    
    logger.info(f"Padronização concluída: {len(mapeamento)} colunas processadas")
    return mapeamento


def aplicar_padronizacao_colunas(df, mapeamento_colunas: Dict[str, str] = None) -> Any:
    """
    Aplica a padronização de colunas em um DataFrame.
    
    Args:
        df: DataFrame a ser processado
        mapeamento_colunas: Mapeamento de nomes de colunas (opcional)
        
    Returns:
        DataFrame com colunas renomeadas
        
    Nota: Esta função é genérica e pode ser adaptada para diferentes
    bibliotecas de DataFrame (pandas, polars, etc.)
    """
    if mapeamento_colunas is None:
        mapeamento_colunas = padronizar_colunas_dataframe(df.columns)
    
    # Renomear colunas
    df_renomeado = df.rename(columns=mapeamento_colunas)
    
    logger.info(f"Colunas renomeadas: {len(mapeamento_colunas)} colunas processadas")
    return df_renomeado


def validar_nome_coluna(nome_coluna: str) -> bool:
    """
    Valida se um nome de coluna é válido após padronização.
    
    Args:
        nome_coluna: Nome da coluna a ser validado
        
    Returns:
        True se o nome é válido, False caso contrário
    """
    nome_padronizado = padronizar_nome_coluna(nome_coluna)
    
    # Verificar se o nome não está vazio
    if not nome_padronizado:
        return False
    
    # Verificar se não começa com número
    if nome_padronizado[0].isdigit():
        return False
    
    # Verificar se contém apenas caracteres válidos
    if not re.match(r'^[A-Z0-9_]+$', nome_padronizado):
        return False
    
    return True


def obter_colunas_invalidas(colunas: List[str]) -> List[str]:
    """
    Identifica colunas com nomes inválidos.
    
    Args:
        colunas: Lista de nomes de colunas
        
    Returns:
        Lista de colunas com nomes inválidos
    """
    colunas_invalidas = []
    
    for coluna in colunas:
        if not validar_nome_coluna(coluna):
            colunas_invalidas.append(coluna)
    
    if colunas_invalidas:
        logger.warning(f"Encontradas {len(colunas_invalidas)} colunas com nomes inválidos: {colunas_invalidas}")
    
    return colunas_invalidas 