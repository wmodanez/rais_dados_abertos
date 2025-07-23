import logging
import polars as pl
from pathlib import Path
from typing import Set, Optional, Dict, Any

logger = logging.getLogger("filtro_cnae")

class FiltroCNAE:
    """
    Classe genérica para filtrar CNAE baseado em qualquer arquivo CSV com classificação.
    Permite filtrar empregos verdes, da indústria, serviços, etc.
    """
    
    def __init__(self, caminho_arquivo_cnae: str, nome_filtro: str = "CNAE"):
        """
        Inicializa o filtro de CNAE.
        
        Args:
            caminho_arquivo_cnae: Caminho para o arquivo CSV com classificação CNAE
            nome_filtro: Nome descritivo do filtro (ex: "Empregos Verdes", "Indústria", "Serviços")
        """
        self.caminho_arquivo_cnae = Path(caminho_arquivo_cnae)
        self.nome_filtro = nome_filtro
        self._classes_filtradas: Optional[Set[str]] = None
        
        if not self.caminho_arquivo_cnae.exists():
            logger.warning(f"Arquivo CNAE não encontrado: {self.caminho_arquivo_cnae}")
            logger.warning(f"Filtro {nome_filtro} será desabilitado")
    
    def carregar_classes_filtradas(self, situacao_desejada: int = 1) -> Set[str]:
        """
        Carrega as classes CNAE com a situação desejada.
        
        Args:
            situacao_desejada: Valor da coluna SITUACAO para filtrar (padrão: 1)
            
        Returns:
            Conjunto com os códigos das classes CNAE filtradas
        """
        if self._classes_filtradas is not None:
            return self._classes_filtradas
        
        if not self.caminho_arquivo_cnae.exists():
            logger.error(f"Arquivo CNAE não encontrado: {self.caminho_arquivo_cnae}")
            return set()
        
        try:
            # Ler arquivo CSV com separador ';'
            df_cnae = pl.read_csv(
                self.caminho_arquivo_cnae,
                separator=';',
                encoding='utf-8'
            )
            
            # Verificar se as colunas necessárias existem
            colunas_necessarias = ["CLASSE_CNAE", "SITUACAO"]
            colunas_faltantes = [col for col in colunas_necessarias if col not in df_cnae.columns]
            
            if colunas_faltantes:
                logger.error(f"Colunas necessárias não encontradas: {colunas_faltantes}")
                logger.info(f"Colunas disponíveis: {df_cnae.columns}")
                return set()
            
            # Filtrar apenas as classes com a situação desejada
            df_filtrado = df_cnae.filter(pl.col("SITUACAO") == situacao_desejada)
            
            # Extrair os códigos das classes filtradas como strings
            classes_filtradas = set(str(codigo) for codigo in df_filtrado["CLASSE_CNAE"].to_list())
            
            logger.info(f"Carregadas {len(classes_filtradas)} classes CNAE para {self.nome_filtro} (situação {situacao_desejada})")
            logger.debug(f"Classes filtradas: {sorted(classes_filtradas)}")
            
            self._classes_filtradas = classes_filtradas
            return classes_filtradas
            
        except Exception as e:
            logger.error(f"Erro ao carregar classes filtradas: {e}")
            return set()
    
    def filtrar_dataframe(self, df: pl.DataFrame, coluna_cnae: str = "CNAE_2_0_CLASSE", situacao_desejada: int = 1) -> pl.DataFrame:
        """
        Filtra um DataFrame mantendo apenas os registros com CNAE da classificação desejada.
        
        Args:
            df: DataFrame a ser filtrado
            coluna_cnae: Nome da coluna que contém o código CNAE
            situacao_desejada: Valor da coluna SITUACAO para filtrar
            
        Returns:
            DataFrame filtrado apenas com os registros da classificação desejada
        """
        if not self.caminho_arquivo_cnae.exists():
            logger.warning(f"Arquivo CNAE não encontrado, retornando DataFrame original")
            return df
        
        # Carregar classes filtradas se ainda não foram carregadas
        classes_filtradas = self.carregar_classes_filtradas(situacao_desejada)
        
        if not classes_filtradas:
            logger.warning(f"Nenhuma classe encontrada para {self.nome_filtro}, retornando DataFrame original")
            return df
        
        # Verificar se a coluna CNAE existe no DataFrame
        if coluna_cnae not in df.columns:
            logger.warning(f"Coluna {coluna_cnae} não encontrada no DataFrame")
            logger.info(f"Colunas disponíveis: {df.columns}")
            return df
        
        # Contar registros antes do filtro
        total_antes = df.height
        
        # Filtrar apenas registros com CNAE da classificação desejada
        # Converter classes_filtradas para lista para compatibilidade com Polars
        classes_lista = list(classes_filtradas)
        df_filtrado = df.filter(pl.col(coluna_cnae).is_in(classes_lista))
        
        # Contar registros após o filtro
        total_depois = df_filtrado.height
        
        logger.info(f"Filtro {self.nome_filtro} aplicado:")
        logger.info(f"  Total antes: {total_antes:,} registros")
        logger.info(f"  Total depois: {total_depois:,} registros")
        logger.info(f"  Registros mantidos: {total_depois:,} ({total_depois/total_antes*100:.2f}%)")
        
        return df_filtrado
    
    def verificar_disponibilidade(self) -> bool:
        """
        Verifica se o filtro está disponível.
        
        Returns:
            True se o arquivo CNAE existe e pode ser carregado
        """
        if not self.caminho_arquivo_cnae.exists():
            return False
        
        try:
            classes_filtradas = self.carregar_classes_filtradas()
            return len(classes_filtradas) > 0
        except Exception:
            return False
    
    def obter_estatisticas(self, situacao_desejada: int = 1) -> dict:
        """
        Obtém estatísticas sobre as classes filtradas carregadas.
        
        Args:
            situacao_desejada: Valor da coluna SITUACAO para filtrar
            
        Returns:
            Dicionário com estatísticas
        """
        if not self.caminho_arquivo_cnae.exists():
            return {
                "disponivel": False,
                "nome_filtro": self.nome_filtro,
                "total_classes": 0,
                "classes_filtradas": 0,
                "percentual_filtrado": 0.0,
                "situacao_desejada": situacao_desejada
            }
        
        try:
            # Ler arquivo completo
            df_cnae = pl.read_csv(
                self.caminho_arquivo_cnae,
                separator=';',
                encoding='utf-8'
            )
            
            total_classes = df_cnae.height
            classes_filtradas = len(self.carregar_classes_filtradas(situacao_desejada))
            percentual = (classes_filtradas / total_classes * 100) if total_classes > 0 else 0
            
            return {
                "disponivel": True,
                "nome_filtro": self.nome_filtro,
                "total_classes": total_classes,
                "classes_filtradas": classes_filtradas,
                "percentual_filtrado": percentual,
                "situacao_desejada": situacao_desejada
            }
            
        except Exception as e:
            logger.error(f"Erro ao obter estatísticas: {e}")
            return {
                "disponivel": False,
                "nome_filtro": self.nome_filtro,
                "total_classes": 0,
                "classes_filtradas": 0,
                "percentual_filtrado": 0.0,
                "situacao_desejada": situacao_desejada
            } 