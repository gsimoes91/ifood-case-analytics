import pyspark.sql.functions as f

from loguru import logger
from pyspark.sql import DataFrame

class transform_data:
    """Classe responsável pelo tratamento e limpeza dos dados brutos."""
    
    def __init__(self):
        logger.info("Tratando dados")

    def clean_orders(self, df_orders: DataFrame) -> DataFrame:
        """Trata tipos de dados e remove duplicados de pedidos."""
        logger.info("Limpando dados de pedidos...")
        return (
            df_orders
            .withColumn("order_total_amount", f.col("order_total_amount").cast("double"))
            .withColumn("order_created_at", f.to_timestamp("order_created_at"))
            .filter(f.col("order_id").isNotNull())
            .dropDuplicates(["order_id"])
        )

    def clean_restaurants(self, df_rest: DataFrame) -> DataFrame:
        """Remove duplicados e trata valores nulos em restaurantes."""
        logger.info("Limpando dados de restaurantes...")
        return (
            df_rest
            .dropDuplicates(["id"])
            .fillna({ "enabled": "INACTIVE"})
        )

    def clean_consumers(self, df_cons: DataFrame) -> DataFrame:
        """Remove duplicados de consumidores."""
        logger.info("Limpando dados de consumidores...")
        return df_cons.dropDuplicates(["customer_id"])