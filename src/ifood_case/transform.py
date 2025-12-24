import pyspark.sql.functions as f

from loguru import logger
from pyspark.sql import DataFrame

class TransformData:
    """Classe responsável pelo tratamento e limpeza dos dados brutos."""
    def __init__(self):
        logger.info("Tratando dados")

    def clean_orders(self, df_orders: DataFrame) -> DataFrame:
        """Trata tipos de dados, remove duplicados e cria e seleciona colunas essenciais para a analise"""
        logger.info("Limpando dados de pedidos...")

        out = (
            df_orders.select(
                f.col("order_id").cast("string").alias("order_id"),
                f.col("customer_id").cast("string").alias("customer_id"),
                f.col("merchant_id").cast("string").alias("merchant_id"),
                f.to_timestamp("order_created_at").alias("order_created_at"),
                f.col("order_total_amount").cast("double").alias("order_total_amount"),
                f.col("origin_platform").cast("string").alias("origin_platform"),
                f.col("delivery_address_city").cast("string").alias("delivery_address_city"),
                f.col("delivery_address_state").cast("string").alias("delivery_address_state"),
            )
            .withColumn("order_date", f.to_date("order_created_at"))
            .filter(f.col("order_id").isNotNull())
            .filter(f.col("customer_id").isNotNull())
            .filter(f.col("order_created_at").isNotNull())
            .filter(f.col("order_total_amount").isNotNull())
            .filter(f.col("order_total_amount") > 0)
            .dropDuplicates(["order_id"])
        )

        return out

    def clean_consumers(self, df_cons: DataFrame) -> DataFrame:
        """Trata tipos de dados, remove duplicados e cria e seleciona colunas essenciais para a analise"""
        logger.info("Limpando consumers...")

        out = (
            df_cons.select(
                f.col("customer_id").cast("string").alias("customer_id"),
                f.col("language").cast("string").alias("language"),
                f.to_timestamp("created_at").alias("created_at"),
                f.col("active").cast("string").alias("active"),
            )
            .filter(f.col("customer_id").isNotNull())
            .dropDuplicates(["customer_id"])
        )
        return out

    def clean_restaurants(self, df_rest: DataFrame) -> DataFrame:
        """Trata tipos de dados, remove duplicados e cria e seleciona colunas essenciais para a analise"""
        logger.info("Limpando dados de restaurantes...")

        out = (
            df_rest.select(
                f.col("id").cast("string").alias("merchant_id"),
                f.to_timestamp("created_at").alias("created_at"),
                f.col("enabled").cast("string").alias("enabled"),
                f.col("price_range").cast("string").alias("price_range"),
                f.col("average_ticket").cast("double").alias("average_ticket"),
                f.col("merchant_city").cast("string").alias("merchant_city"),
                f.col("merchant_state").cast("string").alias("merchant_state"),
                f.col("merchant_country").cast("string").alias("merchant_country"),
            )
            .filter(f.col("merchant_id").isNotNull())
            .dropDuplicates(["merchant_id"])
            .fillna({"enabled": "INACTIVE"})
        )
        return out

    def clean_ab_test(self, df: DataFrame) -> DataFrame:
        """Trata tipos de dados, remove duplicados e cria e seleciona colunas essenciais para a analise"""
        logger.info("Limpando ab_test_ref...")

        out = (
            df.select(
                f.col("customer_id").cast("string").alias("customer_id"),
                f.col("is_target"),
            )
            .filter(f.col("customer_id").isNotNull())
            .dropDuplicates(["customer_id"])
        )
        return out