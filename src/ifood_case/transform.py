import pyspark.sql.functions as f

from loguru import logger
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DoubleType

class TransformData:
    """Classe responsável pelo tratamento e limpeza dos dados brutos."""
    def __init__(self):
        logger.info("Tratando dados")

    def clean_orders(self, df_orders: DataFrame) -> DataFrame:
        """Trata tipos de dados, remove duplicados e cria e seleciona colunas essenciais para a analise"""
        logger.info("Limpando dados de pedidos...")

        money_struct = StructType([
            StructField("value", StringType()),
            StructField("currency", StringType())
        ])

        item_structure = ArrayType(StructType([
            StructField("name", StringType()),
            StructField("quantity", DoubleType()),
            StructField("unitPrice", money_struct),
            StructField("totalValue", money_struct),
            StructField("discount", money_struct),
            StructField("externalId", StringType())
           ]))

        return (
            df_orders
            .withColumn("items", f.from_json(f.col("items"), item_structure))
            .select(
                "order_id", 
                "customer_id", 
                "merchant_id",
                "order_created_at", 
                "order_total_amount",
                "origin_platform",
                "delivery_address_city",
                "delivery_address_state", 
                "items"
            )
            .withColumn("order_total_amount", f.col("order_total_amount").cast("double"))
            .withColumn("order_created_at", f.to_timestamp("order_created_at"))
            .withColumn("order_date", f.to_date("order_created_at"))
            .filter(f.col("order_id").isNotNull())
            .filter(f.col("customer_id").isNotNull())
            .filter(f.col("order_created_at").isNotNull())
            .filter(f.col("order_total_amount") > 0)
            .dropDuplicates(["order_id"])
        )
    
    def build_order_items(self, df_orders_clean: DataFrame) -> DataFrame:
        """
        Gera uma tabela 'order_items' (1 linha por item) a partir de orders.items.
        """
        logger.info("Criando tabela de itens (order_items) a partir do array items...")

        return (
            df_orders_clean
            .select(
                "order_id",
                "customer_id",
                "merchant_id",
                "order_created_at",
                "order_date",
                f.explode_outer("items").alias("item")
            )
            .select(
                "order_id",
                "customer_id",
                "merchant_id",
                "order_created_at",
                "order_date",
                f.col("item.externalId").alias("item_external_id"),
                f.col("item.name").alias("item_name"),
                (f.col("item.unitPrice.value").cast("double") / 100).alias("unit_price"),
                (f.col("item.totalValue.value").cast("double") / 100).alias("total_value"),
                (f.col("item.discount.value").cast("double") / 100).alias("item_discount"),
                f.col("item.quantity").alias("quantity")
            )
            .filter(f.col("item_name").isNotNull())
            .filter(f.col("quantity").isNotNull())
            .filter(f.col("unit_price").isNotNull())
            .filter(f.col("quantity") > 0)
        )

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