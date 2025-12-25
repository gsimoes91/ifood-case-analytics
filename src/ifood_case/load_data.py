import os
import tarfile
import requests

from pathlib import Path
from loguru import logger
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, FloatType, IntegerType, TimestampType, ArrayType

from src.ifood_case.config import Config


class LoadData:
    """
    classe para que a sessao do spark fique armazenada e para deixar o codigo mais limpo nas chamadas
    """
    def __init__(self, spark: SparkSession):
        self.spark = spark
        Config.create_dirs()

    def download_file(self, filename: str):
        """
        funcao para baixar os arquivos
        """
        url = Config.DATA_URLS.get(filename)
        local_path = Config.DATA_RAW / filename
        
        try:
            logger.info(f"Iniciando download: {filename}")
            response = requests.get(url, timeout=60)
            response.raise_for_status()
            
            with open(local_path, 'wb') as f:
                f.write(response.content)
                
            logger.success(f"Arquivo salvo com sucesso: {filename}")
            
        except Exception as e:
            logger.error(f"Falha ao baixar {filename}: {str(e)}")
            raise

    def get_orders(self) -> DataFrame:
        self.download_file("order.json.gz")
        
        schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("merchant_id", StringType(), True),
            StructField("order_created_at", StringType(), True),
            StructField("order_total_amount", DoubleType(), True),
            StructField("origin_platform", StringType(), True),
            StructField("delivery_address_city", StringType(), True),
            StructField("delivery_address_state", StringType(), True),
            StructField("items", StringType(), True)
        ])
        
        df = self.spark.read.schema(schema).json(str(Config.DATA_RAW / "order.json.gz"))
        logger.info("DataFrame de pedidos (orders) carregado.")
        return df

    def get_consumers(self) -> DataFrame:
        self.download_file("consumer.csv.gz")
        path = str(Config.DATA_RAW / "consumer.csv.gz")
        df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(path)
        logger.info("DataFrame de consumidores carregado.")
        return df

    def get_restaurants(self) -> DataFrame:
        self.download_file("restaurant.csv.gz")
        path = str(Config.DATA_RAW / "restaurant.csv.gz")
        df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(path)
        logger.info("DataFrame de restaurantes carregado.")
        return df

    def get_ab_test(self) -> DataFrame:
        self.download_file("ab_test_ref.tar.gz")
        try:
            logger.info("Extraindo arquivos do pacote tar.gz...")
            with tarfile.open(Config.DATA_RAW / "ab_test_ref.tar.gz", "r:gz") as tar:
                tar.extractall(path=Config.DATA_RAW)
            
            df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(
                str(Config.DATA_RAW / "ab_test_ref.csv")
            )
            logger.success("DataFrame de teste A/B carregado após extração.")
            return df
        except Exception as e:
            logger.error(f"Erro ao processar arquivo comprimido: {e}")
            raise