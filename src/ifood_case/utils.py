import sys
from loguru import logger
from pyspark.sql import SparkSession
from src.ifood_case.config import Config

def get_spark_session(app_name: str = "iFood_Case") -> SparkSession:
    #Cria a pasta data/raw se ela nao existir
    Config.create_dirs()

    #padronizando salvamento dos arquivos de log
    logger.add(
        "data_processing_{time:YYYY-MM-DD}.log",
        rotation="00:00",        # Novo arquivo todo dia à meia-noite
        retention="1 week",      # Mantém apenas a última semana
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
        compression="zip"        # Opcional: comprime logs antigos para economizar espaço
    )
    
    #Inicia a sessao no Spark
    logger.info(f"Subindo Spark Session: {app_name}")
    
    spark = (
        SparkSession
        .builder
        .master("local[*]")
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    
    # Silencia logs técnicos do Java (só mostra se der erro grave)
    spark.sparkContext.setLogLevel("ERROR")
    
    return spark