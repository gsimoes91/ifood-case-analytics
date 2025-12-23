from loguru import logger

from src.ifood_case.load_data import load_data
from src.ifood_case.utils import get_spark_session

def main():
    spark = get_spark_session(app_name="load_data")
    
    logger.info("Iniciando Pipeline de Dados...")
    
    loader = load_data(spark)
    
    try:
        df_orders = loader.get_orders()
        df_consumers = loader.get_consumers()
        df_restaurants = loader.get_restaurants()
        df_ab_test = loader.get_ab_test()
        
        df_orders.show(5, truncate=False)
        
        logger.success("Pipeline de Carga finalizado com sucesso!")
        
    except Exception as e:
        logger.critical(f"Falha do carregamento dos dados: {e}")
    finally:
        # 5. Encerra a sessão Spark de forma limpa
        spark.stop()
        logger.info("Spark Session encerrada.")

if __name__ == "__main__":
    main()