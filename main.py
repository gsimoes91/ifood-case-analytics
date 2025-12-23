from loguru import logger

from src.ifood_case.utils import get_spark_session
from src.ifood_case.load_data import load_data
from src.ifood_case.transform import transform_data

def main():
    spark = get_spark_session(app_name="ifood_case")
    
    logger.info("Iniciando Pipeline de Dados...")
    
    load = load_data(spark)
    transform = transform_data()
    
    try:
        #carregamento
        df_orders = load.get_orders()
        df_consumers = load.get_consumers()
        df_restaurants = load.get_restaurants()
        df_ab_test = load.get_ab_test()
        
        #tratamento
        df_orders_tr = transform.clean_orders(df_orders)
        df_rest_tr = transform.clean_restaurants(df_restaurants)

        #amostra
        df_orders_tr.select("order_id", "order_total_amount", "order_created_at").show(5)
        
        logger.success("Pipeline com sucesso!")
        
    except Exception as e:
        logger.critical(f"Falha no pipeline: {e}")
    finally:
        # 5. Encerra a sessão Spark de forma limpa
        spark.stop()
        logger.info("Spark Session encerrada.")

if __name__ == "__main__":
    main()