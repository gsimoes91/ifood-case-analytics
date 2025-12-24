from loguru import logger

from src.ifood_case.utils import get_spark_session
from src.ifood_case.config import Config
from src.ifood_case.load_data import LoadData
from src.ifood_case.transform import TransformData

def main():
    spark = get_spark_session(app_name="ifood_case")
    
    logger.info("Iniciando Pipeline de Dados...")
    
    load = LoadData(spark)
    transform = TransformData()
    
    try:
        #carregamento
        df_orders = load.get_orders()
        df_consumers = load.get_consumers()
        df_restaurants = load.get_restaurants()
        df_ab_test = load.get_ab_test()
        
        #tratamento
        df_orders_tr = transform.clean_orders(df_orders)
        df_consumers_tr = transform.clean_consumers(df_consumers)
        df_rest_tr = transform.clean_restaurants(df_restaurants)
        df_ab_tr = transform.clean_ab_test(df_ab_test)

        # escrita (silver)
        out_orders = Config.DATA_PROCESSED / "orders"
        out_consumers = Config.DATA_PROCESSED / "consumers"
        out_restaurants = Config.DATA_PROCESSED / "restaurants"
        out_ab = Config.DATA_PROCESSED / "ab_test_ref"

        df_orders_tr.write.mode("overwrite").parquet(str(out_orders))
        df_consumers_tr.write.mode("overwrite").parquet(str(out_consumers))
        df_rest_tr.write.mode("overwrite").parquet(str(out_restaurants))
        df_ab_tr.write.mode("overwrite").parquet(str(out_ab))
        
        logger.success("Pipeline executado com sucesso!")
        
    except Exception as e:
        logger.critical(f"Falha no pipeline: {e}")
        raise
    finally:
        spark.stop()
        logger.info("Spark Session encerrada.")

if __name__ == "__main__":
    main()