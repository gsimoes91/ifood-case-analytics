import os
from pathlib import Path
from dotenv import load_dotenv

# Carrega variáveis do arquivo .env (se você criar um no futuro)
load_dotenv()

class Config:
    #acha a raiz do projeto automaticamente para nao ter conflito com pasta local
    PROJECT_ROOT = Path(__file__).parent.parent.parent
    
    #onde os dados serao baixados para posterior tratamento
    DATA_PATH = Path(os.getenv("DATA_PATH", PROJECT_ROOT / "data" / "raw"))
    
    #nesse caso, informando as urls por ser algo estatico, mas varia conforme projeto
    DATA_URLS = {
        "order.json.gz": "https://data-architect-test-source.s3-sa-east-1.amazonaws.com/order.json.gz",
        "consumer.csv.gz": "https://data-architect-test-source.s3-sa-east-1.amazonaws.com/consumer.csv.gz",
        "restaurant.csv.gz": "https://data-architect-test-source.s3-sa-east-1.amazonaws.com/restaurant.csv.gz",
        "ab_test_ref.tar.gz": "https://data-architect-test-source.s3-sa-east-1.amazonaws.com/ab_test_ref.tar.gz"
    }

    @classmethod
    def create_dirs(cls):
        #Cria a pasta data/raw se ela não existir
        os.makedirs(cls.DATA_PATH, exist_ok=True)