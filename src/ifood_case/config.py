import os
from pathlib import Path
from dotenv import load_dotenv

# Carrega variáveis do arquivo .env (se você criar um no futuro)
load_dotenv()

class Config:
    #acha a raiz do projeto automaticamente para nao ter conflito com pasta local
    PROJECT_ROOT = Path(__file__).parent.parent.parent
    
    #onde os dados serao baixados e armazenados para posterior tratamento
    DATA_RAW = Path(os.getenv("DATA_RAW", PROJECT_ROOT / "data" / "raw"))
    DATA_PROCESSED = Path(os.getenv("DATA_PROCESSED", PROJECT_ROOT / "data" / "processed"))
    REPORTS_PATH = Path(os.getenv("REPORTS_PATH", PROJECT_ROOT / "reports"))
    LOG_PATH = PROJECT_ROOT / "logs"
    
    #nesse caso, informando as urls por ser algo estatico, mas varia conforme projeto
    DATA_URLS = {
        "order.json.gz": "https://data-architect-test-source.s3-sa-east-1.amazonaws.com/order.json.gz",
        "consumer.csv.gz": "https://data-architect-test-source.s3-sa-east-1.amazonaws.com/consumer.csv.gz",
        "restaurant.csv.gz": "https://data-architect-test-source.s3-sa-east-1.amazonaws.com/restaurant.csv.gz",
        "ab_test_ref.tar.gz": "https://data-architect-test-source.s3-sa-east-1.amazonaws.com/ab_test_ref.tar.gz"
    }

    @classmethod
    def create_dirs(cls):
        #Cria as pastas se não existirem
        os.makedirs(cls.DATA_RAW, exist_ok=True)
        os.makedirs(cls.DATA_PROCESSED, exist_ok=True)
        os.makedirs(cls.REPORTS_PATH, exist_ok=True)
        os.makedirs(cls.LOG_PATH, exist_ok=True)