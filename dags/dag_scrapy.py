from datetime import datetime, timedelta
from pymongo import MongoClient
from os import getenv
from dotenv import load_dotenv
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import requests

default_args = {
    "start_date": datetime(2025, 9, 6),
    "retries": 3,
    "retry_delay": timedelta(minutes=1)
}

# Conexión a Mongo
MONGO_URI = "mongodb://10.34.1.50:17048"  
DB_NAME = "raw_productos"                 
COLLECTION_NAME = "arroz" 

total_shards = 3 
image_name = "giancass07/scrapy-app"

# Cargar variables de entorno
load_dotenv()

# Obtener IP de la VPN desde la variable de entorno
VPN_IP = getenv("VPN_IP")
if not VPN_IP:
    raise ValueError("La variable de entorno 'VPN_IP' no está definida") 

# 
def send_post_to_gateway(**kwargs):
    # Conectarse a Mongo
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    
    # Tomar un documento (ejemplo: el primero)
    doc = collection.find_one()  
    
    if doc is None:
        print("No hay documentos en la colección")
        return

    payload = doc
    
    response = requests.post(VPN_IP, json=payload)  # Enviar POST al gateway
    
    print("Response status:", response.status_code)
    print("Response body:", response.json())

with DAG(
    "scrapy_shards_dag",
    schedule_interval=None,
    default_args=default_args,
    catchup=False
) as dag:

    start = EmptyOperator(task_id="start")

    shards = list(range(total_shards))

    scraping_task = DockerOperator.partial(
        task_id="scrapy_shard",
        image=image_name,
        api_version='auto',
        auto_remove="force",
        docker_url="unix://var/run/docker.sock",
        network_mode="airflow_net",
        environment={
            "IS_PROD": "True",
            "MONGO_URI": MONGO_URI,
            "MONGO_RESTART": "False",
        },
    ).expand(
        command=[f"scrapy crawl simple_product_spider -a shard={shard} -a total_shards={total_shards}" for shard in shards]
    )

    send_to_gateway = PythonOperator(
        task_id="send_to_gateway",
        python_callable=send_post_to_gateway
    )

    end = EmptyOperator(task_id="end")

    start >> scraping_task >> send_to_gateway >> end
