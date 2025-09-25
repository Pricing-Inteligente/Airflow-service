from datetime import datetime, timedelta
from pymongo import MongoClient
from os import getenv
from bson import json_util, ObjectId
from dotenv import load_dotenv
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
import requests
import json

default_args = {
    "start_date": datetime(2025, 9, 6),
    "retries": 3,
    "retry_delay": timedelta(minutes=1)
}

# Conexión a Mongo
MONGO_URI = "mongodb://192.168.40.10:8580"  
DB_NAME = "raw_productos"                 
COLLECTION_NAME = "arroz" 

total_shards = 3
image_name = "giancass07/scrapy-app:v1.1"

# Cargar variables de entorno
load_dotenv()

VPN_IP = getenv("VPN_IP")
if not VPN_IP:
    raise ValueError("La variable de entorno 'VPN_IP' no está definida") 

def send_post_to_gateway(**kwargs):
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    
    # fetch one doc
    doc = collection.find_one()
    if doc is None:
        print("No hay documentos en la colección")
        return

    # only keep the _id as string
    payload = {"_id": str(doc["_id"])}
    
    print("Document to send:", payload)
    print("Sending payload to gateway...")

    # send to gateway
    response = requests.post(f"http://{VPN_IP}:8000/receive", json=payload)

    print("Payload sent:", payload)
    print("Response status:", response.status_code)
    try:
        print("Response body:", response.json())
    except Exception:
        print("Non-JSON response:", response.text)

with DAG(
    "scrapy_shards_dag",
    schedule_interval=None,
    default_args=default_args,
    catchup=False
) as dag:

    shards = list(range(total_shards))

    # START 
    start = EmptyOperator(task_id="start")
    
    # Pull latest Docker image
    pull_image = BashOperator(
        task_id="pull_latest_image",
        bash_command=f"docker pull {image_name}"
    )

    # Grouping scraping tasks
    with TaskGroup("scraping_group") as scraping_group:
        # Product scraping task
        product_scraping_task = DockerOperator.partial(
            task_id="product_scraping_task",
            image=image_name,
            api_version='auto',
            auto_remove="force",
            docker_url="unix://var/run/docker.sock",
            network_mode="airflow_net",
            environment={
                "IS_PROD": "False",
                "MONGO_URI": MONGO_URI,
                "MONGO_RESTART": "False",
            },
        ).expand(
            command=[f"scrapy crawl simple_product_spider -a shard={shard} -a total_shards={total_shards}" for shard in shards]
        )
        # Variables scraping task
        variable_scraping_task = DockerOperator.partial(
            task_id="variable_scraping_task",
            image=image_name,
            api_version='auto',
            auto_remove="force",
            docker_url="unix://var/run/docker.sock",
            network_mode="airflow_net",
            environment={
                "IS_PROD": "True",
                "MONGO_URI": MONGO_URI,
                "MONGO_RESTART": "True",
            },
        ).expand(
            command=[f"scrapy crawl simple_variable_spider -a shard={shard} -a total_shards={total_shards}" for shard in shards]
        )
        
    # Task to send data to the gateway
    send_to_gateway = PythonOperator(
        task_id="send_to_gateway",
        python_callable=send_post_to_gateway
    )
    # END
    end = EmptyOperator(task_id="end")

    start >> pull_image >> scraping_group >> send_to_gateway >> end
