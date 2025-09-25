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
import pika
import os

default_args = {
    "start_date": datetime(2025, 9, 6),
    "retries": 3,
    "retry_delay": timedelta(minutes=1)
}

IS_PROD = False

# Conexión a Mongo
MONGO_URI = "192.168.40.10:8580"
MONGO_PRODUCTS_DB  = "raw_productos" if IS_PROD else "TEST_raw_productos" # BD para productos
# MONGO_VARIABLES_DB = "raw_variables" if IS_PROD else "TEST_raw_variables" # BD para variables

total_shards = 3
image_name = "giancass07/scrapy-app:v1.1"


def send_to_rabbit(**kwargs):
    client = MongoClient(MONGO_URI)
    db = client[MONGO_PRODUCTS_DB]

    RABBIT_HOST = "192.168.40.10"
    RABBIT_PORT = 8180  # tu puerto expuesto de RabbitMQ

    print(f"=== SENDING TO RABBIT at {RABBIT_HOST}:{RABBIT_PORT} from {MONGO_PRODUCTS_DB}===")

    # Conexión a RabbitMQ
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBIT_HOST, port=RABBIT_PORT)
    )
    channel = connection.channel()

    # Creamos una cola llamada "mongo_docs"
    channel.queue_declare(queue="mongo_docs", durable=True)

    for collection_name in db.list_collection_names():
        collection = db[collection_name]
        print(f"--- Processing collection: {collection_name} ---")

        for doc in collection.find({}, {"_id": 1}):  # fetch only _id
            payload = {
                "collection": collection_name,
                "_id": str(doc["_id"])
            }

            print("Document to send:", payload)

            try:
                # Enviar a RabbitMQ
                channel.basic_publish(
                    exchange="",
                    routing_key="mongo_docs",
                    body=str(payload).encode(),
                    properties=pika.BasicProperties(delivery_mode=2)  # persistente
                )
                print("Payload sent to Rabbit:", payload)

            except Exception as e:
                print(f"Error sending payload for {payload}: {e}")

    connection.close()
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
        task_id="send_ids_to_rabbit",
        python_callable=send_to_rabbit
    )
    # END
    end = EmptyOperator(task_id="end")

    start >> pull_image >> scraping_group >> send_to_gateway >> end
