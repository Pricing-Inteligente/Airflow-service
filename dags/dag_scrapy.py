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


def send_post_to_rabbit(**kwargs):
    client = MongoClient(MONGO_URI)
    db = client[MONGO_PRODUCTS_DB]
    
    VPN_IP="10.101.137.179"
    print("VPN_IP=", VPN_IP)

    print(f"=== SENDING TO RABBIT from {MONGO_PRODUCTS_DB}===")
    for collection_name in db.list_collection_names():
        collection = db[collection_name]
        print(f"--- Processing collection: {collection_name} ---")

        # Iterate over all docs in this collection
        for doc in collection.find({}, {"_id": 1}):  # fetch only _id
            payload = {
                "collection": collection_name,
                "_id": str(doc["_id"])
            }

            print("Document to send:", payload)
            print("Sending payload to gateway...")

            try:
                response = requests.post(f"http://{VPN_IP}:8000/receive", json=payload)
                print("Payload sent:", payload)
                print("Response status:", response.status_code)

                try:
                    print("Response body:", response.json())
                except Exception:
                    print("Non-JSON response:", response.text)

            except Exception as e:
                print(f"Error sending payload for {payload}: {e}")

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
        python_callable=send_post_to_rabbit
    )
    # END
    end = EmptyOperator(task_id="end")

    start >> pull_image >> scraping_group >> send_to_gateway >> end
