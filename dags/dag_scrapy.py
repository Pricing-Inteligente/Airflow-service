from datetime import datetime, timedelta
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

total_shards = 3 
image_name = "giancass07/scrapy-app"

def send_post_to_gateway():
    url = "http:///10.101.137.251:8080/receive" 
    payload = {"msg": "Hola desde Airflow"}
    
    response = requests.post(url, json=payload)
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
            "MONGO_URI": "mongodb://mongo:27017",
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
