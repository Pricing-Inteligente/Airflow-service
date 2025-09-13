from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    "start_date": datetime(2025, 9, 6),
    "retries": 3,                  # Número de reintentos
    "retry_delay": timedelta(minutes=1)  # Tiempo entre reintentos
}

total_shards = 3 
image_name = "giancass07/scrapy-app"

with DAG(
    "scrapy_shards_dag",
    schedule_interval=None,
    default_args=default_args,
    catchup=False
) as dag:

    start = EmptyOperator(task_id="start")

    shards = list(range(total_shards))

    scrap_tasks = DockerOperator.partial(
        # -------------------- Identificación de la task --------------------
        task_id="scrapy_shard",
        # -------------------- Imagen y ejecución de Docker --------------------
        image=image_name,
        api_version='auto',
        auto_remove="force",
        docker_url="unix://var/run/docker.sock",
        network_mode="airflow_net",
        # -------------------- Variables de entorno para el contenedor --------------------
        environment={
            "IS_PROD": "True",
            "MONGO_URI": "mongodb://mongo:27017",
            "MONGO_RESTART": "False",
        },
    ).expand(
        command=[f"scrapy crawl simple_product_spider -a shard={shard} -a total_shards={total_shards}" for shard in shards]
    )

    end = EmptyOperator(task_id="end")

    start >> scrap_tasks >> end
