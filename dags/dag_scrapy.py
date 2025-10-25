# ---------------- Core ----------------
import json
import os
import time
from datetime import datetime
from datetime import timedelta

# ---------------- External ----------------
import pika
import psycopg2
import requests
from dotenv import load_dotenv
from bson import json_util
from bson import ObjectId
from pymongo import MongoClient

# ---------------- Airflow ----------------
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule


default_args = {
    "start_date": datetime(2025, 9, 6),
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

IS_PROD = True

# ---------------- MongoDB ----------------
MONGO_URI = "192.168.40.10:8580"
MONGO_PRODUCTS_DB = (
    "raw_productos" if IS_PROD else "TEST_raw_productos"
)  # BD para productos
MONGO_VARIABLES_DB = (
    "raw_variables" if IS_PROD else "TEST_raw_variables"
)  # BD para variables

MONGO_CLEAN_DB = "clean_productos"
MONGO_CLEAN_VAR_DB = "clean_variables"

# ---------------- RabbitMQ ----------------
RABBIT_HOST = "192.168.40.10"
RABBIT_PORT = 8180  # puerto mappeado a 5672 en docker
RABBIT_USER = "admin"
RABBIT_PASS = "adminpassword"
RABBIT_VHOST = "/"  # virtual host por defecto

# ---------------- PostgreSQL ----------------
PG_HOST = "192.168.40.10"
PG_PORT = 8080
PG_DB = "mydb"
PG_USER = "admin"
PG_PASS = "adminpassword"

# -----------------------------------------------------------


def send_to_rabbit(**kwargs):

    print(f"=== SENDING TO RABBIT at {RABBIT_HOST}:{RABBIT_PORT} ===")

    # Credenciales
    credentials = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)

    # Conexión a RabbitMQ
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=RABBIT_HOST,
            port=RABBIT_PORT,
            virtual_host=RABBIT_VHOST,
            credentials=credentials,
        )
    )
    channel = connection.channel()
    client = MongoClient(MONGO_URI)

    # ---------------- Productos ----------------
    db_products = client[MONGO_PRODUCTS_DB]
    channel.queue_declare(queue="productos_ids", durable=True)

    # ---------------- Enviar productos ----------------
    for collection_name in reversed(db_products.list_collection_names()):
        collection = db_products[collection_name]
        print(f"--- Processing productos collection: {collection_name} ---")

        for doc in collection.find({}, {"_id": 1}):
            payload = {"collection": collection_name, "_id": str(doc["_id"])}
            try:
                channel.basic_publish(
                    exchange="",
                    routing_key="productos_ids",
                    body=str(payload).encode(),
                    properties=pika.BasicProperties(delivery_mode=2),
                )
                print("Producto sent:", payload)
            except Exception as e:
                print(f"Error sending producto payload {payload}: {e}")

    # ---------------- Variables ----------------
    db_variables = client[MONGO_VARIABLES_DB]
    channel.queue_declare(queue="variables_ids", durable=True)

    # ---------------- Enviar variables ----------------
    for collection_name in reversed(db_variables.list_collection_names()):
        collection = db_variables[collection_name]
        print(f"--- Processing variables collection: {collection_name} ---")

        for doc in collection.find({}, {"_id": 1}):
            payload = {"collection": collection_name, "_id": str(doc["_id"])}
            try:
                channel.basic_publish(
                    exchange="",
                    routing_key="variables_ids",
                    body=str(payload).encode(),
                    properties=pika.BasicProperties(delivery_mode=2),
                )
                print("Variable sent:", payload)
            except Exception as e:
                print(f"Error sending variable payload {payload}: {e}")

    connection.close()


# ---------------- HELPER ----------------
def wait_for_queue_empty(queue_name):
    credentials = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=RABBIT_HOST, port=RABBIT_PORT, credentials=credentials
        )
    )
    channel = connection.channel()
    print(f"Waiting for queue '{queue_name}' to be empty...")
    while True:
        count = channel.queue_declare(
            queue=queue_name, passive=True
        ).method.message_count
        if count == 0:
            print(f"Queue '{queue_name}' is empty, proceeding...")
            break
        print(f"Queue '{queue_name}' has {count} messages, waiting 10m...")
        time.sleep(600)  # Espera 10 minutos antes de volver a comprobar
    connection.close()


# ------------------------------------------

# 🔹 Supone que las variables de conexión ya están definidas:
# MONGO_URI, PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASS
# Y que existe la función wait_for_queue_empty(nombre_cola)

def mongo_productos_to_pg(**kwargs):
    wait_for_queue_empty("productos_ids")

    client = MongoClient(MONGO_URI)
    db = client["clean_productos"]
    collection = db["clean_docs"]

    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS
    )
    cur = conn.cursor()

    # 🧱 Corrección: faltaban comas y orden correcto de columnas
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS productos_clean_fields (
            _id TEXT PRIMARY KEY,
            nombre TEXT,
            marca TEXT,
            precio TEXT,
            referencia TEXT,
            cantidad TEXT,
            unidad TEXT,
            descripcion TEXT,
            fecha TEXT,
            categoria TEXT,
            pais TEXT,
            retail TEXT
        )
    """
    )

    for doc in collection.find({}):
        clean = doc.get("clean_fields", {})
        if not clean:
            continue

        # Campos desde clean_fields
        precio = clean.get("precio") or ""
        referencia = clean.get("referencia") or ""
        cantidad = clean.get("cantidad") or ""
        unidad = clean.get("unidad") or ""
        descripcion = clean.get("descripcion") or ""
        nombre = clean.get("nombre") or ""

        # Campos desde el root del documento
        marca = doc.get("marca") or ""
        pais = doc.get("pais") or ""
        retail = doc.get("retail") or ""
        fecha_scraping = doc.get("fecha_scraping")
        categoria = doc.get("producto") or ""

        # Si no hay fecha, usar actual
        if not fecha_scraping:
            fecha_scraping = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Inserción con manejo de conflicto
        cur.execute(
            """
            INSERT INTO productos_clean_fields (
                _id, nombre, marca, precio, referencia, cantidad, unidad,
                descripcion, fecha, categoria, pais, retail
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (_id) DO UPDATE SET
                nombre = EXCLUDED.nombre,
                marca = EXCLUDED.marca,
                precio = EXCLUDED.precio,
                referencia = EXCLUDED.referencia,
                cantidad = EXCLUDED.cantidad,
                unidad = EXCLUDED.unidad,
                descripcion = EXCLUDED.descripcion,
                fecha = EXCLUDED.fecha,
                categoria = EXCLUDED.categoria,
                pais = EXCLUDED.pais,
                retail = EXCLUDED.retail
        """,
            (
                str(doc.get("_id")),
                nombre,
                marca,
                precio,
                referencia,
                cantidad,
                unidad,
                descripcion,
                fecha_scraping,
                categoria,
                pais,
                retail,
            ),
        )

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Productos guardados en PostgreSQL con campos limpios y externos.")


def mongo_variables_to_pg(**kwargs):
    wait_for_queue_empty("variables_ids")

    client = MongoClient(MONGO_URI)
    db = client["clean_variables"]
    collection = db["clean_docs"]

    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS
    )
    cur = conn.cursor()

    # 🧱 Creación de tabla limpia
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS variables_clean_fields (
            _id TEXT PRIMARY KEY,
            nombre TEXT,
            valor TEXT,
            previous TEXT,
            unit TEXT,
            pais TEXT,
            fecha TEXT
        )
    """
    )

    for doc in collection.find({}):
        clean = doc.get("clean_fields", {})
        if not clean:
            continue

        valor = clean.get("Actual") or clean.get("Last") or ""
        previous = clean.get("Previous") or ""
        unit = clean.get("Unit") or ""
        nombre = doc.get("variable") or ""
        pais = doc.get("pais") or ""

        fecha = datetime.now().strftime("%y.%m.%d.%H")

        cur.execute(
            """
            INSERT INTO variables_clean_fields (_id, nombre, valor, previous, unit, pais, fecha)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (_id) DO UPDATE SET
                nombre = EXCLUDED.nombre,
                valor = EXCLUDED.valor,
                previous = EXCLUDED.previous,
                unit = EXCLUDED.unit,
                pais = EXCLUDED.pais,
                fecha = EXCLUDED.fecha
        """,
            (str(doc.get("_id")), nombre, valor, previous, unit, pais, fecha),
        )

    conn.commit()
    cur.close()
    conn.close()
    print(
        "✅ Variables guardadas en PostgreSQL con _id, nombre, valor, previous, unit, pais, fecha."
    )


total_shards = 3
image_name = "sboterop/scrapy-app:latest"

with DAG(
    "scrapy_shards_dag",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag:

    shards = list(range(total_shards))

    # START
    start = EmptyOperator(task_id="start")

    # Pull latest Docker image
    pull_image = BashOperator(
        task_id="pull_latest_image", bash_command=f"docker pull {image_name}"
    )

    # Grouping scraping tasks
    with TaskGroup("scraping_group") as scraping_group:
        # Product scraping task
        product_scraping_task = DockerOperator.partial(
            task_id="product_scraping_task",
            image=image_name,
            api_version="auto",
            auto_remove="force",
            docker_url="unix://var/run/docker.sock",
            network_mode="airflow_net",
            environment={
                "IS_PROD": IS_PROD,
                "MONGO_URI": MONGO_URI,
                "MONGO_RESTART": "False",
            },
        ).expand(
            command=[
                f"scrapy crawl simple_product_spider -a shard={shard} -a total_shards={total_shards}"
                for shard in shards
            ]
        )
        # Variables scraping task
        variable_scraping_task = DockerOperator.partial(
            task_id="variable_scraping_task",
            image=image_name,
            api_version="auto",
            auto_remove="force",
            docker_url="unix://var/run/docker.sock",
            network_mode="airflow_net",
            environment={
                "IS_PROD": IS_PROD,
                "MONGO_URI": MONGO_URI,
                "MONGO_RESTART": "False",
            },
        ).expand(
            command=[
                f"scrapy crawl simple_variable_spider -a shard={shard} -a total_shards={total_shards}"
                for shard in shards
            ]
        )

    # Task to send data to the worker
    queue_to_rabbit = PythonOperator(
        task_id="send_ids_to_rabbit", python_callable=send_to_rabbit
    )

    # Tasks to save cleaned data to PostgreSQL
    task_productos = PythonOperator(
        task_id="mongo_productos_to_pg", python_callable=mongo_productos_to_pg
    )
    task_variables = PythonOperator(
        task_id="mongo_variables_to_pg", python_callable=mongo_variables_to_pg
    )

    # END
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ONE_SUCCESS)

    (
        start
        >> pull_image
        >> scraping_group
        >> queue_to_rabbit
        >> [task_variables, task_productos]
        >> end
    )
