from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import requests
from pymongo import MongoClient
from bson import ObjectId
from os import getenv
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()
VPN_IP = getenv("VPN_IP")
if not VPN_IP:
    raise ValueError("La variable de entorno 'VPN_IP' no está definida")

URL_GATEWAY = f"http://{VPN_IP}:8000/receive"
MONGO_URI = "mongodb://10.34.1.50:17048"
DB_NAME = "raw_productos"
COLLECTION_NAME = "arroz"

# Función recursiva para convertir ObjectId a string
def convert_objectid(obj):
    if isinstance(obj, dict):
        return {k: convert_objectid(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_objectid(v) for v in obj]
    elif isinstance(obj, ObjectId):
        return str(obj)
    else:
        return obj

def send_post_to_gateway():
    print("Iniciando conexión a MongoDB...")
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        print("Conexión a MongoDB exitosa")
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]

        doc = collection.find_one()
        if not doc:
            print("No hay documentos en la colección")
            return

        payload = convert_objectid(doc)
        print("Documento listo para enviar:", payload)

        response = requests.post(URL_GATEWAY, json=payload)
        print("Response status:", response.status_code)
        try:
            print("Response body:", response.json())
        except Exception:
            print("Response body (raw):", response.text)

    except Exception as e:
        print("Error:", e)

# Definición del DAG
with DAG(
    dag_id="dag_get_mongo",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["mongo", "gateway"]
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    task_send_post = PythonOperator(
        task_id="send_post_to_gateway",
        python_callable=send_post_to_gateway
    )

    # Secuencia de tareas
    start >> task_send_post >> end
