import requests
from pymongo import MongoClient
from bson import ObjectId

# Función para convertir ObjectId a string recursivamente
def convert_objectid(obj):
    if isinstance(obj, dict):
        return {k: convert_objectid(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_objectid(v) for v in obj]
    elif isinstance(obj, ObjectId):
        return str(obj)
    else:
        return obj

MONGO_URI = "mongodb://10.34.1.50:17048"
DB_NAME = "raw_productos"
COLLECTION_NAME = "arroz"
URL_GATEWAY = "http://10.101.139.15:8000/receive"

def send_post_to_gateway():
    try:
        print("[INFO] Conectando a MongoDB...")
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        print("[INFO] Conexión a MongoDB exitosa")

        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]

        print(f"[INFO] Buscando un documento en la colección '{COLLECTION_NAME}'...")
        doc = collection.find_one()
        if not doc:
            print("[WARNING] No hay documentos en la colección")
            return
        print("[INFO] Documento encontrado")

        # Convertir ObjectId a string
        print("[INFO] Convirtiendo ObjectId a string...")
        payload = convert_objectid(doc)
        print("[INFO] Payload listo para enviar")

        print(f"[INFO] Enviando POST a {URL_GATEWAY}...")
        response = requests.post(URL_GATEWAY, json=payload)
        print("[INFO] POST enviado")

        # Manejo de la respuesta
        if response.status_code == 200:
            try:
                print("[SUCCESS] Respuesta recibida:", response.json())
            except Exception:
                print("[SUCCESS] Respuesta recibida (raw):", response.text)
        elif response.status_code == 404:
            print("[ERROR] 404 Not Found: La URL del gateway no existe o es incorrecta")
            print("[DEBUG] Respuesta:", response.text)
        else:
            print(f"[ERROR] Código de respuesta inesperado: {response.status_code}")
            print("[DEBUG] Respuesta:", response.text)

    except requests.exceptions.RequestException as req_err:
        print("[ERROR] Error en la petición HTTP:", req_err)
    except Exception as e:
        print("[ERROR] Error general:", e)

if __name__ == "__main__":
    send_post_to_gateway()
