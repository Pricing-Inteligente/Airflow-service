"""
Utilidades para RabbitMQ: envío de mensajes y monitoreo de colas.
"""
import time
import pika
from pymongo import MongoClient


def send_to_rabbit(mongo_uri, mongo_config, rabbit_config):
    """
    Envía IDs de productos y variables a RabbitMQ.
    
    Args:
        mongo_uri: URI de conexión a MongoDB
        mongo_config: Dict con configuración de MongoDB
            - products_db_prefix: Prefijo para BDs de productos
            - variables_db: Nombre de BD de variables
        rabbit_config: Dict con configuración de RabbitMQ
            - host, port, user, pass, vhost
    """
    print(f"=== SENDING TO RABBIT at {rabbit_config['host']}:{rabbit_config['port']} ===")

    # Credenciales
    credentials = pika.PlainCredentials(rabbit_config['user'], rabbit_config['pass'])

    # Conexión a RabbitMQ
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=rabbit_config['host'],
            port=rabbit_config['port'],
            virtual_host=rabbit_config['vhost'],
            credentials=credentials,
        )
    )
    channel = connection.channel()
    client = MongoClient(mongo_uri)

    # ---------------- Productos (Retail-based DBs) ----------------
    channel.queue_declare(queue="productos_ids", durable=True)
    
    # Obtener todas las BDs que empiezan con el prefijo de productos
    all_dbs = client.list_database_names()
    product_dbs = [db for db in all_dbs if db.startswith(mongo_config['products_db_prefix'])]
    
    print(f"[send_to_rabbit] Found {len(product_dbs)} retail databases")
    
    # Recorrer cada BD de retail
    for db_name in sorted(product_dbs):
        db_retail = client[db_name]
        retail_name = db_name.replace(mongo_config['products_db_prefix'], "")
        print(f"\n=== Processing retail DB: {db_name} (retail={retail_name}) ===")
        
        # Recorrer cada colección (producto) dentro de la BD del retail
        for collection_name in reversed(db_retail.list_collection_names()):
            collection = db_retail[collection_name]
            doc_count = collection.count_documents({})
            print(f"--- Processing DB={db_name} collection={collection_name} ({doc_count} docs) ---")

            for doc in collection.find({}, {"_id": 1}):
                # Payload ahora incluye la BD (retail) y la colección (producto)
                payload = {
                    "db": db_name,
                    "collection": collection_name,
                    "_id": str(doc["_id"])
                }
                try:
                    channel.basic_publish(
                        exchange="",
                        routing_key="productos_ids",
                        body=str(payload).encode(),
                        properties=pika.BasicProperties(delivery_mode=2),
                    )
                    print(f"Producto sent: db={db_name} col={collection_name} _id={payload['_id']}")
                except Exception as e:
                    print(f"Error sending producto payload {payload}: {e}")

    # ---------------- Variables ----------------
    db_variables = client[mongo_config['variables_db']]
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


def wait_for_queue_empty(queue_name, rabbit_config):
    """
    Espera indefinidamente a que una cola de RabbitMQ esté vacía.
    Verifica cada 10 minutos.
    
    Args:
        queue_name: Nombre de la cola a monitorear
        rabbit_config: Dict con configuración de RabbitMQ
            - host, port, user, pass
    """
    credentials = pika.PlainCredentials(rabbit_config['user'], rabbit_config['pass'])
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=rabbit_config['host'],
            port=rabbit_config['port'],
            credentials=credentials
        )
    )
    channel = connection.channel()
    
    start_time = time.time()
    print(f"⏳ Esperando a que la cola '{queue_name}' esté vacía...")
    
    while True:
        count = channel.queue_declare(
            queue=queue_name, passive=True
        ).method.message_count
        
        if count == 0:
            elapsed_hours = (time.time() - start_time) / 3600
            print(f"✅ Cola '{queue_name}' vacía. Tiempo total de espera: {elapsed_hours:.2f} horas")
            connection.close()
            return
        
        elapsed_hours = (time.time() - start_time) / 3600
        print(f"📊 Cola '{queue_name}': {count:,} mensajes restantes (esperando {elapsed_hours:.2f}h)")
        time.sleep(600)  # Espera 10 minutos antes de volver a comprobar
    
    connection.close()

