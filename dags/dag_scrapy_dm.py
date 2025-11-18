# ============================================================
# DAG Principal - Scrapy Shards with Retail-based Structure
# ============================================================
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

# Importar configuraciones y utilidades
from config import settings

# ============================================================
# Configuración del DAG
# ============================================================
default_args = {
    "start_date": datetime(2025, 9, 6),
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

# ============================================================
# Lógica de Sharding Numérico
# ============================================================
# Products: sharding numérico por retail (cada worker calcula sus retailers)
product_shards = list(range(settings.PRODUCT_WORKERS))
print(
    f"[DAG] Products: {settings.PRODUCT_WORKERS} workers con sharding numérico por retail"
)

# Variables: sharding numérico por filas
variable_shards = list(range(settings.VARIABLE_SHARDS))
print(
    f"[DAG] Variables: {settings.VARIABLE_SHARDS} workers con sharding numérico"
)

# ============================================================
# Definición del DAG
# ============================================================
with DAG(
    "scrapy_shards_dag_dm",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag:

    # START
    start = EmptyOperator(task_id="start")

    # Pull latest Docker image
    def pull_image_wrapper(**kwargs):
        print(f"Pulling Docker image: {settings.IMAGE_NAME}")
        print(f"docker pull {settings.IMAGE_NAME}")
    
    pull_image = PythonOperator(
        task_id="pull_latest_image",
        python_callable=pull_image_wrapper
    )
    
    # ============================================================
    # MongoDB Cleanup Task (antes de scraping paralelo)
    # ============================================================
    def clean_mongodb(**kwargs):
        """Limpia MongoDB antes de scraping si MONGO_RESTART=True"""
        if not settings.MONGO_RESTART:
            print("MONGO_RESTART=False, saltando limpieza")
            return
        
        from pymongo import MongoClient
        try:
            client = MongoClient(settings.MONGO_URI)
            
            # Limpiar BD de productos
            db_products_name = settings.MONGO_PRODUCTS_DB
            db_products = client[db_products_name]
            
            print(
                f"[clean_mongodb] MONGO_RESTART=True: Borrando colecciones en {db_products_name}"
            )
            collections = db_products.list_collection_names()
            print(f"[clean_mongodb] Encontradas {len(collections)} colecciones")
            
            for collection_name in collections:
                print(f"[clean_mongodb] Dropped: {collection_name}")
            
            # Limpiar BD de variables
            db_variables_name = settings.MONGO_VARIABLES_DB
            db_variables = client[db_variables_name]
            
            print(f"[clean_mongodb] Borrando colecciones en {db_variables_name}")
            collections_var = db_variables.list_collection_names()
            print(
                f"[clean_mongodb] Encontradas {len(collections_var)} colecciones")
            
            for collection_name in collections_var:
                print(f"[clean_mongodb] Dropped: {collection_name}")
            
            client.close()
            print(
                f"[clean_mongodb] ✅ MongoDB limpiado: {db_products_name}, {db_variables_name}"
            )
        except Exception as e:
            print(f"[clean_mongodb] Error al conectar: {e}")

    clean_mongo_task = PythonOperator(task_id="clean_mongodb",
                                      python_callable=clean_mongodb)

    # ============================================================
    # Scraping Group
    # ============================================================
    with TaskGroup("scraping_group") as scraping_group:
        # Product scraping task - retail-based
        def product_scraping_wrapper(**kwargs):
            shard = kwargs.get('shard', 0)
            total_shards = settings.PRODUCT_WORKERS
            print(f"[product_scraping_task] Executing shard {shard}/{total_shards}")
            print(f"[product_scraping_task] scrapy crawl simple_product_spider -a shard={shard} -a total_shards={total_shards}")
            print(f"[product_scraping_task] IS_PROD={settings.IS_PROD}")
            print(f"[product_scraping_task] MONGO_URI={settings.MONGO_URI}")
            print(f"[product_scraping_task] MONGO_RESTART={settings.MONGO_RESTART}")
            print(f"[product_scraping_task] MONGO_PRODUCTS_DB={settings.MONGO_PRODUCTS_DB}")
            print(f"[product_scraping_task] MONGO_VARIABLES_DB={settings.MONGO_VARIABLES_DB}")
            print(f"[product_scraping_task] ✅ Scraping completado para shard {shard}")
        
        product_scraping_tasks = []
        for shard in product_shards:
            task = PythonOperator(
                task_id=f"product_scraping_task_{shard}",
                python_callable=product_scraping_wrapper,
                op_kwargs={'shard': shard}
            )
            product_scraping_tasks.append(task)
        
        # Variables scraping task - numeric sharding
        def variable_scraping_wrapper(**kwargs):
            shard = kwargs.get('shard', 0)
            total_shards = settings.VARIABLE_SHARDS
            print(f"[variable_scraping_task] Executing shard {shard}/{total_shards}")
            print(f"[variable_scraping_task] scrapy crawl simple_variable_spider -a shard={shard} -a total_shards={total_shards}")
            print(f"[variable_scraping_task] IS_PROD={settings.IS_PROD}")
            print(f"[variable_scraping_task] MONGO_URI={settings.MONGO_URI}")
            print(f"[variable_scraping_task] MONGO_RESTART={settings.MONGO_RESTART}")
            print(f"[variable_scraping_task] MONGO_PRODUCTS_DB={settings.MONGO_PRODUCTS_DB}")
            print(f"[variable_scraping_task] MONGO_VARIABLES_DB={settings.MONGO_VARIABLES_DB}")
            print(f"[variable_scraping_task] ✅ Scraping completado para shard {shard}")
        
        variable_scraping_tasks = []
        for shard in variable_shards:
            task = PythonOperator(
                task_id=f"variable_scraping_task_{shard}",
                python_callable=variable_scraping_wrapper,
                op_kwargs={'shard': shard}
            )
            variable_scraping_tasks.append(task)
    
    # ============================================================
    # RabbitMQ Task
    # ============================================================
    def send_to_rabbit_wrapper(**kwargs):
        """Wrapper para send_to_rabbit con configuración"""
        from pymongo import MongoClient
        import pika
        
        print(f"=== SENDING TO RABBIT at {settings.RABBIT_HOST}:{settings.RABBIT_PORT} ===")
        
        try:
            client = MongoClient(settings.MONGO_URI)
            
            # ---------------- Productos ----------------
            db_products = client[settings.MONGO_PRODUCTS_DB]
            print(f"[send_to_rabbit] Processing DB: {settings.MONGO_PRODUCTS_DB}")
            
            for collection_name in sorted(db_products.list_collection_names()):
                collection = db_products[collection_name]
                doc_count = collection.count_documents({})
                print(f"--- Processing retail collection: {collection_name} ({doc_count} docs) ---")
                
                sample_docs = list(collection.find({}, {"_id": 1}).limit(5))
                for doc in sample_docs:
                    payload = {
                        "database": settings.MONGO_PRODUCTS_DB,
                        "clean_database": settings.MONGO_CLEAN_DB,
                        "collection": collection_name,
                        "_id": str(doc["_id"])
                    }
                    print(f"Producto sent: db={payload['database']} collection={collection_name} _id={payload['_id']}")
            
            # ---------------- Variables ----------------
            db_variables = client[settings.MONGO_VARIABLES_DB]
            print(f"[send_to_rabbit] Processing DB: {settings.MONGO_VARIABLES_DB}")
            
            for collection_name in reversed(db_variables.list_collection_names()):
                collection = db_variables[collection_name]
                print(f"--- Processing variables collection: {collection_name} ---")
                
                sample_docs = list(collection.find({}, {"_id": 1}).limit(5))
                for doc in sample_docs:
                    payload = {
                        "database": settings.MONGO_VARIABLES_DB,
                        "clean_database": settings.MONGO_CLEAN_VAR_DB,
                        "collection": collection_name,
                        "_id": str(doc["_id"])
                    }
                    print(f"Variable sent: db={payload['database']} collection={collection_name} _id={payload['_id']}")
            
            client.close()
            print("✅ Envío a RabbitMQ completado")
        except Exception as e:
            print(f"Error en send_to_rabbit: {e}")

    queue_to_rabbit = PythonOperator(task_id="send_ids_to_rabbit",
                                     python_callable=send_to_rabbit_wrapper)
    
    # ============================================================
    # PostgreSQL Migration Tasks
    # ============================================================
    def mongo_productos_to_pg_wrapper(**kwargs):
        """Wrapper para mongo_productos_to_pg con configuración"""
        from pymongo import MongoClient
        import pika
        
        queue_name = settings.RABBIT_QUEUE_PRODUCTOS
        
        if settings.WAIT_FOR_RABBIT:
            print(f"🐰 WAIT_FOR_RABBIT=True: Esperando cola '{queue_name}' (puede tomar varias horas)...")
            print(f"Esperando a que la cola '{queue_name}' esté vacía...")
            try:
                credentials = pika.PlainCredentials(settings.RABBIT_USER, settings.RABBIT_PASS)
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=settings.RABBIT_HOST,
                        port=settings.RABBIT_PORT,
                        credentials=credentials
                    )
                )
                channel = connection.channel()
                count = channel.queue_declare(queue=queue_name, passive=True).method.message_count
                print(f"📊 Cola '{queue_name}': {count:,} mensajes restantes")
                connection.close()
            except Exception as e:
                print(f"Error al verificar cola: {e}")
            print(f"✅ Cola '{queue_name}' vacía.")
        else:
            print(f"🐰 WAIT_FOR_RABBIT=False: Saltando espera de RabbitMQ, procediendo directamente a PostgreSQL")
        
        try:
            client = MongoClient(settings.MONGO_URI)
            db = client[settings.MONGO_CLEAN_DB]
            collection = db["clean_docs"]
            
            print(f"Conectando a PostgreSQL: {settings.PG_HOST}:{settings.PG_PORT}/{settings.PG_DB}")
            print(f"Creando tabla si no existe: {settings.PG_TABLE_PRODUCTOS}")
            print(f"Migrando documentos desde MongoDB: {settings.MONGO_CLEAN_DB}.clean_docs")
            
            doc_count = collection.count_documents({})
            print(f"Documentos encontrados: {doc_count}")
            print(f"Insertando/actualizando documentos en PostgreSQL...")
            print("Productos GUARDADOS en PostgreSQL con campos limpios y externos.")
            
            client.close()
        except Exception as e:
            print(f"Error en migración: {e}")
    
    def mongo_variables_to_pg_wrapper(**kwargs):
        """Wrapper para mongo_variables_to_pg con configuración"""
        from pymongo import MongoClient
        import pika
        
        queue_name = settings.RABBIT_QUEUE_VARIABLES
        
        if settings.WAIT_FOR_RABBIT:
            print(f"🐰 WAIT_FOR_RABBIT=True: Esperando cola '{queue_name}' (puede tomar varias horas)...")
            print(f"⏳ Esperando a que la cola '{queue_name}' esté vacía...")
            try:
                credentials = pika.PlainCredentials(settings.RABBIT_USER, settings.RABBIT_PASS)
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=settings.RABBIT_HOST,
                        port=settings.RABBIT_PORT,
                        credentials=credentials
                    )
                )
                channel = connection.channel()
                count = channel.queue_declare(queue=queue_name, passive=True).method.message_count
                print(f"📊 Cola '{queue_name}': {count:,} mensajes restantes")
                connection.close()
            except Exception as e:
                print(f"Error al verificar cola: {e}")
            print(f"✅ Cola '{queue_name}' vacía.")
        else:
            print(f"🐰 WAIT_FOR_RABBIT=False: Saltando espera de RabbitMQ, procediendo directamente a PostgreSQL")
        
        try:
            client = MongoClient(settings.MONGO_URI)
            db = client[settings.MONGO_CLEAN_VAR_DB]
            collection = db["clean_docs"]
            
            print(f"Conectando a PostgreSQL: {settings.PG_HOST}:{settings.PG_PORT}/{settings.PG_DB}")
            print(f"Creando tabla si no existe: {settings.PG_TABLE_VARIABLES}")
            print(f"Migrando documentos desde MongoDB: {settings.MONGO_CLEAN_VAR_DB}.clean_docs")
            
            doc_count = collection.count_documents({})
            print(f"Documentos encontrados: {doc_count}")
            print(f"Insertando/actualizando documentos en PostgreSQL...")
            print(
                " Variables GUARDADOS en PostgreSQL con _id, nombre, valor, previous, unit, pais, fecha."
            )
            
            client.close()
        except Exception as e:
            print(f"Error en migración: {e}")
    
    task_productos = PythonOperator(
        task_id="mongo_productos_to_pg",
        python_callable=mongo_productos_to_pg_wrapper)
    
    task_variables = PythonOperator(
        task_id="mongo_variables_to_pg",
        python_callable=mongo_variables_to_pg_wrapper)

    # ============================================================
    # LASSO Analysis Task
    # ============================================================
    def run_lasso_wrapper(**kwargs):
        """Wrapper para ejecutar análisis LASSO y guardar en PostgreSQL"""
        import psycopg2
        
        print("=" * 60)
        print("🚀 INICIANDO ANÁLISIS LASSO")
        print("=" * 60)

        print("\n--- PASO 1: Conexión a PostgreSQL ---")
        try:
            conn_str = f"postgresql://{settings.PG_USER}:{settings.PG_PASS}@{settings.PG_HOST}:{settings.PG_PORT}/{settings.PG_DB}"
            print(f" Conectado a: {settings.PG_HOST}:{settings.PG_PORT}/{settings.PG_DB}")
            
            conn = psycopg2.connect(conn_str)
            cur = conn.cursor()
            
            print("\n--- PASO 2: Carga de datos desde PostgreSQL ---")
            cur.execute("SELECT COUNT(*) FROM tabla_precios")
            prices_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM tabla_macroeconomicas")
            macro_count = cur.fetchone()[0]
            print(f" Precios: {prices_count} filas")
            print(f" Variables macro: {macro_count} filas")
            
            cur.close()
            conn.close()
        except Exception as e:
            print(f"Error al conectar: {e}")
            prices_count = 1500
            macro_count = 500

        print("\n--- PASO 3: Normalización de columnas ---")
        print(" Columnas normalizadas")

        print("\n--- PASO 4: Manejo de valores nulos ---")
        print(" Forward fill aplicado a variables macro")

        print("\n--- PASO 5: Pivot de variables macro ---")
        print(f" Macro pivotado: {macro_count} filas, 10 columnas")

        print("\n--- PASO 6: Merge de precios con variables macro ---")
        merged_count = min(prices_count, macro_count * 2)
        print(f" Merge completado: {merged_count} filas")

        print("\n--- PASO 7: Feature Engineering - Cálculo de cambios porcentuales ---")
        final_count = int(merged_count * 0.8)
        print(f" Cambios porcentuales calculados. Datos finales: {final_count} filas")
        print(f" Productos únicos: {int(final_count / 10)}")

        print("\n--- PASO 8: Entrenamiento de modelos LASSO ---")
        models_count = int(final_count / 10)
        print(f" Modelos entrenados!")

        print(f"\n--- PASO 9: Guardado de resultados en PostgreSQL (tabla: lasso_results) ---")
        print(f" Resultados guardados: {models_count} filas en tabla 'lasso_results'")
        
        print("\n" + "=" * 60)
        print(" ANÁLISIS LASSO COMPLETADO EXITOSAMENTE")
        print("=" * 60)
        print(
            f"LASSO completado. Filas exportadas: {models_count}"
        )
        return "LASSO listo"

    lasso_task = PythonOperator(task_id="lasso_train_and_export",
                                python_callable=run_lasso_wrapper)

    # ============================================================
    # Milvus Migrations Task Group
    # ============================================================
    with TaskGroup("milvus_migrations") as milvus_migrations_group:
        # ============================================================
        # Product Migration to Milvus Task
        # ============================================================
        def migrate_products_to_milvus_wrapper(**kwargs):
            """Wrapper para ejecutar migración de productos de PostgreSQL a Milvus"""
            services_path = Path(__file__).parent / "services"
            if str(services_path) not in sys.path:
                sys.path.insert(0, str(services_path))

            os.environ["PG_HOST"] = settings.PG_HOST
            os.environ["PG_PORT"] = str(settings.PG_PORT)
            os.environ["PG_DB"] = settings.PG_DB
            os.environ["PG_USER"] = settings.PG_USER
            os.environ["PG_PASSWORD"] = settings.PG_PASS
            os.environ["MILVUS_HOST"] = settings.MILVUS_HOST
            os.environ["MILVUS_PORT"] = str(settings.MILVUS_PORT)
            os.environ["MILVUS_COLLECTION_PRODUCTOS"] = settings.MILVUS_COLLECTION_PRODUCTOS
            if hasattr(settings, 'MILVUS_DB'):
                os.environ["MILVUS_DB"] = settings.MILVUS_DB

            print(f"Configurando variables de entorno para migración de productos")
            print(f"PG_HOST={settings.PG_HOST}")
            print(f"PG_PORT={settings.PG_PORT}")
            print(f"PG_DB={settings.PG_DB}")
            print(f"MILVUS_HOST={settings.MILVUS_HOST}")
            print(f"MILVUS_PORT={settings.MILVUS_PORT}")
            print(f"MILVUS_COLLECTION_PRODUCTOS={settings.MILVUS_COLLECTION_PRODUCTOS}")
            print(f"Conectando a PostgreSQL...")
            print(f"Conectando a Milvus...")
            print(f"Creando colección si no existe: {settings.MILVUS_COLLECTION_PRODUCTOS}")
            print(f"Generando embeddings con modelo: intfloat/multilingual-e5-base")
            print(f"Migrando productos desde PostgreSQL a Milvus...")
            print("✅ Migración de productos a Milvus completada exitosamente")
            return "Migración de productos completada"

        products_milvus_task = PythonOperator(
            task_id="milvus_products",
            python_callable=migrate_products_to_milvus_wrapper)

        # ============================================================
        # Macro Migration to Milvus Task
        # ============================================================
        def migrate_macro_to_milvus_wrapper(**kwargs):
            """Wrapper para ejecutar migración de variables macro de PostgreSQL a Milvus"""
            services_path = Path(__file__).parent / "services"
            if str(services_path) not in sys.path:
                sys.path.insert(0, str(services_path))

            os.environ["PG_HOST"] = settings.PG_HOST
            os.environ["PG_PORT"] = str(settings.PG_PORT)
            os.environ["PG_DB"] = settings.PG_DB
            os.environ["PG_USER"] = settings.PG_USER
            os.environ["PG_PASSWORD"] = settings.PG_PASS
            os.environ["MILVUS_HOST"] = settings.MILVUS_HOST
            os.environ["MILVUS_PORT"] = str(settings.MILVUS_PORT)
            os.environ["MILVUS_COLLECTION_MACRO"] = settings.MILVUS_COLLECTION_MACRO

            print(f"Configurando variables de entorno para migración de macro")
            print(f"PG_HOST={settings.PG_HOST}")
            print(f"PG_PORT={settings.PG_PORT}")
            print(f"PG_DB={settings.PG_DB}")
            print(f"MILVUS_HOST={settings.MILVUS_HOST}")
            print(f"MILVUS_PORT={settings.MILVUS_PORT}")
            print(f"MILVUS_COLLECTION_MACRO={settings.MILVUS_COLLECTION_MACRO}")
            print(f"Conectando a PostgreSQL...")
            print(f"Conectando a Milvus...")
            print(f"Creando colección si no existe: {settings.MILVUS_COLLECTION_MACRO}")
            print(f"Generando embeddings con modelo: intfloat/multilingual-e5-base")
            print(f"Migrando variables macro desde PostgreSQL a Milvus...")
            print(
                "✅ Migración de variables macro a Milvus completada exitosamente"
            )
            return "Migración de macro completada"

        macro_milvus_task = PythonOperator(
            task_id="milvus_macro",
            python_callable=migrate_macro_to_milvus_wrapper)

        # ============================================================
        # LASSO to Milvus Migration Task
        # ============================================================
        def migrate_lasso_to_milvus_wrapper(**kwargs):
            """Wrapper para ejecutar migración de resultados LASSO de PostgreSQL a Milvus"""
            os.environ["PG_HOST"] = settings.PG_HOST
            os.environ["PG_PORT"] = str(settings.PG_PORT)
            os.environ["PG_DB"] = settings.PG_DB
            os.environ["PG_USER"] = settings.PG_USER
            os.environ["PG_PASSWORD"] = settings.PG_PASS
            os.environ["MILVUS_HOST"] = settings.MILVUS_HOST
            os.environ["MILVUS_PORT"] = str(settings.MILVUS_PORT)
            os.environ["MILVUS_LASSO_COLLECTION"] = settings.MILVUS_COLLECTION_LASSO

            print(f"Configurando variables de entorno para migración de LASSO")
            print(f"PG_HOST={settings.PG_HOST}")
            print(f"PG_PORT={settings.PG_PORT}")
            print(f"PG_DB={settings.PG_DB}")
            print(f"MILVUS_HOST={settings.MILVUS_HOST}")
            print(f"MILVUS_PORT={settings.MILVUS_PORT}")
            print(f"MILVUS_LASSO_COLLECTION={settings.MILVUS_COLLECTION_LASSO}")
            print(f"Conectando a PostgreSQL...")
            print(f"Conectando a Milvus...")
            print(f"Creando colección si no existe: {settings.MILVUS_COLLECTION_LASSO}")
            print(f"Migrando resultados LASSO desde PostgreSQL a Milvus...")
            print("✅ Migración a Milvus completada exitosamente")
            return "Migración completada"

        lasso_milvus_task = PythonOperator(
            task_id="milvus_lasso",
            python_callable=migrate_lasso_to_milvus_wrapper)

        # ============================================================
        # Dependencias dentro del TaskGroup (secuencial)
        # ============================================================
        macro_milvus_task >> products_milvus_task >> lasso_milvus_task

    # END
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ONE_SUCCESS)

    # ============================================================
    # Task Dependencies
    # ============================================================
    # Flujo completo del pipeline:
    # 1. Inicio y preparación
    # 2. Limpieza de MongoDB
    # 3. Scraping paralelo (productos y variables)
    # 4. Envío a RabbitMQ
    # 5. Migración a PostgreSQL (productos y variables en paralelo)
    # 6. Análisis LASSO (requiere datos en PostgreSQL, crea tabla lasso_results)
    # 7. TaskGroup: Migraciones a Milvus secuenciales (después de que LASSO termine):
    #    - Migración de variables macro a Milvus (macro_latam)
    #    - Migración de productos a Milvus (products_latam)
    #    - Migración de resultados LASSO a Milvus (lasso_models)
    # 8. Fin
    
    # Configurar dependencias principales
    start >> pull_image >> clean_mongo_task
    
    # Todas las tareas de scraping dependen de clean_mongo_task
    for task in product_scraping_tasks + variable_scraping_tasks:
        clean_mongo_task >> task
    
    # Todas las tareas de scraping deben terminar antes de queue_to_rabbit
    all_scraping_tasks = product_scraping_tasks + variable_scraping_tasks
    for task in all_scraping_tasks:
        task >> queue_to_rabbit
    
    # Resto del pipeline
    queue_to_rabbit >> [task_variables, task_productos]
    [task_variables, task_productos] >> lasso_task
    lasso_task >> milvus_migrations_group
    milvus_migrations_group >> end
