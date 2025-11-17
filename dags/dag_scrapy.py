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
from utils.rabbit_utils import send_to_rabbit
from utils.postgres_utils import mongo_productos_to_pg, mongo_variables_to_pg
from services.lasso_runner import run_lasso
from services.milvus_lasso_migrator import migrate as migrate_lasso_to_milvus

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
    "scrapy_shards_dag",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag:

    # START
    start = EmptyOperator(task_id="start")

    # Pull latest Docker image
    pull_image = BashOperator(
        task_id="pull_latest_image",
        bash_command=f"docker pull {settings.IMAGE_NAME}")
    
    # ============================================================
    # MongoDB Cleanup Task (antes de scraping paralelo)
    # ============================================================
    def clean_mongodb(**kwargs):
        """Limpia MongoDB antes de scraping si MONGO_RESTART=True"""
        if not settings.MONGO_RESTART:
            print("MONGO_RESTART=False, saltando limpieza")
            return
        
        from pymongo import MongoClient
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
            db_products[collection_name].drop()
            print(f"[clean_mongodb] Dropped: {collection_name}")
        
        # Limpiar BD de variables
        db_variables_name = settings.MONGO_VARIABLES_DB
        db_variables = client[db_variables_name]
        
        print(f"[clean_mongodb] Borrando colecciones en {db_variables_name}")
        collections_var = db_variables.list_collection_names()
        print(
            f"[clean_mongodb] Encontradas {len(collections_var)} colecciones")
        
        for collection_name in collections_var:
            db_variables[collection_name].drop()
            print(f"[clean_mongodb] Dropped: {collection_name}")
        
        client.close()
        print(
            f"[clean_mongodb] ✅ MongoDB limpiado: {db_products_name}, {db_variables_name}"
        )

    clean_mongo_task = PythonOperator(task_id="clean_mongodb",
                                      python_callable=clean_mongodb)

    # ============================================================
    # Scraping Group
    # ============================================================
    with TaskGroup("scraping_group") as scraping_group:
        # Product scraping task - retail-based
        product_scraping_task = DockerOperator.partial(
            task_id="product_scraping_task",
            image=settings.IMAGE_NAME,
            api_version="auto",
            auto_remove="success",
            docker_url="unix://var/run/docker.sock",
            network_mode="airflow_net",
            mount_tmp_dir=False,
            environment={
                "IS_PROD": str(settings.IS_PROD),
                "MONGO_URI": settings.MONGO_URI,
                "MONGO_RESTART": str(settings.MONGO_RESTART),
            },
        ).expand(command=[
                f"scrapy crawl simple_product_spider -a shard={shard} -a total_shards={settings.PRODUCT_WORKERS}"
                for shard in product_shards
        ])
        
        # Variables scraping task - numeric sharding
        variable_scraping_task = DockerOperator.partial(
            task_id="variable_scraping_task",
            image=settings.IMAGE_NAME,
            api_version="auto",
            auto_remove="success",
            docker_url="unix://var/run/docker.sock",
            network_mode="airflow_net",
            mount_tmp_dir=False,
            environment={
                "IS_PROD": str(settings.IS_PROD),
                "MONGO_URI": settings.MONGO_URI,
                "MONGO_RESTART": str(settings.MONGO_RESTART),
            },
        ).expand(command=[
                f"scrapy crawl simple_variable_spider -a shard={shard} -a total_shards={settings.VARIABLE_SHARDS}"
                for shard in variable_shards
        ])
    
    # ============================================================
    # RabbitMQ Task
    # ============================================================
    def send_to_rabbit_wrapper(**kwargs):
        """Wrapper para send_to_rabbit con configuración"""
        return send_to_rabbit(mongo_uri=settings.MONGO_URI,
            mongo_config={
                'products_db': settings.MONGO_PRODUCTS_DB,
                'variables_db': settings.MONGO_VARIABLES_DB,
            },
            rabbit_config={
                'host': settings.RABBIT_HOST,
                'port': settings.RABBIT_PORT,
                'user': settings.RABBIT_USER,
                'pass': settings.RABBIT_PASS,
                'vhost': settings.RABBIT_VHOST,
                              })

    queue_to_rabbit = PythonOperator(task_id="send_ids_to_rabbit",
                                     python_callable=send_to_rabbit_wrapper)
    
    # ============================================================
    # PostgreSQL Migration Tasks
    # ============================================================
    def mongo_productos_to_pg_wrapper(**kwargs):
        """Wrapper para mongo_productos_to_pg con configuración"""
        rabbit_config = {
            'host': settings.RABBIT_HOST,
            'port': settings.RABBIT_PORT,
            'user': settings.RABBIT_USER,
            'pass': settings.RABBIT_PASS,
        }
        pg_config = {
            'host': settings.PG_HOST,
            'port': settings.PG_PORT,
            'dbname': settings.PG_DB,
            'user': settings.PG_USER,
            'password': settings.PG_PASS,
        }
        return mongo_productos_to_pg(
            mongo_uri=settings.MONGO_URI,
            mongo_clean_db=settings.MONGO_CLEAN_DB,
            pg_config=pg_config,
            wait_for_rabbit_flag=settings.WAIT_FOR_RABBIT,
            rabbit_config=rabbit_config)
    
    def mongo_variables_to_pg_wrapper(**kwargs):
        """Wrapper para mongo_variables_to_pg con configuración"""
        rabbit_config = {
            'host': settings.RABBIT_HOST,
            'port': settings.RABBIT_PORT,
            'user': settings.RABBIT_USER,
            'pass': settings.RABBIT_PASS,
        }
        pg_config = {
            'host': settings.PG_HOST,
            'port': settings.PG_PORT,
            'dbname': settings.PG_DB,
            'user': settings.PG_USER,
            'password': settings.PG_PASS,
        }
        return mongo_variables_to_pg(
            mongo_uri=settings.MONGO_URI,
            mongo_clean_var_db=settings.MONGO_CLEAN_VAR_DB,
            pg_config=pg_config,
            wait_for_rabbit_flag=settings.WAIT_FOR_RABBIT,
            rabbit_config=rabbit_config)
    
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
        os.environ["PG_HOST"] = settings.PG_HOST
        os.environ["PG_PORT"] = str(settings.PG_PORT)
        os.environ["PG_DB"] = settings.PG_DB
        os.environ["PG_USER"] = settings.PG_USER
        os.environ["PG_PASS"] = settings.PG_PASS
        os.environ.setdefault("LASSO_TABLE", "lasso_results")

        result = run_lasso()
        print(
            f"✅ LASSO completado. Filas exportadas: {len(result) if result is not None else 0}"
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
            # Agregar ruta de services al path
            services_path = Path(__file__).parent / "services"
            if str(services_path) not in sys.path:
                sys.path.insert(0, str(services_path))

            # Configurar variables de entorno desde settings
            os.environ["PG_HOST"] = settings.PG_HOST
            os.environ["PG_PORT"] = str(settings.PG_PORT)
            os.environ["PG_DB"] = settings.PG_DB
            os.environ["PG_USER"] = settings.PG_USER
            os.environ["PG_PASSWORD"] = settings.PG_PASS
            os.environ["MILVUS_HOST"] = settings.MILVUS_HOST
            os.environ["MILVUS_PORT"] = str(settings.MILVUS_PORT)
            if hasattr(settings, 'MILVUS_DB'):
                os.environ["MILVUS_DB"] = settings.MILVUS_DB

            # Importar y ejecutar main de product_milvus_migrator.py
            try:
                import product_milvus_migrator
                product_milvus_migrator.main()
                print("✅ Migración de productos a Milvus completada exitosamente")
                return "Migración de productos completada"
            except Exception as e:
                print(f"❌ Error ejecutando migración de productos a Milvus: {e}")
                import traceback
                traceback.print_exc()
                raise

        products_milvus_task = PythonOperator(
            task_id="milvus_products",
            python_callable=migrate_products_to_milvus_wrapper)

        # ============================================================
        # Macro Migration to Milvus Task
        # ============================================================
        def migrate_macro_to_milvus_wrapper(**kwargs):
            """Wrapper para ejecutar migración de variables macro de PostgreSQL a Milvus"""
            # Agregar ruta de services al path
            services_path = Path(__file__).parent / "services"
            if str(services_path) not in sys.path:
                sys.path.insert(0, str(services_path))

            # Configurar variables de entorno desde settings
            os.environ["PG_HOST"] = settings.PG_HOST
            os.environ["PG_PORT"] = str(settings.PG_PORT)
            os.environ["PG_DB"] = settings.PG_DB
            os.environ["PG_USER"] = settings.PG_USER
            os.environ["PG_PASSWORD"] = settings.PG_PASS
            os.environ["MILVUS_HOST"] = settings.MILVUS_HOST
            os.environ["MILVUS_PORT"] = str(settings.MILVUS_PORT)

            # Importar y ejecutar migrate de macro_milvus_migrator.py
            try:
                import macro_milvus_migrator
                macro_milvus_migrator.migrate()
                print(
                    "✅ Migración de variables macro a Milvus completada exitosamente"
                )
                return "Migración de macro completada"
            except Exception as e:
                print(f"❌ Error ejecutando migración de macro a Milvus: {e}")
                import traceback
                traceback.print_exc()
                raise

        macro_milvus_task = PythonOperator(
            task_id="milvus_macro",
            python_callable=migrate_macro_to_milvus_wrapper)

        # ============================================================
        # LASSO to Milvus Migration Task
        # ============================================================
        def migrate_lasso_to_milvus_wrapper(**kwargs):
            """Wrapper para ejecutar migración de resultados LASSO de PostgreSQL a Milvus"""
            # Configurar variables de entorno desde settings
            os.environ["PG_HOST"] = settings.PG_HOST
            os.environ["PG_PORT"] = str(settings.PG_PORT)
            os.environ["PG_DB"] = settings.PG_DB
            os.environ["PG_USER"] = settings.PG_USER
            os.environ["PG_PASSWORD"] = settings.PG_PASS
            os.environ["MILVUS_HOST"] = settings.MILVUS_HOST
            os.environ["MILVUS_PORT"] = str(settings.MILVUS_PORT)

            migrate_lasso_to_milvus()
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
    (
        start
        >> pull_image
        >> clean_mongo_task  # Limpiar MongoDB antes de scraping paralelo
        >> scraping_group  # Scraping paralelo de productos y variables
        >> queue_to_rabbit  # Enviar IDs a RabbitMQ
        >> [task_variables, task_productos]  # Migrar a PostgreSQL en paralelo
        >> lasso_task  # Análisis LASSO (requiere datos en PostgreSQL, crea tabla lasso_results)
        >> milvus_migrations_group  # TaskGroup con las 3 migraciones a Milvus en paralelo
        >> end
    )
