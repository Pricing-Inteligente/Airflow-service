"""
Configuración centralizada para el DAG de scraping.
"""

# ============================================================
# Environment Configuration
# ============================================================
IS_PROD = False
MONGO_RESTART = True

# ============================================================
# RabbitMQ Wait Control
# ============================================================
# Si es True, esperará a que las colas de RabbitMQ estén vacías antes de migrar a PostgreSQL
# Si es False, migrará inmediatamente sin esperar (útil para ejecución manual)
WAIT_FOR_RABBIT = True  # Cambiar a True si quieres esperar automáticamente (puede tomar ~15 hrs)

# ============================================================
# Sharding Configuration for Products
# ============================================================
# Number of parallel workers for products (numeric sharding by retail)
PRODUCT_WORKERS = 5
# Nota: Cada worker calcula automáticamente qué retailers procesar según su shard
# DEBUG_RETAIL ya no se usa - para debug, ejecuta con shard=0 y total_shards=1

# Variables keep simple numeric sharding
VARIABLE_SHARDS = 2

# ============================================================
# MongoDB Configuration
# ============================================================
MONGO_URI = "192.168.40.10:8580"

# BD fija para productos (colecciones por retail)
MONGO_PRODUCTS_DB = (
    "raw_productos_demo" if IS_PROD else "TEST_raw_productos_demo"
)
MONGO_VARIABLES_DB = (
    "raw_variables_demo" if IS_PROD else "TEST_raw_variables_demo"
)

MONGO_CLEAN_DB = (
    "clean_productos_demo" if IS_PROD else "TEST_clean_productos_demo"
)
MONGO_CLEAN_VAR_DB = (
    "clean_variables_demo" if IS_PROD else "TEST_clean_variables_demo"
)

# ============================================================
# RabbitMQ Configuration
# ============================================================
RABBIT_HOST = "192.168.40.10"
RABBIT_PORT = 8180  # puerto mappeado a 5672 en docker
RABBIT_USER = "admin"
RABBIT_PASS = "adminpassword"
RABBIT_VHOST = "/"  # virtual host por defecto
RABBIT_QUEUE_PRODUCTOS = "productos_ids"  # Nombre de la cola para productos
RABBIT_QUEUE_VARIABLES = "variables_ids"  # Nombre de la cola para variables

# ============================================================
# PostgreSQL Configuration
# ============================================================
PG_HOST = "192.168.40.10"
PG_PORT = 8080
PG_DB = "mydb"
PG_USER = "admin"
PG_PASS = "adminpassword"
PG_TABLE_PRODUCTOS = "demo_tabla_precios"  # Nombre de la tabla para productos
PG_TABLE_VARIABLES = "demo_tabla_macroeconomicas"  # Nombre de la tabla para variables

# ============================================================
# Milvus Configuration
# ============================================================
MILVUS_HOST = "192.168.40.10"
MILVUS_PORT = 19530  # Puerto gRPC de Milvus (no HTTP)
MILVUS_DB = "default"  # Base de datos de Milvus (namespace)
MILVUS_COLLECTION_PRODUCTOS = "demo_products_latam"  # Nombre de la colección para productos
MILVUS_COLLECTION_MACRO = "demo_macro_latam"  # Nombre de la colección para variables macro
MILVUS_COLLECTION_LASSO = "demo_lasso_models"  # Nombre de la colección para modelos LASSO

# ============================================================
# Docker Configuration
# ============================================================
IMAGE_NAME = "sboterop/scrapy-app:latest"

