"""
Configuración centralizada para el DAG de scraping.
"""

# ============================================================
# Environment Configuration
# ============================================================
IS_PROD = True
MONGO_RESTART = True

# ============================================================
# RabbitMQ Wait Control
# ============================================================
# Si es True, esperará a que las colas de RabbitMQ estén vacías antes de migrar a PostgreSQL
# Si es False, migrará inmediatamente sin esperar (útil para ejecución manual)
WAIT_FOR_RABBIT = False  # Cambiar a True si quieres esperar automáticamente (puede tomar ~15 hrs)

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
    "raw_productos_playwright" if IS_PROD else "TEST_raw_productos_playwright"
)
MONGO_VARIABLES_DB = (
    "raw_variables_playwright" if IS_PROD else "TEST_raw_variables_playwright"
)

MONGO_CLEAN_DB = "clean_productos"
MONGO_CLEAN_VAR_DB = "clean_variables"

# ============================================================
# RabbitMQ Configuration
# ============================================================
RABBIT_HOST = "192.168.40.10"
RABBIT_PORT = 8180  # puerto mappeado a 5672 en docker
RABBIT_USER = "admin"
RABBIT_PASS = "adminpassword"
RABBIT_VHOST = "/"  # virtual host por defecto

# ============================================================
# PostgreSQL Configuration
# ============================================================
PG_HOST = "192.168.40.10"
PG_PORT = 8080
PG_DB = "mydb"
PG_USER = "admin"
PG_PASS = "adminpassword"

# ============================================================
# Milvus Configuration
# ============================================================
MILVUS_HOST = "192.168.40.10"
MILVUS_PORT = 19530  # Puerto gRPC de Milvus (no HTTP)
MILVUS_DB = "default"  # Base de datos de Milvus (namespace)

# ============================================================
# Docker Configuration
# ============================================================
IMAGE_NAME = "sboterop/scrapy-app:latest"

