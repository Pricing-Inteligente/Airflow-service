# Airflow Orchestrator

Pipeline de orquestación basado en Apache Airflow para coordinar el scraping distribuido, la limpieza de MongoDB y la migración de datos hacia RabbitMQ -> PostgreSQL -> Procesamiento Lasso y finalmente -> Milvus.

## Arquitectura
- **Airflow** se ejecuta con `CeleryExecutor`, respaldado por **PostgreSQL** y **Redis** en contenedores Docker.
- El DAG `scrapy_shards_dag` usa `DockerOperator` para levantar workers de Scrapy a partir de la imagen `sboterop/scrapy-app`.
- Se realizan tareas de apoyo:
  - Limpieza previa en MongoDB (`clean_mongodb`).
  - Envío de identificadores a RabbitMQ (`send_ids_to_rabbit`).
  - Migración de colecciones limpias hacia PostgreSQL (`mongo_productos_to_pg` y `mongo_variables_to_pg`).

## Requisitos
- Docker ≥ 24 y Docker Compose v2.
- GNU Make.
- Acceso a los servicios externos configurados en `dags/config/settings.py`:
  - MongoDB (raw y clean).
  - RabbitMQ.
  - PostgreSQL.
- La imagen Docker `sboterop/scrapy-app:latest` debe estar disponible en el registro configurado (puedes cambiarla en `IMAGE_NAME`).

## Estructura principal
```text
airflow/
├── docker-compose.yaml        # Stack Airflow completo (scheduler, webserver, worker, triggerer, etc.)
├── Dockerfile                 # Imagen base de Airflow extendida con dependencias adicionales
├── Makefile                   # Atajos para levantar/parar la pila y crear la red docker
├── dags/
│   ├── dag_scrapy.py          # DAG principal del scraping
│   ├── config/settings.py     # Parámetros de conexión y sharding
│   └── utils/                 # Funciones auxiliares (RabbitMQ, PostgreSQL, etc.)
├── logs/                      # Logs generados por Airflow (montados como volumen)
├── plugins/                   # Plugins personalizados (si se requieren)
└── requirements.txt           # Dependencias extra que instala la imagen extendida
```

## Configuración previa
1. **Crear red Docker**  
   ```bash
   make network
   ```
   La red `airflow_net` es requerida por el `DockerOperator` para comunicarse con otros contenedores (Scrapy, RabbitMQ, etc.).

2. **Archivo `.env` (opcional pero recomendado)**  
   Crea `airflow/.env` para parametrizar la instancia:
   ```env
   AIRFLOW_UID=50000            # UID del usuario que ejecuta Airflow en tu host
   _AIRFLOW_WWW_USER_USERNAME=airflow
   _AIRFLOW_WWW_USER_PASSWORD=airflow
   ```
   También puedes sobrescribir variables del DAG (por ejemplo `IS_PROD`, `MONGO_URI`, etc.) si las referencias desde `dags/config/settings.py`.

3. **Actualizar conexiones externas**  
   Edita `dags/config/settings.py` con las IPs/puertos/credenciales correctos de MongoDB, RabbitMQ y PostgreSQL.  
   - `PRODUCT_WORKERS` y `VARIABLE_SHARDS` controlan el sharding.
   - `WAIT_FOR_RABBIT` determina si las migraciones esperan a que Rabbit procese todas las colas.

4. **Dependencias adicionales**  
   Lista en `requirements.txt` (por defecto `pymongo` y `requests`). Amplía este archivo si tus DAGs requieren librerías extra.

## Puesta en marcha
```bash
make airflow-up           # build + docker compose up -d
```
- La interfaz web estará disponible en `http://localhost:7777`.
- Usuario/password por defecto: `airflow / airflow` (puede modificarse en el `.env`).
- Los logs se guardan en `logs/` y se montan como volumen para su persistencia.

### Servicios incluidos
- `airflow-webserver`: interfaz y API (puerto local 7777 → 8080 interno).
- `airflow-scheduler`: programa las tareas.
- `airflow-worker`: ejecuta tareas del DAG utilizando Celery.
- `airflow-triggerer`: maneja eventos asíncronos.
- `airflow-init`: inicializa la base de datos y el usuario admin.
- `postgres`, `redis`: backend de Airflow.
- `flower` (opcional con `docker compose --profile flower up`): monitoreo de Celery.

## Uso del DAG
1. **Registrar el DAG**: al levantar la pila, Airflow sincroniza automáticamente `dags/dag_scrapy.py`.
2. **Parametros clave** (controlados en `settings.py`):
   - `MONGO_RESTART`: si `True`, `clean_mongodb` vacía las bases antes de scrapear.
   - `IMAGE_NAME`: imagen Docker de Scrapy que ejecuta los spiders.
   - `PRODUCT_WORKERS`, `VARIABLE_SHARDS`: número de shards paralelos.
3. **Ejecución manual**:
   - Desde la UI: activar el DAG y lanzar un run manual.
   - Desde CLI:
     ```bash
     docker compose run --rm airflow-cli dags trigger scrapy_shards_dag
     ```
4. **Monitoreo**:
   - UI de Airflow → Graph View para ver el progreso del pipeline.
   - Logs en la UI o en `logs/scheduler/latest/`.

## Flujo de tareas
1. `start` → `pull_latest_image`: garantiza que se use la última imagen de Scrapy.
2. `clean_mongodb`: limpia colecciones en MongoDB si `MONGO_RESTART` es `True`.
3. `scraping_group`: ejecuta spiders de productos y variables de forma paralela con sharding numérico.
4. `send_ids_to_rabbit`: publica IDs en RabbitMQ para procesos posteriores.
5. `mongo_productos_to_pg` y `mongo_variables_to_pg`: migran datos limpios a PostgreSQL (pueden esperar señales de RabbitMQ dependiendo de `WAIT_FOR_RABBIT`).
6. `end`: marca el cierre del pipeline (éxito si al menos una migración finaliza correctamente).

## Comandos útiles
- Detener la pila:
  ```bash
  make airflow-down
  ```
- Reiniciar (rebuild incluido):
  ```bash
  make airflow-restart
  ```
- Ver estado/servicios:
  ```bash
  make airflow-ps
  ```
- Consultar logs en tiempo real:
  ```bash
  make airflow-logs
  ```

## Integración con Scrapy
- El `DockerOperator` ejecuta la imagen `sboterop/scrapy-app` y le pasa parámetros de sharding (`shard`, `total_shards`).
- Variables de entorno (`IS_PROD`, `MONGO_URI`, `MONGO_RESTART`) se inyectan desde Airflow para controlar el comportamiento del contenedor.
- Asegúrate de que la imagen tenga acceso a los mismos hosts/red que Airflow (`airflow_net`).

## Solución de problemas
- **Permisos en archivos**: si ves archivos creados como root en `dags/` o `logs/`, define `AIRFLOW_UID` en `.env` con tu UID local (`echo $UID`).
- **Errores de conexión**: revisa la red `airflow_net` y que los hostnames/IPs de Mongo, Rabbit y Postgres sean accesibles desde los contenedores.
- **Imagen desactualizada**: `pull_latest_image` puede fallar si no existe la imagen. Actualiza `IMAGE_NAME` o construye la tuya y publícala.
- **Recursos insuficientes**: el servicio `airflow-init` verifica CPU/RAM/Disco. Ajusta la asignación de Docker Desktop/WSL si aparecen advertencias.

## Mantenimiento
- Actualiza dependencias editando `requirements.txt` y reconstruyendo:
  ```bash
  docker compose build airflow-webserver airflow-scheduler airflow-worker airflow-triggerer
  ```
- Para añadir nuevos DAGs o utilidades, colócalos en `dags/` y `plugins/` y Airflow los detectará automáticamente.
- Realiza backups periódicos del directorio `logs/` y de la base de datos de Airflow (PostgreSQL) si necesitas auditoría histórica.


