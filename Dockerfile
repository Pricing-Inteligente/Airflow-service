FROM apache/airflow:2.11.0

# Dependencias adicionales necesarias para los DAGs (LASSO + migraciones a Milvus)
RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    lxml \
    python-dotenv \
    pymongo \
    psycopg2-binary \
    requests \
    pika \
    numpy \
    pandas \
    scikit-learn \
    sqlalchemy \
    tqdm \
    sentence-transformers \
    pymilvus==2.4.5 \
    torch==2.4.1 \
    python-dateutil