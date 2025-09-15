FROM apache/airflow:2.11.0
RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    lxml \
    pymongo \
    requests