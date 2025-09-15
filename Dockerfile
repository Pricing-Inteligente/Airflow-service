# Base: imagen oficial de Airflow con Python 3.11
FROM apache/airflow:2.11.0-python3.11

# Instalar dependencias adicionales
RUN pip install --no-cache-dir pymongo requests

