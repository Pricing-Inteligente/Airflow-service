# Base: imagen oficial de Airflow con Python 3.11
FROM apache/airflow:2.11.0-python3.11

# USER airflow

# Copiar el archivo de requerimientos
COPY requirements.txt .

# Instalar dependencias adicionales
RUN pip install --no-cache-dir -r requirements.txt



