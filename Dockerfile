FROM apache/airflow:3.1.0-python3.11

# Copia requirements.txt
COPY requirements.txt /requirements.txt

# Instala dependências
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt

# Define usuário do Airflow
USER airflow