from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import json
import logging

# Configurações da API CoinMarketCap
CMC_API_KEY = 'bdce5c9312664f07aad6b91921ad5fdd' 
CMC_API_URL = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'

# Argumentos padrão da DAG
default_args = {
    'owner': 'seu_nome',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 21),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def create_landing_table():
    """
    Cria a tabela na Landing Zone para armazenar dados brutos da API
    """
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS landing_coinmarketcap_raw (
        id SERIAL PRIMARY KEY,
        ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        api_response JSONB,
        status_code INTEGER,
        request_params JSONB
    );
    """
    
    postgres_hook.run(create_table_sql)
    logging.info("Tabela landing_coinmarketcap_raw criada/verificada com sucesso")


def fetch_cryptocurrency_data(**context):
    """
    Busca dados de criptomoedas da API CoinMarketCap
    """
    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': CMC_API_KEY,
    }
    
    parameters = {
        'start': '1',
        'limit': '100',  # Top 100 criptomoedas
        'convert': 'USD'
    }
    
    try:
        response = requests.get(CMC_API_URL, headers=headers, params=parameters)
        data = response.json()
        
        # Armazena informações no XCom para próxima task
        context['ti'].xcom_push(key='api_data', value=data)
        context['ti'].xcom_push(key='status_code', value=response.status_code)
        context['ti'].xcom_push(key='request_params', value=parameters)
        
        logging.info(f"Dados obtidos com sucesso. Status: {response.status_code}")
        logging.info(f"Total de criptomoedas: {len(data.get('data', []))}")
        
        return True
        
    except Exception as e:
        logging.error(f"Erro ao buscar dados: {str(e)}")
        raise


def save_to_landing_zone(**context):
    """
    Salva dados brutos na Landing Zone (PostgreSQL)
    """
    ti = context['ti']
    api_data = ti.xcom_pull(key='api_data', task_ids='fetch_data')
    status_code = ti.xcom_pull(key='status_code', task_ids='fetch_data')
    request_params = ti.xcom_pull(key='request_params', task_ids='fetch_data')
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    insert_sql = """
    INSERT INTO landing_coinmarketcap_raw (api_response, status_code, request_params)
    VALUES (%s, %s, %s)
    RETURNING id;
    """
    
    result = postgres_hook.get_first(
        insert_sql,
        parameters=(
            json.dumps(api_data),
            status_code,
            json.dumps(request_params)
        )
    )
    
    logging.info(f"Dados salvos na Landing Zone com ID: {result[0]}")


# Definição da DAG
with DAG(
    'pipeline_1_coinmarketcap_ingestion',
    default_args=default_args,
    description='Ingestão de dados da CoinMarketCap para Landing Zone',
    schedule='@hourly',  # Executa a cada hora
    catchup=False,
    tags=['coinmarketcap', 'landing', 'ingestion'],
) as dag:
    
    # Task 1: Criar tabela se não existir
    create_table_task = PythonOperator(
        task_id='create_landing_table',
        python_callable=create_landing_table,
    )
    
    # Task 2: Buscar dados da API
    fetch_data_task = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_cryptocurrency_data,
    
    )
    
    # Task 3: Salvar na Landing Zone
    save_data_task = PythonOperator(
        task_id='save_to_landing',
        python_callable=save_to_landing_zone,
        
    )
    
    # Ordem de execução
    create_table_task >> fetch_data_task >> save_data_task