from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import json
import logging

default_args = {
    'owner': 'seu_nome',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 21),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

def create_staging_tables():
    """
    Cria tabelas na Staging Zone para dados limpos
    """
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Tabela principal de criptomoedas
    create_crypto_table = """
    CREATE TABLE IF NOT EXISTS staging_cryptocurrencies (
        crypto_id INTEGER PRIMARY KEY,
        symbol VARCHAR(20) NOT NULL,
        name VARCHAR(200) NOT NULL,
        slug VARCHAR(200),
        last_updated TIMESTAMP,
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    # Tabela de métricas de preço
    create_metrics_table = """
    CREATE TABLE IF NOT EXISTS staging_crypto_metrics (
        id SERIAL PRIMARY KEY,
        crypto_id INTEGER REFERENCES staging_cryptocurrencies(crypto_id),
        price_usd DECIMAL(20, 8),
        volume_24h DECIMAL(20, 2),
        market_cap DECIMAL(20, 2),
        percent_change_1h DECIMAL(10, 4),
        percent_change_24h DECIMAL(10, 4),
        percent_change_7d DECIMAL(10, 4),
        circulating_supply DECIMAL(20, 2),
        total_supply DECIMAL(20, 2),
        max_supply DECIMAL(20, 2),
        recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(crypto_id, recorded_at)
    );
    """
    
    postgres_hook.run(create_crypto_table)
    postgres_hook.run(create_metrics_table)
    logging.info("Tabelas de staging criadas com sucesso")


def clean_and_transform_data(**context):
    """
    Lê dados brutos da Landing Zone, limpa e transforma
    """
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Busca dados mais recentes da landing zone
    query = """
    SELECT id, api_response, ingestion_timestamp
    FROM landing_coinmarketcap_raw
    WHERE ingestion_timestamp >= NOW() - INTERVAL '2 hours'
    ORDER BY ingestion_timestamp DESC
    LIMIT 1;
    """
    
    result = postgres_hook.get_first(query)
    
    if not result:
        logging.warning("Nenhum dado novo encontrado na Landing Zone")
        return False
    
    landing_id, api_response, ingestion_time = result
    data = json.loads(api_response) if isinstance(api_response, str) else api_response
    
    # Extrai e limpa dados
    cryptocurrencies = data.get('data', [])
    cleaned_data = []
    
    for crypto in cryptocurrencies:
        try:
            # Limpeza básica
            cleaned = {
                'crypto_id': crypto['id'],
                'symbol': crypto['symbol'].upper().strip(),
                'name': crypto['name'].strip(),
                'slug': crypto['slug'],
                'last_updated': crypto['last_updated'],
                'quote': crypto['quote']['USD']
            }
            
            # Validações
            if cleaned['crypto_id'] and cleaned['symbol'] and cleaned['name']:
                # Remove valores nulos ou inválidos
                quote = cleaned['quote']
                if quote.get('price') and quote.get('price') > 0:
                    cleaned_data.append(cleaned)
                    
        except (KeyError, TypeError) as e:
            logging.warning(f"Erro ao processar crypto {crypto.get('id')}: {str(e)}")
            continue
    
    logging.info(f"Dados limpos: {len(cleaned_data)} de {len(cryptocurrencies)} criptomoedas")
    
    # Armazena dados limpos no XCom
    context['ti'].xcom_push(key='cleaned_data', value=cleaned_data)
    context['ti'].xcom_push(key='landing_id', value=landing_id)
    
    return True


def load_to_staging(**context):
    """
    Carrega dados limpos na Staging Zone
    """
    ti = context['ti']
    cleaned_data = ti.xcom_pull(key='cleaned_data', task_ids='clean_transform_data')
    
    if not cleaned_data:
        logging.warning("Nenhum dado limpo para carregar")
        return
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Insere/atualiza criptomoedas
    for crypto in cleaned_data:
        # Upsert na tabela de criptomoedas
        upsert_crypto = """
        INSERT INTO staging_cryptocurrencies 
            (crypto_id, symbol, name, slug, last_updated)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (crypto_id) 
        DO UPDATE SET
            symbol = EXCLUDED.symbol,
            name = EXCLUDED.name,
            slug = EXCLUDED.slug,
            last_updated = EXCLUDED.last_updated,
            processed_at = CURRENT_TIMESTAMP;
        """
        
        postgres_hook.run(
            upsert_crypto,
            parameters=(
                crypto['crypto_id'],
                crypto['symbol'],
                crypto['name'],
                crypto['slug'],
                crypto['last_updated']
            )
        )
        
        # Insere métricas
        quote = crypto['quote']
        insert_metrics = """
        INSERT INTO staging_crypto_metrics 
            (crypto_id, price_usd, volume_24h, market_cap, 
             percent_change_1h, percent_change_24h, percent_change_7d,
             circulating_supply, total_supply, max_supply)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (crypto_id, recorded_at) DO NOTHING;
        """
        
        postgres_hook.run(
            insert_metrics,
            parameters=(
                crypto['crypto_id'],
                quote.get('price'),
                quote.get('volume_24h'),
                quote.get('market_cap'),
                quote.get('percent_change_1h'),
                quote.get('percent_change_24h'),
                quote.get('percent_change_7d'),
                quote.get('circulating_supply'),
                quote.get('total_supply'),
                quote.get('max_supply')
            )
        )
    
    logging.info(f"Carregados {len(cleaned_data)} registros na Staging Zone")


# Definição da DAG
with DAG(
    'pipeline_2_coinmarketcap_staging',
    default_args=default_args,
    description='Limpeza e transformação de dados para Staging',
    schedule='15 * * * *',  # Executa 15 minutos após a hora
    catchup=False,
    tags=['coinmarketcap', 'staging', 'transformation'],
) as dag:
    
    create_tables = PythonOperator(
        task_id='create_staging_tables',
        python_callable=create_staging_tables,
    )
    
    clean_transform = PythonOperator(
        task_id='clean_transform_data',
        python_callable=clean_and_transform_data,
    
    )
    
    load_staging = PythonOperator(
        task_id='load_to_staging',
        python_callable=load_to_staging,
    
    )
    
    create_tables >> clean_transform >> load_staging