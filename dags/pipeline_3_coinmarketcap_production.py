from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'seu_nome',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 21),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

def create_star_schema():
    """
    Cria esquema estrela na Production Zone
    
    Star Schema:
    - Fato: fact_crypto_performance (métricas de performance)
    - Dimensões: dim_cryptocurrency, dim_time, dim_market_category
    """
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Dimensão Criptomoeda
    dim_crypto = """
    CREATE TABLE IF NOT EXISTS dim_cryptocurrency (
        crypto_key SERIAL PRIMARY KEY,
        crypto_id INTEGER UNIQUE NOT NULL,
        symbol VARCHAR(20) NOT NULL,
        name VARCHAR(200) NOT NULL,
        slug VARCHAR(200),
        first_seen_date DATE,
        last_updated TIMESTAMP
    );
    """
    
    # Dimensão Tempo
    dim_time = """
    CREATE TABLE IF NOT EXISTS dim_time (
        time_key SERIAL PRIMARY KEY,
        full_date TIMESTAMP UNIQUE NOT NULL,
        date DATE,
        hour INTEGER,
        day INTEGER,
        month INTEGER,
        year INTEGER,
        day_of_week INTEGER,
        week_of_year INTEGER
    );
    """
    
    # Dimensão Categoria de Mercado
    dim_category = """
    CREATE TABLE IF NOT EXISTS dim_market_category (
        category_key SERIAL PRIMARY KEY,
        market_cap_range VARCHAR(50) UNIQUE,
        category_description TEXT
    );
    """
    
    # Tabela Fato - Performance de Criptomoedas
    fact_table = """
    CREATE TABLE IF NOT EXISTS fact_crypto_performance (
        fact_key SERIAL PRIMARY KEY,
        crypto_key INTEGER REFERENCES dim_cryptocurrency(crypto_key),
        time_key INTEGER REFERENCES dim_time(time_key),
        category_key INTEGER REFERENCES dim_market_category(category_key),
        
        -- Métricas de preço
        price_usd DECIMAL(20, 8),
        volume_24h DECIMAL(20, 2),
        market_cap DECIMAL(20, 2),
        
        -- Métricas de variação
        percent_change_1h DECIMAL(10, 4),
        percent_change_24h DECIMAL(10, 4),
        percent_change_7d DECIMAL(10, 4),
        
        -- Métricas de supply
        circulating_supply DECIMAL(20, 2),
        total_supply DECIMAL(20, 2),
        max_supply DECIMAL(20, 2),
        
        -- Métricas calculadas
        volume_to_market_cap_ratio DECIMAL(10, 6),
        
        recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(crypto_key, time_key)
    );
    """
    
    postgres_hook.run(dim_crypto)
    postgres_hook.run(dim_time)
    postgres_hook.run(dim_category)
    postgres_hook.run(fact_table)
    
    # Popula dimensão de categorias
    populate_categories = """
    INSERT INTO dim_market_category (market_cap_range, category_description)
    VALUES 
        ('Large Cap', 'Market Cap > $10 Billion'),
        ('Mid Cap', 'Market Cap $1B - $10B'),
        ('Small Cap', 'Market Cap $100M - $1B'),
        ('Micro Cap', 'Market Cap < $100M')
    ON CONFLICT (market_cap_range) DO NOTHING;
    """
    postgres_hook.run(populate_categories)
    
    logging.info("Star Schema criado com sucesso")


def populate_dimensions(**context):
    """
    Popula tabelas de dimensão a partir da Staging Zone
    """
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Popula dim_cryptocurrency
    populate_dim_crypto = """
    INSERT INTO dim_cryptocurrency (crypto_id, symbol, name, slug, last_updated)
    SELECT 
        crypto_id,
        symbol,
        name,
        slug,
        last_updated
    FROM staging_cryptocurrencies
    ON CONFLICT (crypto_id) 
    DO UPDATE SET
        symbol = EXCLUDED.symbol,
        name = EXCLUDED.name,
        slug = EXCLUDED.slug,
        last_updated = EXCLUDED.last_updated;
    """
    postgres_hook.run(populate_dim_crypto)
    
    # Popula dim_time para registros que ainda não existem
    populate_dim_time = """
    INSERT INTO dim_time (full_date, date, hour, day, month, year, day_of_week, week_of_year)
    SELECT DISTINCT
        recorded_at as full_date,
        DATE(recorded_at) as date,
        EXTRACT(HOUR FROM recorded_at) as hour,
        EXTRACT(DAY FROM recorded_at) as day,
        EXTRACT(MONTH FROM recorded_at) as month,
        EXTRACT(YEAR FROM recorded_at) as year,
        EXTRACT(DOW FROM recorded_at) as day_of_week,
        EXTRACT(WEEK FROM recorded_at) as week_of_year
    FROM staging_crypto_metrics
    WHERE recorded_at >= NOW() - INTERVAL '2 hours'
    ON CONFLICT (full_date) DO NOTHING;
    """
    postgres_hook.run(populate_dim_time)
    
    logging.info("Dimensões populadas com sucesso")


def populate_fact_table(**context):
    """
    Popula tabela fato com métricas da Staging Zone
    """
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    populate_fact = """
    INSERT INTO fact_crypto_performance (
        crypto_key, time_key, category_key,
        price_usd, volume_24h, market_cap,
        percent_change_1h, percent_change_24h, percent_change_7d,
        circulating_supply, total_supply, max_supply,
        volume_to_market_cap_ratio
    )
    SELECT 
        dc.crypto_key,
        dt.time_key,
        dmc.category_key,
        sm.price_usd,
        sm.volume_24h,
        sm.market_cap,
        sm.percent_change_1h,
        sm.percent_change_24h,
        sm.percent_change_7d,
        sm.circulating_supply,
        sm.total_supply,
        sm.max_supply,
        CASE 
            WHEN sm.market_cap > 0 THEN sm.volume_24h / sm.market_cap
            ELSE 0
        END as volume_to_market_cap_ratio
    FROM staging_crypto_metrics sm
    JOIN staging_cryptocurrencies sc ON sm.crypto_id = sc.crypto_id
    JOIN dim_cryptocurrency dc ON sc.crypto_id = dc.crypto_id
    JOIN dim_time dt ON sm.recorded_at = dt.full_date
    JOIN dim_market_category dmc ON 
        CASE 
            WHEN sm.market_cap >= 10000000000 THEN 'Large Cap'
            WHEN sm.market_cap >= 1000000000 THEN 'Mid Cap'
            WHEN sm.market_cap >= 100000000 THEN 'Small Cap'
            ELSE 'Micro Cap'
        END = dmc.market_cap_range
    WHERE sm.recorded_at >= NOW() - INTERVAL '2 hours'
    ON CONFLICT (crypto_key, time_key) DO NOTHING;
    """
    
    postgres_hook.run(populate_fact)
    logging.info("Tabela fato populada com sucesso")


def create_analytical_views():
    """
    Cria views para análise de dados (Data Marts)
    """
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # View 1: Top performers por período
    view_top_performers = """
    CREATE OR REPLACE VIEW vw_top_performers_24h AS
    SELECT 
        dc.name,
        dc.symbol,
        f.price_usd,
        f.market_cap,
        f.volume_24h,
        f.percent_change_24h,
        dmc.market_cap_range,
        dt.date
    FROM fact_crypto_performance f
    JOIN dim_cryptocurrency dc ON f.crypto_key = dc.crypto_key
    JOIN dim_time dt ON f.time_key = dt.time_key
    JOIN dim_market_category dmc ON f.category_key = dmc.category_key
    WHERE dt.date >= CURRENT_DATE - INTERVAL '7 days'
    ORDER BY f.percent_change_24h DESC
    LIMIT 20;
    """
    
    # View 2: Análise de market cap por categoria
    view_market_analysis = """
    CREATE OR REPLACE VIEW vw_market_cap_analysis AS
    SELECT 
        dmc.market_cap_range,
        dmc.category_description,
        COUNT(DISTINCT dc.crypto_id) as total_cryptos,
        SUM(f.market_cap) as total_market_cap,
        AVG(f.market_cap) as avg_market_cap,
        AVG(f.volume_to_market_cap_ratio) as avg_liquidity_ratio
    FROM fact_crypto_performance f
    JOIN dim_cryptocurrency dc ON f.crypto_key = dc.crypto_key
    JOIN dim_market_category dmc ON f.category_key = dmc.category_key
    JOIN dim_time dt ON f.time_key = dt.time_key
    WHERE dt.date = CURRENT_DATE
    GROUP BY dmc.market_cap_range, dmc.category_description
    ORDER BY total_market_cap DESC;
    """
    
    # View 3: Tendências semanais
    view_weekly_trends = """
    CREATE OR REPLACE VIEW vw_weekly_trends AS
    SELECT 
        dc.name,
        dc.symbol,
        dt.week_of_year,
        AVG(f.price_usd) as avg_price,
        AVG(f.volume_24h) as avg_volume,
        AVG(f.percent_change_24h) as avg_daily_change,
        STDDEV(f.price_usd) as price_volatility
    FROM fact_crypto_performance f
    JOIN dim_cryptocurrency dc ON f.crypto_key = dc.crypto_key
    JOIN dim_time dt ON f.time_key = dt.time_key
    WHERE dt.date >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY dc.name, dc.symbol, dt.week_of_year
    ORDER BY dt.week_of_year DESC, avg_volume DESC;
    """
    
    postgres_hook.run(view_top_performers)
    postgres_hook.run(view_market_analysis)
    postgres_hook.run(view_weekly_trends)
    
    logging.info("Views analíticas criadas com sucesso")


# Definição da DAG
with DAG(
    'pipeline_3_coinmarketcap_production',
    default_args=default_args,
    description='Criação de Star Schema e Data Marts na Production Zone',
    schedule='30 * * * *',  # Executa 30 minutos após a hora
    catchup=False,
    tags=['coinmarketcap', 'production', 'star-schema'],
) as dag:
    
    create_schema = PythonOperator(
        task_id='create_star_schema',
        python_callable=create_star_schema,
    )
    
    populate_dims = PythonOperator(
        task_id='populate_dimensions',
        python_callable=populate_dimensions,
       
    )
    
    populate_fact = PythonOperator(
        task_id='populate_fact_table',
        python_callable=populate_fact_table,
        
    )
    
    create_views = PythonOperator(
        task_id='create_analytical_views',
        python_callable=create_analytical_views,
    )
    
    create_schema >> populate_dims >> populate_fact >> create_views