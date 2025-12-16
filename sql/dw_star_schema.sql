-- Star schema for data loaded from MongoDB into Postgres.
-- Intended target: the dedicated project Postgres ("postgres-data" service).

CREATE SCHEMA IF NOT EXISTS dw;

-- ---------- Dimensions ----------

CREATE TABLE IF NOT EXISTS dw.dim_time (
  time_key BIGSERIAL PRIMARY KEY,
  ts_utc TIMESTAMPTZ NOT NULL UNIQUE,
  date DATE NOT NULL,
  year SMALLINT NOT NULL,
  quarter SMALLINT NOT NULL,
  month SMALLINT NOT NULL,
  day SMALLINT NOT NULL,
  hour SMALLINT NOT NULL,
  minute SMALLINT NOT NULL,
  day_of_week SMALLINT NOT NULL,  -- Postgres-style: Sunday=0 .. Saturday=6
  week_of_year SMALLINT NOT NULL,
  is_weekend BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS dw.dim_exchange (
  exchange_key SERIAL PRIMARY KEY,
  exchange_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dw.dim_asset (
  asset_key SERIAL PRIMARY KEY,
  symbol TEXT NOT NULL UNIQUE,
  base_asset TEXT,
  quote_asset TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dw.dim_interval (
  interval_key SERIAL PRIMARY KEY,
  interval TEXT NOT NULL UNIQUE,
  interval_seconds INTEGER
);

CREATE TABLE IF NOT EXISTS dw.dim_chain (
  chain_key SERIAL PRIMARY KEY,
  chain_name TEXT NOT NULL UNIQUE,
  chain_id INTEGER UNIQUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ---------- Facts ----------

CREATE TABLE IF NOT EXISTS dw.fact_binance_candle (
  candle_key BIGSERIAL PRIMARY KEY,
  exchange_key INTEGER NOT NULL REFERENCES dw.dim_exchange(exchange_key),
  asset_key INTEGER NOT NULL REFERENCES dw.dim_asset(asset_key),
  interval_key INTEGER NOT NULL REFERENCES dw.dim_interval(interval_key),
  open_time_key BIGINT NOT NULL REFERENCES dw.dim_time(time_key),
  close_time_key BIGINT NOT NULL REFERENCES dw.dim_time(time_key),
  trading_day DATE,

  open NUMERIC(20, 8),
  high NUMERIC(20, 8),
  low NUMERIC(20, 8),
  close NUMERIC(20, 8),
  volume NUMERIC(28, 8),
  quote_asset_volume NUMERIC(28, 8),
  number_of_trades INTEGER,
  taker_buy_base_volume NUMERIC(28, 8),
  taker_buy_quote_volume NUMERIC(28, 8),

  source_file TEXT,
  ingested_at TIMESTAMPTZ,

  UNIQUE (exchange_key, asset_key, interval_key, open_time_key)
);

CREATE INDEX IF NOT EXISTS ix_fact_binance_candle_open_time_key
  ON dw.fact_binance_candle (open_time_key);

CREATE TABLE IF NOT EXISTS dw.fact_ethereum_block (
  block_key BIGSERIAL PRIMARY KEY,
  chain_key INTEGER NOT NULL REFERENCES dw.dim_chain(chain_key),
  block_time_key BIGINT NOT NULL REFERENCES dw.dim_time(time_key),
  block_number BIGINT,
  block_hash TEXT,
  tx_count INTEGER,
  miner TEXT,
  gas_used BIGINT,
  gas_limit BIGINT,
  size_bytes INTEGER,
  base_fee_per_gas BIGINT,
  blob_gas_used BIGINT,
  excess_blob_gas BIGINT,
  ingested_at TIMESTAMPTZ,
  ingestion_ds DATE,

  UNIQUE (chain_key, block_number),
  UNIQUE (block_hash)
);

CREATE INDEX IF NOT EXISTS ix_fact_ethereum_block_time_key
  ON dw.fact_ethereum_block (block_time_key);

