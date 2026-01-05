"""
Pipeline 2: Staging Zone
=========================
Loads data from Landing Zone, cleans and validates it, and stores in Staging Zone.

Data flow (granular steps):
1. Fetch: Reads raw data from Landing Zone
2. Clean: Cleans, validates and transforms data
3. Load: Writes cleaned data to Staging Zone

Data sources:
- MongoDB (binance_candles) → Postgres staging.binance_candles
- MongoDB (etherscan_blocks) → Postgres staging.ethereum_blocks
- Postgres (landing_coinmarketcap_raw) → Postgres staging_cryptocurrencies + staging_crypto_metrics
"""
from __future__ import annotations

import json
from datetime import date
from typing import Any, Dict, List, Optional

import pendulum
from pymongo import MongoClient

try:
    from airflow.sdk.dag import dag
    from airflow.sdk.task import task
except ModuleNotFoundError:
    from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable

# MongoDB defaults (Landing Zone)
DEFAULT_MONGO_URI = "mongodb://root:example@mongodb:27017"
DEFAULT_MONGO_DB = "blockchain"
DEFAULT_BINANCE_COLLECTION = "binance_candles"
DEFAULT_ETHERSCAN_COLLECTION = "etherscan_blocks"

# Postgres defaults - Staging DB (postgres-data)
DEFAULT_STAGING_PG_HOST = "postgres-data"
DEFAULT_STAGING_PG_PORT = 5432
DEFAULT_STAGING_PG_DB = "datadb"
DEFAULT_STAGING_PG_USER = "datauser"
DEFAULT_STAGING_PG_PASSWORD = "datapass"

# Postgres defaults - Landing DB (Airflow Postgres, for CoinMarketCap)
DEFAULT_LANDING_PG_HOST = "postgres"
DEFAULT_LANDING_PG_PORT = 5432
DEFAULT_LANDING_PG_DB = "airflow"
DEFAULT_LANDING_PG_USER = "airflow"
DEFAULT_LANDING_PG_PASSWORD = "airflow"

STAGING_SCHEMA = "staging"
DEFAULT_BATCH_SIZE = 250  # Reduced from 1000 to lower memory usage

CHECKPOINT_BINANCE_VAR = "STAGING_LAST_BINANCE_INGESTED_AT"
CHECKPOINT_ETHERSCAN_VAR = "STAGING_LAST_ETHERSCAN_INGESTED_AT"
CHECKPOINT_CMC_VAR = "STAGING_LAST_CMC_ID"


def _pg_connect_staging():
    """PostgreSQL connection for Staging Zone (postgres-data)"""
    try:
        import psycopg2  # type: ignore
        host = Variable.get("STAGING_PG_HOST", default_var=DEFAULT_STAGING_PG_HOST)
        port = int(Variable.get("STAGING_PG_PORT", default_var=str(DEFAULT_STAGING_PG_PORT)))
        db = Variable.get("STAGING_PG_DB", default_var=DEFAULT_STAGING_PG_DB)
        user = Variable.get("STAGING_PG_USER", default_var=DEFAULT_STAGING_PG_USER)
        password = Variable.get("STAGING_PG_PASSWORD", default_var=DEFAULT_STAGING_PG_PASSWORD)
        return psycopg2.connect(host=host, port=port, dbname=db, user=user, password=password)
    except ModuleNotFoundError:
        import psycopg  # type: ignore
        host = Variable.get("STAGING_PG_HOST", default_var=DEFAULT_STAGING_PG_HOST)
        port = int(Variable.get("STAGING_PG_PORT", default_var=str(DEFAULT_STAGING_PG_PORT)))
        db = Variable.get("STAGING_PG_DB", default_var=DEFAULT_STAGING_PG_DB)
        user = Variable.get("STAGING_PG_USER", default_var=DEFAULT_STAGING_PG_USER)
        password = Variable.get("STAGING_PG_PASSWORD", default_var=DEFAULT_STAGING_PG_PASSWORD)
        return psycopg.connect(host=host, port=port, dbname=db, user=user, password=password)


def _pg_connect_landing():
    """PostgreSQL connection for Landing Zone (Airflow Postgres)"""
    try:
        import psycopg2  # type: ignore
        host = Variable.get("LANDING_PG_HOST", default_var=DEFAULT_LANDING_PG_HOST)
        port = int(Variable.get("LANDING_PG_PORT", default_var=str(DEFAULT_LANDING_PG_PORT)))
        db = Variable.get("LANDING_PG_DB", default_var=DEFAULT_LANDING_PG_DB)
        user = Variable.get("LANDING_PG_USER", default_var=DEFAULT_LANDING_PG_USER)
        password = Variable.get("LANDING_PG_PASSWORD", default_var=DEFAULT_LANDING_PG_PASSWORD)
        return psycopg2.connect(host=host, port=port, dbname=db, user=user, password=password)
    except ModuleNotFoundError:
        import psycopg  # type: ignore
        host = Variable.get("LANDING_PG_HOST", default_var=DEFAULT_LANDING_PG_HOST)
        port = int(Variable.get("LANDING_PG_PORT", default_var=str(DEFAULT_LANDING_PG_PORT)))
        db = Variable.get("LANDING_PG_DB", default_var=DEFAULT_LANDING_PG_DB)
        user = Variable.get("LANDING_PG_USER", default_var=DEFAULT_LANDING_PG_USER)
        password = Variable.get("LANDING_PG_PASSWORD", default_var=DEFAULT_LANDING_PG_PASSWORD)
        return psycopg.connect(host=host, port=port, dbname=db, user=user, password=password)


def _parse_hex_int(value: Any) -> Optional[int]:
    """Parse hex or decimal integer values"""
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        s = value.strip().lower()
        try:
            return int(s, 16) if s.startswith("0x") else int(s)
        except ValueError:
            return None
    return None


@dag(
    start_date=pendulum.datetime(2025, 10, 1, tz="Europe/Paris"),
    schedule="*/20 * * * *",  # Every 20 minutes
    catchup=False,
    max_active_runs=1,
    tags=["pipeline-2", "staging", "transformation"],
    default_args=dict(retries=2, retry_delay=pendulum.duration(minutes=2)),
    description="Pipeline 2: Loads and cleans data from Landing → Staging Zone (granular: Fetch → Clean → Load).",
)
def pipeline_2_staging():

    # ========== SCHEMA CREATION ==========

    @task
    def create_staging_schema() -> None:
        """Creates Staging schema and all tables"""
        conn = _pg_connect_staging()
        try:
            ddl = f"""
            CREATE SCHEMA IF NOT EXISTS {STAGING_SCHEMA};

            -- Binance Candles
            CREATE TABLE IF NOT EXISTS {STAGING_SCHEMA}.binance_candles (
              staging_id BIGSERIAL PRIMARY KEY,
              symbol TEXT NOT NULL,
              interval TEXT NOT NULL,
              open_time_ms BIGINT NOT NULL,
              close_time_ms BIGINT NOT NULL,
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
              staged_at TIMESTAMPTZ DEFAULT NOW(),
              UNIQUE (symbol, interval, open_time_ms)
            );
            CREATE INDEX IF NOT EXISTS ix_staging_binance_staged_at ON {STAGING_SCHEMA}.binance_candles (staged_at);

            -- Ethereum Blocks
            CREATE TABLE IF NOT EXISTS {STAGING_SCHEMA}.ethereum_blocks (
              staging_id BIGSERIAL PRIMARY KEY,
              block_hash TEXT UNIQUE NOT NULL,
              block_number BIGINT UNIQUE NOT NULL,
              timestamp_unix BIGINT NOT NULL,
              tx_count INTEGER,
              miner TEXT,
              gas_used BIGINT,
              gas_limit BIGINT,
              size_bytes INTEGER,
              base_fee_per_gas BIGINT,
              blob_gas_used BIGINT,
              excess_blob_gas BIGINT,
              ingestion_ds DATE,
              ingested_at TIMESTAMPTZ,
              staged_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS ix_staging_ethereum_staged_at ON {STAGING_SCHEMA}.ethereum_blocks (staged_at);

            -- CoinMarketCap Cryptocurrencies
            CREATE TABLE IF NOT EXISTS {STAGING_SCHEMA}.cryptocurrencies (
              crypto_id INTEGER PRIMARY KEY,
              symbol VARCHAR(20) NOT NULL,
              name VARCHAR(200) NOT NULL,
              slug VARCHAR(200),
              last_updated TIMESTAMP,
              processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            -- CoinMarketCap Crypto Metrics
            CREATE TABLE IF NOT EXISTS {STAGING_SCHEMA}.crypto_metrics (
              id SERIAL PRIMARY KEY,
              crypto_id INTEGER REFERENCES {STAGING_SCHEMA}.cryptocurrencies(crypto_id),
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
            with conn.cursor() as cur:
                cur.execute(ddl)
            conn.commit()
            print(f"✓ Staging schema '{STAGING_SCHEMA}' successfully created")
        finally:
            conn.close()

    # ========== BINANCE PIPELINE ==========

    @task
    def fetch_binance_from_landing() -> List[Dict[str, Any]]:
        """Step 1: Reads Binance raw data from MongoDB Landing Zone"""
        checkpoint = Variable.get(CHECKPOINT_BINANCE_VAR, default_var=None)
        query: Dict[str, Any] = {}
        if checkpoint:
            query = {"ingested_at": {"$gt": checkpoint}}

        mongo_uri = Variable.get("MONGO_URI", default_var=DEFAULT_MONGO_URI)
        mongo_db = Variable.get("MONGO_DB", default_var=DEFAULT_MONGO_DB)
        collection_name = Variable.get("BINANCE_MONGO_COLLECTION", default_var=DEFAULT_BINANCE_COLLECTION)

        client = MongoClient(mongo_uri)
        coll = client[mongo_db][collection_name]

        try:
            cursor = coll.find(query).sort("ingested_at", 1)
            raw_data = list(cursor)

            if not raw_data:
                raise AirflowSkipException("No new Binance raw data in Landing Zone")

            print(f"✓ Binance: {len(raw_data)} raw data documents read from MongoDB")
            return raw_data
        finally:
            client.close()

    @task
    def clean_binance_data(raw_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Step 2: Cleans and validates Binance data"""
        stats = {"input": len(raw_data), "valid": 0, "invalid": 0}
        cleaned_data: List[Dict[str, Any]] = []
        last_ingested_at: Optional[str] = None

        for doc in raw_data:
            try:
                # Pflichtfeld-Validierung
                symbol = (doc.get("symbol") or "").strip().upper()
                interval = (doc.get("interval") or "").strip()
                open_time_ms = doc.get("open_time_ms")
                close_time_ms = doc.get("close_time_ms")
                close_price = doc.get("close")

                if not symbol or not interval or open_time_ms is None or close_time_ms is None:
                    stats["invalid"] += 1
                    continue

                # Price validation (must be > 0)
                if close_price is not None and float(close_price) <= 0:
                    stats["invalid"] += 1
                    continue

                # Parse trading day
                trading_day: Optional[date] = None
                trading_day_raw = doc.get("trading_day")
                if isinstance(trading_day_raw, str):
                    try:
                        trading_day = date.fromisoformat(trading_day_raw)
                    except ValueError:
                        pass

                # Cleaned data
                cleaned = {
                    "symbol": symbol,  # Normalized: UPPERCASE
                    "interval": interval,
                    "open_time_ms": int(open_time_ms),
                    "close_time_ms": int(close_time_ms),
                    "trading_day": trading_day,
                    "open": doc.get("open"),
                    "high": doc.get("high"),
                    "low": doc.get("low"),
                    "close": doc.get("close"),
                    "volume": doc.get("volume"),
                    "quote_asset_volume": doc.get("quote_asset_volume"),
                    "number_of_trades": doc.get("number_of_trades"),
                    "taker_buy_base_volume": doc.get("taker_buy_base_volume"),
                    "taker_buy_quote_volume": doc.get("taker_buy_quote_volume"),
                    "source_file": doc.get("source_file"),
                    "ingested_at": doc.get("ingested_at"),
                }

                cleaned_data.append(cleaned)
                stats["valid"] += 1

                # Checkpoint tracking
                ia = doc.get("ingested_at")
                if isinstance(ia, str):
                    last_ingested_at = ia

            except (KeyError, TypeError, ValueError) as e:
                print(f"⚠ Cleaning error: {e}")
                stats["invalid"] += 1
                continue

        if not cleaned_data:
            raise AirflowSkipException("No valid Binance data after cleaning")

        print(f"✓ Binance: {stats['valid']} valid, {stats['invalid']} invalid records")
        return {
            "cleaned_data": cleaned_data,
            "last_ingested_at": last_ingested_at,
            "stats": stats
        }

    @task
    def load_binance_to_staging(cleaned_result: Dict[str, Any]) -> Dict[str, Any]:
        """Step 3: Loads cleaned Binance data into Staging Zone"""
        cleaned_data = cleaned_result["cleaned_data"]
        last_ingested_at = cleaned_result["last_ingested_at"]
        checkpoint = Variable.get(CHECKPOINT_BINANCE_VAR, default_var=None)

        conn = _pg_connect_staging()
        batch_size = int(Variable.get("STAGING_BATCH_SIZE", default_var=str(DEFAULT_BATCH_SIZE)))

        try:
            inserted = 0
            for i in range(0, len(cleaned_data), batch_size):
                batch = cleaned_data[i:i + batch_size]

                sql = f"""
                INSERT INTO {STAGING_SCHEMA}.binance_candles (
                  symbol, interval, open_time_ms, close_time_ms, trading_day,
                  open, high, low, close, volume, quote_asset_volume,
                  number_of_trades, taker_buy_base_volume, taker_buy_quote_volume,
                  source_file, ingested_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, interval, open_time_ms)
                DO UPDATE SET
                  close_time_ms = EXCLUDED.close_time_ms,
                  trading_day = COALESCE(EXCLUDED.trading_day, {STAGING_SCHEMA}.binance_candles.trading_day),
                  open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low, close = EXCLUDED.close,
                  volume = EXCLUDED.volume, quote_asset_volume = EXCLUDED.quote_asset_volume,
                  number_of_trades = EXCLUDED.number_of_trades,
                  taker_buy_base_volume = EXCLUDED.taker_buy_base_volume,
                  taker_buy_quote_volume = EXCLUDED.taker_buy_quote_volume,
                  source_file = EXCLUDED.source_file, ingested_at = EXCLUDED.ingested_at,
                  staged_at = NOW()
                """

                rows = [(d["symbol"], d["interval"], d["open_time_ms"], d["close_time_ms"], d["trading_day"],
                        d["open"], d["high"], d["low"], d["close"], d["volume"], d["quote_asset_volume"],
                        d["number_of_trades"], d["taker_buy_base_volume"], d["taker_buy_quote_volume"],
                        d["source_file"], d["ingested_at"]) for d in batch]

                with conn.cursor() as cur:
                    cur.executemany(sql, rows)
                conn.commit()
                inserted += len(rows)

            # Save checkpoint
            if last_ingested_at and last_ingested_at != checkpoint:
                Variable.set(CHECKPOINT_BINANCE_VAR, last_ingested_at)

            print(f"✓ Binance: {inserted} records loaded into Staging")
            return {"inserted": inserted, "checkpoint": last_ingested_at}
        finally:
            conn.close()

    # ========== ETHEREUM PIPELINE ==========

    @task
    def fetch_ethereum_from_landing() -> List[Dict[str, Any]]:
        """Step 1: Reads Ethereum raw data from MongoDB Landing Zone"""
        checkpoint = Variable.get(CHECKPOINT_ETHERSCAN_VAR, default_var=None)
        query: Dict[str, Any] = {}
        if checkpoint:
            query = {"ingested_at": {"$gt": checkpoint}}

        mongo_uri = Variable.get("MONGO_URI", default_var=DEFAULT_MONGO_URI)
        mongo_db = Variable.get("MONGO_DB", default_var=DEFAULT_MONGO_DB)
        collection_name = Variable.get("ETHERSCAN_MONGO_COLLECTION", default_var=DEFAULT_ETHERSCAN_COLLECTION)

        client = MongoClient(mongo_uri)
        coll = client[mongo_db][collection_name]

        try:
            cursor = coll.find(query).sort("ingested_at", 1)
            raw_data = list(cursor)

            if not raw_data:
                raise AirflowSkipException("No new Ethereum raw data in Landing Zone")

            print(f"✓ Ethereum: {len(raw_data)} raw data documents read from MongoDB")
            return raw_data
        finally:
            client.close()

    @task
    def clean_ethereum_data(raw_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Step 2: Cleans and validates Ethereum data"""
        stats = {"input": len(raw_data), "valid": 0, "invalid": 0}
        cleaned_data: List[Dict[str, Any]] = []
        last_ingested_at: Optional[str] = None

        for doc in raw_data:
            try:
                # Pflichtfeld-Validierung
                block_hash = doc.get("hash")
                block_number = doc.get("number")
                timestamp_unix = doc.get("timestamp_unix")

                if not block_hash or block_number is None or timestamp_unix is None:
                    stats["invalid"] += 1
                    continue

                # Raw block data for hex conversion
                raw = doc.get("raw") if isinstance(doc.get("raw"), dict) else {}

                # Parse ingestion date
                ingestion_ds: Optional[date] = None
                ingestion_ds_raw = doc.get("ingestion_ds")
                if isinstance(ingestion_ds_raw, str):
                    try:
                        ingestion_ds = date.fromisoformat(ingestion_ds_raw)
                    except ValueError:
                        pass

                # Cleaned data (Hex → Integer conversion!)
                cleaned = {
                    "block_hash": str(block_hash),
                    "block_number": int(block_number),
                    "timestamp_unix": int(timestamp_unix),
                    "tx_count": doc.get("tx_count"),
                    "miner": raw.get("miner"),
                    "gas_used": _parse_hex_int(raw.get("gasUsed")),  # Hex → Int
                    "gas_limit": _parse_hex_int(raw.get("gasLimit")),  # Hex → Int
                    "size_bytes": _parse_hex_int(raw.get("size")),  # Hex → Int
                    "base_fee_per_gas": _parse_hex_int(raw.get("baseFeePerGas")),  # Hex → Int
                    "blob_gas_used": _parse_hex_int(raw.get("blobGasUsed")),  # Hex → Int
                    "excess_blob_gas": _parse_hex_int(raw.get("excessBlobGas")),  # Hex → Int
                    "ingestion_ds": ingestion_ds,
                    "ingested_at": doc.get("ingested_at"),
                }

                cleaned_data.append(cleaned)
                stats["valid"] += 1

                # Checkpoint tracking
                ia = doc.get("ingested_at")
                if isinstance(ia, str):
                    last_ingested_at = ia

            except (KeyError, TypeError, ValueError) as e:
                print(f"⚠ Cleaning error: {e}")
                stats["invalid"] += 1
                continue

        if not cleaned_data:
            raise AirflowSkipException("No valid Ethereum data after cleaning")

        print(f"✓ Ethereum: {stats['valid']} valid, {stats['invalid']} invalid records")
        return {
            "cleaned_data": cleaned_data,
            "last_ingested_at": last_ingested_at,
            "stats": stats
        }

    @task
    def load_ethereum_to_staging(cleaned_result: Dict[str, Any]) -> Dict[str, Any]:
        """Step 3: Loads cleaned Ethereum data into Staging Zone"""
        cleaned_data = cleaned_result["cleaned_data"]
        last_ingested_at = cleaned_result["last_ingested_at"]
        checkpoint = Variable.get(CHECKPOINT_ETHERSCAN_VAR, default_var=None)

        conn = _pg_connect_staging()
        batch_size = int(Variable.get("STAGING_BATCH_SIZE", default_var=str(DEFAULT_BATCH_SIZE)))

        try:
            inserted = 0
            for i in range(0, len(cleaned_data), batch_size):
                batch = cleaned_data[i:i + batch_size]

                sql = f"""
                INSERT INTO {STAGING_SCHEMA}.ethereum_blocks (
                  block_hash, block_number, timestamp_unix, tx_count,
                  miner, gas_used, gas_limit, size_bytes,
                  base_fee_per_gas, blob_gas_used, excess_blob_gas,
                  ingestion_ds, ingested_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (block_hash)
                DO UPDATE SET
                  block_number = EXCLUDED.block_number, timestamp_unix = EXCLUDED.timestamp_unix,
                  tx_count = EXCLUDED.tx_count, miner = EXCLUDED.miner,
                  gas_used = EXCLUDED.gas_used, gas_limit = EXCLUDED.gas_limit,
                  size_bytes = EXCLUDED.size_bytes, base_fee_per_gas = EXCLUDED.base_fee_per_gas,
                  blob_gas_used = EXCLUDED.blob_gas_used, excess_blob_gas = EXCLUDED.excess_blob_gas,
                  ingestion_ds = COALESCE(EXCLUDED.ingestion_ds, {STAGING_SCHEMA}.ethereum_blocks.ingestion_ds),
                  ingested_at = EXCLUDED.ingested_at, staged_at = NOW()
                """

                rows = [(d["block_hash"], d["block_number"], d["timestamp_unix"], d["tx_count"],
                        d["miner"], d["gas_used"], d["gas_limit"], d["size_bytes"],
                        d["base_fee_per_gas"], d["blob_gas_used"], d["excess_blob_gas"],
                        d["ingestion_ds"], d["ingested_at"]) for d in batch]

                with conn.cursor() as cur:
                    cur.executemany(sql, rows)
                conn.commit()
                inserted += len(rows)

            # Save checkpoint
            if last_ingested_at and last_ingested_at != checkpoint:
                Variable.set(CHECKPOINT_ETHERSCAN_VAR, last_ingested_at)

            print(f"✓ Ethereum: {inserted} records loaded into Staging")
            return {"inserted": inserted, "checkpoint": last_ingested_at}
        finally:
            conn.close()

    # ========== COINMARKETCAP PIPELINE ==========

    @task
    def fetch_coinmarketcap_from_landing() -> List[Dict[str, Any]]:
        """Step 1: Reads CoinMarketCap raw data from Postgres Landing Zone"""
        checkpoint_id = int(Variable.get(CHECKPOINT_CMC_VAR, default_var="0"))

        landing_conn = _pg_connect_landing()
        try:
            with landing_conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, api_response, ingestion_timestamp
                    FROM landing_coinmarketcap_raw
                    WHERE id > %s
                    ORDER BY id
                    """,
                    (checkpoint_id,)
                )
                rows = cur.fetchall()

            if not rows:
                raise AirflowSkipException("No new CoinMarketCap raw data in Landing Zone")

            print(f"✓ CoinMarketCap: {len(rows)} raw data entries read from Postgres Landing")
            return [{"id": r[0], "api_response": r[1], "timestamp": r[2]} for r in rows]
        finally:
            landing_conn.close()

    @task
    def clean_coinmarketcap_data(raw_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Step 2: Cleans and validates CoinMarketCap data"""
        stats = {"input": len(raw_data), "valid": 0, "invalid": 0}
        cleaned_cryptos: List[Dict[str, Any]] = []
        last_id = int(Variable.get(CHECKPOINT_CMC_VAR, default_var="0"))

        for entry in raw_data:
            api_response = entry["api_response"]
            data = json.loads(api_response) if isinstance(api_response, str) else api_response
            cryptocurrencies = data.get("data", [])

            for crypto in cryptocurrencies:
                try:
                    # Required field validation
                    crypto_id = crypto.get("id")
                    symbol = crypto.get("symbol")
                    name = crypto.get("name")
                    quote = crypto.get("quote", {}).get("USD", {})
                    price = quote.get("price")

                    if not crypto_id or not symbol or not name:
                        stats["invalid"] += 1
                        continue

                    # Price validation
                    if not price or price <= 0:
                        stats["invalid"] += 1
                        continue

                    # Cleaned data
                    cleaned = {
                        "crypto_id": int(crypto_id),
                        "symbol": symbol.upper().strip(),  # Normalized: UPPERCASE
                        "name": name.strip(),
                        "slug": crypto.get("slug"),
                        "last_updated": crypto.get("last_updated"),
                        "price_usd": price,
                        "volume_24h": quote.get("volume_24h"),
                        "market_cap": quote.get("market_cap"),
                        "percent_change_1h": quote.get("percent_change_1h"),
                        "percent_change_24h": quote.get("percent_change_24h"),
                        "percent_change_7d": quote.get("percent_change_7d"),
                        "circulating_supply": quote.get("circulating_supply"),
                        "total_supply": quote.get("total_supply"),
                        "max_supply": quote.get("max_supply"),
                    }

                    cleaned_cryptos.append(cleaned)
                    stats["valid"] += 1

                except (KeyError, TypeError, ValueError) as e:
                    print(f"⚠ Cleaning error: {e}")
                    stats["invalid"] += 1
                    continue

            last_id = entry["id"]

        if not cleaned_cryptos:
            raise AirflowSkipException("No valid CoinMarketCap data after cleaning")

        print(f"✓ CoinMarketCap: {stats['valid']} valid, {stats['invalid']} invalid records")
        return {
            "cleaned_data": cleaned_cryptos,
            "last_id": last_id,
            "stats": stats
        }

    @task
    def load_coinmarketcap_to_staging(cleaned_result: Dict[str, Any]) -> Dict[str, Any]:
        """Step 3: Loads cleaned CoinMarketCap data into Staging Zone"""
        cleaned_data = cleaned_result["cleaned_data"]
        last_id = cleaned_result["last_id"]
        checkpoint_id = int(Variable.get(CHECKPOINT_CMC_VAR, default_var="0"))

        conn = _pg_connect_staging()

        try:
            cryptos_inserted = 0
            metrics_inserted = 0

            for crypto in cleaned_data:
                # Insert Cryptocurrency
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        INSERT INTO {STAGING_SCHEMA}.cryptocurrencies
                            (crypto_id, symbol, name, slug, last_updated)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (crypto_id)
                        DO UPDATE SET
                            symbol = EXCLUDED.symbol, name = EXCLUDED.name,
                            slug = EXCLUDED.slug, last_updated = EXCLUDED.last_updated,
                            processed_at = CURRENT_TIMESTAMP
                        """,
                        (crypto["crypto_id"], crypto["symbol"], crypto["name"],
                         crypto["slug"], crypto["last_updated"])
                    )
                cryptos_inserted += 1

                # Insert Metrics
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        INSERT INTO {STAGING_SCHEMA}.crypto_metrics
                            (crypto_id, price_usd, volume_24h, market_cap,
                             percent_change_1h, percent_change_24h, percent_change_7d,
                             circulating_supply, total_supply, max_supply)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (crypto_id, recorded_at) DO NOTHING
                        """,
                        (crypto["crypto_id"], crypto["price_usd"], crypto["volume_24h"],
                         crypto["market_cap"], crypto["percent_change_1h"],
                         crypto["percent_change_24h"], crypto["percent_change_7d"],
                         crypto["circulating_supply"], crypto["total_supply"],
                         crypto["max_supply"])
                    )
                metrics_inserted += 1

            conn.commit()

            # Save checkpoint
            if last_id > checkpoint_id:
                Variable.set(CHECKPOINT_CMC_VAR, str(last_id))

            print(f"✓ CoinMarketCap: {cryptos_inserted} Cryptos, {metrics_inserted} Metrics loaded into Staging")
            return {
                "cryptos_inserted": cryptos_inserted,
                "metrics_inserted": metrics_inserted,
                "checkpoint_id": last_id
            }
        finally:
            conn.close()

    # ========== SUMMARY ==========

    @task(trigger_rule="all_done")
    def log_summary(
        binance: Dict[str, Any],
        ethereum: Dict[str, Any],
        cmc: Dict[str, Any]
    ) -> None:
        """Logs summary of entire Pipeline 2"""
        print("=" * 70)
        print("Pipeline 2 (Staging Zone) - Summary:")
        print("=" * 70)
        print(f"📊 Binance:       {binance}")
        print(f"⛓️  Ethereum:      {ethereum}")
        print(f"💰 CoinMarketCap: {cmc}")
        print("=" * 70)
        print("✅ All data cleaned and loaded into Staging Zone!")
        print("=" * 70)

    # ========== DAG WORKFLOW ==========

    # Create schema
    schema = create_staging_schema()

    # Binance: Fetch → Clean → Load
    binance_raw = fetch_binance_from_landing()
    binance_clean = clean_binance_data(binance_raw)
    binance_result = load_binance_to_staging(binance_clean)

    # Ethereum: Fetch → Clean → Load
    ethereum_raw = fetch_ethereum_from_landing()
    ethereum_clean = clean_ethereum_data(ethereum_raw)
    ethereum_result = load_ethereum_to_staging(ethereum_clean)

    # CoinMarketCap: Fetch → Clean → Load
    cmc_raw = fetch_coinmarketcap_from_landing()
    cmc_clean = clean_coinmarketcap_data(cmc_raw)
    cmc_result = load_coinmarketcap_to_staging(cmc_clean)

    # Summary
    summary = log_summary(binance_result, ethereum_result, cmc_result)

    # Dependencies
    schema >> [binance_raw, ethereum_raw, cmc_raw]
    [binance_result, ethereum_result, cmc_result] >> summary


pipeline_2_staging()
