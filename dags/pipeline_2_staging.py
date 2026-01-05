"""
Pipeline 2: Staging zone

Takes data from the landing zone, does basic cleanup/validation, and writes it into the staging schema.

Flow:
- MongoDB (binance_candles) -> Postgres staging.binance_candles
- MongoDB (etherscan_blocks) -> Postgres staging.ethereum_blocks
- Postgres (landing_coinmarketcap_raw) -> Postgres staging.cryptocurrencies + staging.crypto_metrics

Cleanup rules:
- Drop obviously broken rows
- Normalize symbols (uppercase)
- Convert hex fields to ints where needed
"""
from __future__ import annotations

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
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from pipeline_datasets import LANDING_DATASET, STAGING_DATASET

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
DEFAULT_BATCH_SIZE = 1000

# Checkpoints so we don't re-process the whole world every run
CHECKPOINT_BINANCE_VAR = "STAGING_LAST_BINANCE_INGESTED_AT"
CHECKPOINT_ETHERSCAN_VAR = "STAGING_LAST_ETHERSCAN_INGESTED_AT"
CHECKPOINT_CMC_VAR = "STAGING_LAST_CMC_ID"


def _pg_connect_staging():
    # Postgres connection for the staging DB (postgres-data).
    # Try psycopg2 first, then fallback to psycopg.
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
    # Postgres connection for the landing DB (Airflow Postgres). Used only for CoinMarketCap raw.
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
    # Accepts either "0x..." or a plain decimal string/int. Returns None if it's junk.
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
    schedule=[LANDING_DATASET],
    catchup=False,
    max_active_runs=1,
    tags=["pipeline-2", "staging", "transformation"],
    default_args=dict(retries=2, retry_delay=pendulum.duration(minutes=2)),
    description="Pipeline 2: Lädt und bereinigt Daten von Landing → Staging Zone.",
)
def pipeline_2_staging():
    @task
    def get_binance_checkpoint() -> Optional[str]:
        # Last processed timestamp for Binance docs (string timestamp stored by previous run).
        return Variable.get(CHECKPOINT_BINANCE_VAR, default_var=None)

    @task
    def get_etherscan_checkpoint() -> Optional[str]:
        # Last processed timestamp for Ethereum block docs.
        return Variable.get(CHECKPOINT_ETHERSCAN_VAR, default_var=None)

    @task
    def get_coinmarketcap_checkpoint_id() -> int:
        # Last processed landing row id for CoinMarketCap.
        return int(Variable.get(CHECKPOINT_CMC_VAR, default_var="0"))

    @task
    def create_staging_schema() -> None:
        # Creates schema + tables if they don't exist (safe to run every time).
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

            CREATE INDEX IF NOT EXISTS ix_staging_binance_staged_at
              ON {STAGING_SCHEMA}.binance_candles (staged_at);

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

            CREATE INDEX IF NOT EXISTS ix_staging_ethereum_staged_at
              ON {STAGING_SCHEMA}.ethereum_blocks (staged_at);

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
            print(f"✓ Staging-Schema '{STAGING_SCHEMA}' erfolgreich erstellt")
        finally:
            conn.close()

    @task
    def stage_binance_candles(checkpoint: Optional[str]) -> Dict[str, Any]:
        # Binance: MongoDB -> Postgres staging.
        # Uses updated_at/ingested_at checkpoint so we only move "new-ish" docs.
        query: Dict[str, Any] = {}
        if checkpoint:
            query = {"$or": [{"updated_at": {"$gt": checkpoint}}, {"ingested_at": {"$gt": checkpoint}}]}

        mongo_uri = Variable.get("MONGO_URI", default_var=DEFAULT_MONGO_URI)
        mongo_db = Variable.get("MONGO_DB", default_var=DEFAULT_MONGO_DB)
        collection_name = Variable.get("BINANCE_MONGO_COLLECTION", default_var=DEFAULT_BINANCE_COLLECTION)
        batch_size = int(Variable.get("STAGING_BATCH_SIZE", default_var=str(DEFAULT_BATCH_SIZE)))

        client = MongoClient(mongo_uri)
        coll = client[mongo_db][collection_name]
        conn = _pg_connect_staging()

        stats = {"seen": 0, "valid": 0, "invalid": 0, "inserted": 0, "checkpoint": checkpoint}
        batch: List[Dict[str, Any]] = []
        last_event_at: Optional[str] = checkpoint

        try:
            cursor = coll.find(query).sort([("updated_at", 1), ("ingested_at", 1)])

            for doc in cursor:
                stats["seen"] += 1

                try:
                    symbol = (doc.get("symbol") or "").strip().upper()
                    interval = (doc.get("interval") or "").strip()
                    open_time_ms = doc.get("open_time_ms")
                    close_time_ms = doc.get("close_time_ms")
                    close_price = doc.get("close")

                    # Must-have fields (otherwise we can't store it reliably)
                    if not symbol or not interval or open_time_ms is None or close_time_ms is None:
                        stats["invalid"] += 1
                        continue

                    # Quick sanity check: negative/zero close prices are almost always bad data
                    if close_price is not None and float(close_price) <= 0:
                        stats["invalid"] += 1
                        continue

                    # trading_day comes as string sometimes
                    trading_day: Optional[date] = None
                    trading_day_raw = doc.get("trading_day")
                    if isinstance(trading_day_raw, str):
                        try:
                            trading_day = date.fromisoformat(trading_day_raw)
                        except ValueError:
                            pass

                    cleaned = {
                        "symbol": symbol,
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

                    batch.append(cleaned)
                    stats["valid"] += 1

                    event_at = doc.get("updated_at") or doc.get("ingested_at")
                    if isinstance(event_at, str):
                        last_event_at = event_at

                except (KeyError, TypeError, ValueError):
                    stats["invalid"] += 1
                    continue

                if len(batch) >= batch_size:
                    stats["inserted"] += _insert_binance_batch(conn, batch)
                    batch = []

            if batch:
                stats["inserted"] += _insert_binance_batch(conn, batch)

            if stats["inserted"] == 0:
                raise AirflowSkipException("Keine neuen Binance-Daten")

            stats["new_checkpoint"] = last_event_at
            return stats

        finally:
            conn.close()
            client.close()

    def _insert_binance_batch(conn, batch: List[Dict[str, Any]]) -> int:
        # Inserts/updates Binance candles in bulk (idempotent thanks to ON CONFLICT).
        if not batch:
            return 0

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
          open = EXCLUDED.open,
          high = EXCLUDED.high,
          low = EXCLUDED.low,
          close = EXCLUDED.close,
          volume = EXCLUDED.volume,
          quote_asset_volume = EXCLUDED.quote_asset_volume,
          number_of_trades = EXCLUDED.number_of_trades,
          taker_buy_base_volume = EXCLUDED.taker_buy_base_volume,
          taker_buy_quote_volume = EXCLUDED.taker_buy_quote_volume,
          source_file = EXCLUDED.source_file,
          ingested_at = EXCLUDED.ingested_at,
          staged_at = NOW()
        """

        rows = [
            (
                d["symbol"],
                d["interval"],
                d["open_time_ms"],
                d["close_time_ms"],
                d["trading_day"],
                d["open"],
                d["high"],
                d["low"],
                d["close"],
                d["volume"],
                d["quote_asset_volume"],
                d["number_of_trades"],
                d["taker_buy_base_volume"],
                d["taker_buy_quote_volume"],
                d["source_file"],
                d["ingested_at"],
            )
            for d in batch
        ]

        with conn.cursor() as cur:
            cur.executemany(sql, rows)
        conn.commit()
        return len(rows)

    @task
    def stage_ethereum_blocks(checkpoint: Optional[str]) -> Dict[str, Any]:
        # Ethereum: MongoDB -> Postgres staging. Same checkpoint logic as Binance.
        query: Dict[str, Any] = {}
        if checkpoint:
            query = {"$or": [{"updated_at": {"$gt": checkpoint}}, {"ingested_at": {"$gt": checkpoint}}]}

        mongo_uri = Variable.get("MONGO_URI", default_var=DEFAULT_MONGO_URI)
        mongo_db = Variable.get("MONGO_DB", default_var=DEFAULT_MONGO_DB)
        collection_name = Variable.get("ETHERSCAN_MONGO_COLLECTION", default_var=DEFAULT_ETHERSCAN_COLLECTION)
        batch_size = int(Variable.get("STAGING_BATCH_SIZE", default_var=str(DEFAULT_BATCH_SIZE)))

        client = MongoClient(mongo_uri)
        coll = client[mongo_db][collection_name]
        conn = _pg_connect_staging()

        stats = {"seen": 0, "valid": 0, "invalid": 0, "inserted": 0, "checkpoint": checkpoint}
        batch: List[Dict[str, Any]] = []
        last_event_at: Optional[str] = checkpoint

        try:
            cursor = coll.find(query).sort([("updated_at", 1), ("ingested_at", 1)])

            for doc in cursor:
                stats["seen"] += 1

                try:
                    block_hash = doc.get("hash")
                    block_number = doc.get("number")
                    timestamp_unix = doc.get("timestamp_unix")

                    if not block_hash or block_number is None or timestamp_unix is None:
                        stats["invalid"] += 1
                        continue

                    raw = doc.get("raw") if isinstance(doc.get("raw"), dict) else {}

                    ingestion_ds: Optional[date] = None
                    ingestion_ds_raw = doc.get("ingestion_ds")
                    if isinstance(ingestion_ds_raw, str):
                        try:
                            ingestion_ds = date.fromisoformat(ingestion_ds_raw)
                        except ValueError:
                            pass

                    cleaned = {
                        "block_hash": str(block_hash),
                        "block_number": int(block_number),
                        "timestamp_unix": int(timestamp_unix),
                        "tx_count": doc.get("tx_count"),
                        "miner": raw.get("miner"),
                        "gas_used": _parse_hex_int(raw.get("gasUsed")),
                        "gas_limit": _parse_hex_int(raw.get("gasLimit")),
                        "size_bytes": _parse_hex_int(raw.get("size")),
                        "base_fee_per_gas": _parse_hex_int(raw.get("baseFeePerGas")),
                        "blob_gas_used": _parse_hex_int(raw.get("blobGasUsed")),
                        "excess_blob_gas": _parse_hex_int(raw.get("excessBlobGas")),
                        "ingestion_ds": ingestion_ds,
                        "ingested_at": doc.get("ingested_at"),
                    }

                    batch.append(cleaned)
                    stats["valid"] += 1

                    event_at = doc.get("updated_at") or doc.get("ingested_at")
                    if isinstance(event_at, str):
                        last_event_at = event_at

                except (KeyError, TypeError, ValueError):
                    stats["invalid"] += 1
                    continue

                if len(batch) >= batch_size:
                    stats["inserted"] += _insert_ethereum_batch(conn, batch)
                    batch = []

            if batch:
                stats["inserted"] += _insert_ethereum_batch(conn, batch)

            if stats["inserted"] == 0:
                raise AirflowSkipException("Keine neuen Ethereum-Blöcke")

            stats["new_checkpoint"] = last_event_at
            return stats

        finally:
            conn.close()
            client.close()

    def _insert_ethereum_batch(conn, batch: List[Dict[str, Any]]) -> int:
        # Inserts/updates Ethereum blocks in bulk.
        if not batch:
            return 0

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
          block_number = EXCLUDED.block_number,
          timestamp_unix = EXCLUDED.timestamp_unix,
          tx_count = EXCLUDED.tx_count,
          miner = EXCLUDED.miner,
          gas_used = EXCLUDED.gas_used,
          gas_limit = EXCLUDED.gas_limit,
          size_bytes = EXCLUDED.size_bytes,
          base_fee_per_gas = EXCLUDED.base_fee_per_gas,
          blob_gas_used = EXCLUDED.blob_gas_used,
          excess_blob_gas = EXCLUDED.excess_blob_gas,
          ingestion_ds = COALESCE(EXCLUDED.ingestion_ds, {STAGING_SCHEMA}.ethereum_blocks.ingestion_ds),
          ingested_at = EXCLUDED.ingested_at,
          staged_at = NOW()
        """

        rows = [
            (
                d["block_hash"],
                d["block_number"],
                d["timestamp_unix"],
                d["tx_count"],
                d["miner"],
                d["gas_used"],
                d["gas_limit"],
                d["size_bytes"],
                d["base_fee_per_gas"],
                d["blob_gas_used"],
                d["excess_blob_gas"],
                d["ingestion_ds"],
                d["ingested_at"],
            )
            for d in batch
        ]

        with conn.cursor() as cur:
            cur.executemany(sql, rows)
        conn.commit()
        return len(rows)

    @task
    def stage_coinmarketcap(checkpoint_id: int) -> Dict[str, Any]:
        # CoinMarketCap: landing (Airflow Postgres) -> staging (postgres-data).
        # Checkpoint is an auto-increment id from landing_coinmarketcap_raw.
        import json

        landing_conn = _pg_connect_landing()
        staging_conn = _pg_connect_staging()

        stats = {
            "seen": 0,
            "valid": 0,
            "invalid": 0,
            "cryptos_inserted": 0,
            "metrics_inserted": 0,
            "checkpoint_id": checkpoint_id,
        }

        try:
            # Just in case pipeline_1 didn't run yet, ensure the landing table exists
            with landing_conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS landing_coinmarketcap_raw (
                        id SERIAL PRIMARY KEY,
                        ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        api_response JSONB,
                        status_code INTEGER,
                        request_params JSONB,
                        source_file TEXT
                    );
                    """
                )
            landing_conn.commit()

            # Grab everything after the last checkpoint
            with landing_conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, api_response, ingestion_timestamp
                    FROM landing_coinmarketcap_raw
                    WHERE id > %s
                    ORDER BY id
                    """,
                    (checkpoint_id,),
                )
                rows = cur.fetchall()

            if not rows:
                raise AirflowSkipException("Keine neuen CoinMarketCap-Daten")

            last_id = checkpoint_id

            for row_id, api_response, ingestion_time in rows:
                stats["seen"] += 1
                data = json.loads(api_response) if isinstance(api_response, str) else api_response
                cryptocurrencies = data.get("data", [])

                crypto_rows: List[Any] = []
                metrics_rows: List[Any] = []

                for crypto in cryptocurrencies:
                    try:
                        cleaned = {
                            "crypto_id": crypto["id"],
                            "symbol": crypto["symbol"].upper().strip(),
                            "name": crypto["name"].strip(),
                            "slug": crypto["slug"],
                            "last_updated": crypto["last_updated"],
                            "quote": crypto["quote"]["USD"],
                        }

                        # Must-have fields
                        if not cleaned["crypto_id"] or not cleaned["symbol"] or not cleaned["name"]:
                            stats["invalid"] += 1
                            continue

                        # Price sanity check
                        quote = cleaned["quote"]
                        if not quote.get("price") or quote.get("price") <= 0:
                            stats["invalid"] += 1
                            continue

                        stats["valid"] += 1
                        crypto_rows.append(
                            (
                                cleaned["crypto_id"],
                                cleaned["symbol"],
                                cleaned["name"],
                                cleaned["slug"],
                                cleaned["last_updated"],
                            )
                        )
                        metrics_rows.append(
                            (
                                cleaned["crypto_id"],
                                quote.get("price"),
                                quote.get("volume_24h"),
                                quote.get("market_cap"),
                                quote.get("percent_change_1h"),
                                quote.get("percent_change_24h"),
                                quote.get("percent_change_7d"),
                                quote.get("circulating_supply"),
                                quote.get("total_supply"),
                                quote.get("max_supply"),
                            )
                        )

                    except (KeyError, TypeError):
                        stats["invalid"] += 1
                        continue

                crypto_upsert_sql = f"""
                INSERT INTO {STAGING_SCHEMA}.cryptocurrencies
                    (crypto_id, symbol, name, slug, last_updated)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (crypto_id)
                DO UPDATE SET
                    symbol = EXCLUDED.symbol,
                    name = EXCLUDED.name,
                    slug = EXCLUDED.slug,
                    last_updated = EXCLUDED.last_updated,
                    processed_at = CURRENT_TIMESTAMP
                """

                metrics_insert_sql = f"""
                INSERT INTO {STAGING_SCHEMA}.crypto_metrics
                    (crypto_id, price_usd, volume_24h, market_cap,
                     percent_change_1h, percent_change_24h, percent_change_7d,
                     circulating_supply, total_supply, max_supply)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (crypto_id, recorded_at) DO NOTHING
                """

                with staging_conn.cursor() as cur:
                    if crypto_rows:
                        cur.executemany(crypto_upsert_sql, crypto_rows)
                        stats["cryptos_inserted"] += len(crypto_rows)
                    if metrics_rows:
                        cur.executemany(metrics_insert_sql, metrics_rows)
                        stats["metrics_inserted"] += len(metrics_rows)

                staging_conn.commit()
                last_id = row_id

            stats["new_checkpoint_id"] = last_id
            return stats

        finally:
            landing_conn.close()
            staging_conn.close()

    @task
    def update_binance_checkpoint(stats: Dict[str, Any]) -> Dict[str, Any]:
        # Persist the new checkpoint for Binance (if it actually advanced).
        previous = stats.get("checkpoint")
        new_val = stats.get("new_checkpoint")
        if new_val and new_val != previous:
            Variable.set(CHECKPOINT_BINANCE_VAR, str(new_val))
            return {"checkpoint": str(new_val)}
        return {"checkpoint": previous}

    @task
    def update_etherscan_checkpoint(stats: Dict[str, Any]) -> Dict[str, Any]:
        # Persist the new checkpoint for Ethereum blocks.
        previous = stats.get("checkpoint")
        new_val = stats.get("new_checkpoint")
        if new_val and new_val != previous:
            Variable.set(CHECKPOINT_ETHERSCAN_VAR, str(new_val))
            return {"checkpoint": str(new_val)}
        return {"checkpoint": previous}

    @task
    def update_coinmarketcap_checkpoint(stats: Dict[str, Any]) -> Dict[str, Any]:
        # Persist the new checkpoint (landing row id).
        previous = stats.get("checkpoint_id")
        new_val = stats.get("new_checkpoint_id")
        if isinstance(new_val, int) and isinstance(previous, int) and new_val > previous:
            Variable.set(CHECKPOINT_CMC_VAR, str(new_val))
            return {"checkpoint_id": new_val}
        return {"checkpoint_id": previous}

    @task(trigger_rule="all_done")
    def log_summary(
        binance: Dict[str, Any],
        ethereum: Dict[str, Any],
        cmc: Dict[str, Any],
        binance_checkpoint: Dict[str, Any],
        ethereum_checkpoint: Dict[str, Any],
        cmc_checkpoint: Dict[str, Any],
    ) -> None:
        # Quick run summary to make debugging easier in Airflow logs.
        print("=" * 60)
        print("Pipeline 2 (Staging Zone) Summary:")
        print("=" * 60)
        print(f"Binance: {binance}")
        print(f"Ethereum: {ethereum}")
        print(f"CoinMarketCap: {cmc}")
        print(f"Binance checkpoint: {binance_checkpoint}")
        print(f"Ethereum checkpoint: {ethereum_checkpoint}")
        print(f"CoinMarketCap checkpoint: {cmc_checkpoint}")
        print("=" * 60)

    schema = create_staging_schema()
    binance_cp = get_binance_checkpoint()
    ethereum_cp = get_etherscan_checkpoint()
    cmc_cp = get_coinmarketcap_checkpoint_id()

    schema >> [binance_cp, ethereum_cp, cmc_cp]

    binance_stats = stage_binance_candles(binance_cp)
    ethereum_stats = stage_ethereum_blocks(ethereum_cp)
    cmc_stats = stage_coinmarketcap(cmc_cp)

    schema >> [binance_stats, ethereum_stats, cmc_stats]
    binance_checkpoint = update_binance_checkpoint(binance_stats)
    ethereum_checkpoint = update_etherscan_checkpoint(ethereum_stats)
    cmc_checkpoint = update_coinmarketcap_checkpoint(cmc_stats)

    log_summary(binance_stats, ethereum_stats, cmc_stats, binance_checkpoint, ethereum_checkpoint, cmc_checkpoint)

    publish_staging_dataset = EmptyOperator(
        task_id="publish_staging_dataset",
        outlets=[STAGING_DATASET],
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    [binance_stats, ethereum_stats, cmc_stats] >> publish_staging_dataset


pipeline_2_staging()

