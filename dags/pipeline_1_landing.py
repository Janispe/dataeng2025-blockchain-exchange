"""
Pipeline 1: Landing Zone
=======================
Loads raw data (from API ingestion or sample files) into the landing zone.
Also works OFFLINE with sample data (no API keys required).

Landing Zones:
- MongoDB: binance_candles, etherscan_blocks
- Postgres: landing_coinmarketcap_raw

Input: JSON/JSONL files from data/
Output: Landing zone databases
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pendulum
from pymongo import MongoClient, UpdateOne

try:
    from airflow.sdk.dag import dag
    from airflow.sdk.task import task
except ModuleNotFoundError:
    from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from pipeline_datasets import LANDING_DATASET

# Input directories (from API ingestion or manually placed)
BINANCE_INPUT_DIR = Path("/opt/airflow/dags/data/binance_klines")
ETHERSCAN_INPUT_DIR = Path("/opt/airflow/dags/data/ethereum_blocks")
COINMARKETCAP_INPUT_DIR = Path("/opt/airflow/dags/data/coinmarketcap")

# MongoDB defaults
DEFAULT_MONGO_URI = "mongodb://root:example@mongodb:27017"
DEFAULT_MONGO_DB = "blockchain"
DEFAULT_BINANCE_COLLECTION = "binance_candles"
DEFAULT_ETHERSCAN_COLLECTION = "etherscan_blocks"

# Postgres defaults (for CoinMarketCap landing)
DEFAULT_PG_HOST = "postgres"
DEFAULT_PG_PORT = 5432
DEFAULT_PG_DB = "airflow"
DEFAULT_PG_USER = "airflow"
DEFAULT_PG_PASSWORD = "airflow"

DEFAULT_BATCH_SIZE = 500


def _is_truthy(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _resolve_partitioned_files(base_dir: Path, ds: str, pattern: str) -> List[Path]:
    """
    By default scan the whole base_dir (load *all* available files).
    If AIRFLOW Variable LANDING_ONLY_DS_PARTITION=true, prefer only /<base>/<ds>/...
    (API Ingestion writes partitioned by ds).
    """
    only_ds_partition = _is_truthy(Variable.get("LANDING_ONLY_DS_PARTITION", default_var="false"))
    if only_ds_partition:
        try:
            ds_dir = base_dir / str(ds)
            if ds and ds_dir.exists():
                return sorted(ds_dir.rglob(pattern))
        except Exception:
            return []
    return sorted(base_dir.rglob(pattern))


def _pg_connect_landing():
    """PostgreSQL connection for the landing zone (Airflow DB)."""
    try:
        import psycopg2  # type: ignore

        host = Variable.get("LANDING_PG_HOST", default_var=DEFAULT_PG_HOST)
        port = int(Variable.get("LANDING_PG_PORT", default_var=str(DEFAULT_PG_PORT)))
        db = Variable.get("LANDING_PG_DB", default_var=DEFAULT_PG_DB)
        user = Variable.get("LANDING_PG_USER", default_var=DEFAULT_PG_USER)
        password = Variable.get("LANDING_PG_PASSWORD", default_var=DEFAULT_PG_PASSWORD)

        return psycopg2.connect(host=host, port=port, dbname=db, user=user, password=password)
    except ModuleNotFoundError:
        import psycopg  # type: ignore

        host = Variable.get("LANDING_PG_HOST", default_var=DEFAULT_PG_HOST)
        port = int(Variable.get("LANDING_PG_PORT", default_var=str(DEFAULT_PG_PORT)))
        db = Variable.get("LANDING_PG_DB", default_var=DEFAULT_PG_DB)
        user = Variable.get("LANDING_PG_USER", default_var=DEFAULT_PG_USER)
        password = Variable.get("LANDING_PG_PASSWORD", default_var=DEFAULT_PG_PASSWORD)

        return psycopg.connect(host=host, port=port, dbname=db, user=user, password=password)


@dag(
    start_date=pendulum.datetime(2025, 10, 1, tz="Europe/Paris"),
    schedule="*/15 * * * *",  # every 15 minutes
    catchup=False,
    tags=["pipeline-1", "landing", "ingestion"],
    default_args=dict(retries=2, retry_delay=pendulum.duration(minutes=2)),
    description="Pipeline 1: Loads raw data (API results or sample files) into the landing zone (MongoDB/Postgres).",
)
def pipeline_1_landing():
    @task
    def discover_binance_files(ds: str) -> List[str]:
        input_dir = BINANCE_INPUT_DIR
        files = _resolve_partitioned_files(input_dir, ds, "*.jsonl")
        return [str(p) for p in files]

    @task
    def discover_etherscan_files(ds: str) -> List[str]:
        input_dir = ETHERSCAN_INPUT_DIR
        files = _resolve_partitioned_files(input_dir, ds, "*.jsonl")
        return [str(p) for p in files]

    @task
    def discover_coinmarketcap_files(ds: str) -> List[str]:
        input_dir = COINMARKETCAP_INPUT_DIR
        files = _resolve_partitioned_files(input_dir, ds, "*.json")
        return [str(p) for p in files]

    @task
    def cleanup_input_files(file_paths: List[str], delete_variable: str) -> Dict[str, Any]:
        if not file_paths:
            return {"attempted": 0, "deleted": 0}
        if not _is_truthy(Variable.get(delete_variable, default_var="false")):
            return {"attempted": len(file_paths), "deleted": 0}

        deleted = 0
        for p in file_paths:
            try:
                Path(p).unlink(missing_ok=True)
                deleted += 1
            except Exception as e:
                print(f"✗ Failed to delete {p}: {e}")
        return {"attempted": len(file_paths), "deleted": deleted}

    @task
    def load_binance_to_landing(file_paths: List[str], ds: str) -> Dict[str, Any]:
        """Loads Binance JSONL files into the MongoDB landing zone."""
        mongo_uri = Variable.get("MONGO_URI", default_var=DEFAULT_MONGO_URI)
        mongo_db = Variable.get("MONGO_DB", default_var=DEFAULT_MONGO_DB)
        collection_name = Variable.get("BINANCE_MONGO_COLLECTION", default_var=DEFAULT_BINANCE_COLLECTION)
        batch_size = int(Variable.get("LANDING_BATCH_SIZE", default_var=str(DEFAULT_BATCH_SIZE)))

        files = [Path(p) for p in (file_paths or [])]
        if not files:
            raise AirflowSkipException(f"Keine Binance JSONL-Dateien gefunden in {BINANCE_INPUT_DIR}")

        client = MongoClient(mongo_uri)
        collection = client[mongo_db][collection_name]
        collection.create_index([("symbol", 1), ("interval", 1), ("open_time_ms", 1)], unique=True)
        collection.create_index("ingested_at")
        collection.create_index("updated_at")

        stats = {"files_processed": 0, "rows_loaded": 0, "rows_skipped": 0}
        batch: List[UpdateOne] = []
        ingested_at = pendulum.now("UTC").to_iso8601_string()
        updated_at = ingested_at

        for file_path in files:
            try:
                with file_path.open("r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue

                        try:
                            doc = json.loads(line)
                            if not isinstance(doc, dict):
                                stats["rows_skipped"] += 1
                                continue

                            # Validate required fields
                            if not all(k in doc for k in ["symbol", "interval", "open_time_ms"]):
                                stats["rows_skipped"] += 1
                                continue

                            # Upsert key
                            key = {
                                "symbol": doc["symbol"],
                                "interval": doc["interval"],
                                "open_time_ms": doc["open_time_ms"],
                            }

                            batch.append(
                                UpdateOne(
                                    key,
                                    {
                                        "$set": {**doc, "updated_at": updated_at},
                                        "$setOnInsert": {
                                            "ingested_at": ingested_at,
                                            "ingestion_ds": ds,
                                            "source_file": file_path.name,
                                        },
                                    },
                                    upsert=True,
                                )
                            )
                            stats["rows_loaded"] += 1

                            if len(batch) >= batch_size:
                                collection.bulk_write(batch, ordered=False)
                                batch.clear()

                        except json.JSONDecodeError:
                            stats["rows_skipped"] += 1
                            continue

                # Write remaining batch
                if batch:
                    collection.bulk_write(batch, ordered=False)
                    batch.clear()

                stats["files_processed"] += 1
                print(f"✓ Binance → MongoDB: {file_path.name}")

            except Exception as e:
                print(f"✗ Fehler bei {file_path}: {e}")
                continue

        client.close()

        if stats["rows_loaded"] == 0:
            raise AirflowSkipException("Keine Binance-Daten geladen")

        return stats

    @task
    def load_etherscan_to_landing(file_paths: List[str], ds: str) -> Dict[str, Any]:
        """Loads Ethereum block JSONL files into the MongoDB landing zone."""
        mongo_uri = Variable.get("MONGO_URI", default_var=DEFAULT_MONGO_URI)
        mongo_db = Variable.get("MONGO_DB", default_var=DEFAULT_MONGO_DB)
        collection_name = Variable.get("ETHERSCAN_MONGO_COLLECTION", default_var=DEFAULT_ETHERSCAN_COLLECTION)

        files = [Path(p) for p in (file_paths or [])]
        if not files:
            raise AirflowSkipException(f"Keine Etherscan JSONL-Dateien gefunden in {ETHERSCAN_INPUT_DIR}")

        client = MongoClient(mongo_uri)
        collection = client[mongo_db][collection_name]
        collection.create_index("hash", unique=True, sparse=True)
        collection.create_index("number", unique=True, sparse=True)
        collection.create_index("ingested_at")
        collection.create_index("updated_at")

        stats = {"files_processed": 0, "blocks_loaded": 0, "blocks_skipped": 0}
        operations: List[UpdateOne] = []
        ingested_at = pendulum.now("UTC").to_iso8601_string()
        updated_at = ingested_at

        for file_path in files:
            try:
                with file_path.open("r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue

                        try:
                            block = json.loads(line)
                            if not isinstance(block, dict):
                                stats["blocks_skipped"] += 1
                                continue

                            number_hex = block.get("number")
                            block_number = int(number_hex, 16) if number_hex else None
                            ts_hex = block.get("timestamp")
                            ts_unix = int(ts_hex, 16) if ts_hex else None
                            block_hash = block.get("hash")

                            if not block_hash or block_number is None:
                                stats["blocks_skipped"] += 1
                                continue

                            # Enriched document
                            doc = {
                                "hash": block_hash,
                                "number": block_number,
                                "number_hex": number_hex,
                                "timestamp_unix": ts_unix,
                                "tx_count": len(block.get("transactions") or []),
                                "raw": block,
                            }

                            key = {"hash": block_hash}
                            operations.append(
                                UpdateOne(
                                    key,
                                    {
                                        "$set": {**doc, "updated_at": updated_at},
                                        "$setOnInsert": {
                                            "ingestion_ds": ds,
                                            "ingested_at": ingested_at,
                                            "source_file": file_path.name,
                                        },
                                    },
                                    upsert=True,
                                )
                            )
                            stats["blocks_loaded"] += 1

                        except (json.JSONDecodeError, ValueError):
                            stats["blocks_skipped"] += 1
                            continue

                # Write batch
                if operations:
                    collection.bulk_write(operations, ordered=False)
                    operations.clear()

                stats["files_processed"] += 1
                print(f"✓ Etherscan → MongoDB: {file_path.name}")

            except Exception as e:
                print(f"✗ Fehler bei {file_path}: {e}")
                continue

        client.close()

        if stats["blocks_loaded"] == 0:
            raise AirflowSkipException("Keine Ethereum-Blöcke geladen")

        return stats

    @task
    def load_coinmarketcap_to_landing(file_paths: List[str], ds: str) -> Dict[str, Any]:
        """Loads CoinMarketCap JSON files into the Postgres landing zone."""
        files = [Path(p) for p in (file_paths or [])]
        if not files:
            raise AirflowSkipException(f"Keine CoinMarketCap JSON-Dateien gefunden in {COINMARKETCAP_INPUT_DIR}")

        conn = _pg_connect_landing()
        try:
            # Create landing table
            with conn.cursor() as cur:
                cur.execute("""
                CREATE TABLE IF NOT EXISTS landing_coinmarketcap_raw (
                    id SERIAL PRIMARY KEY,
                    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    api_response JSONB,
                    status_code INTEGER,
                    request_params JSONB,
                    source_file TEXT
                );
                """)
            conn.commit()

            stats = {"files_processed": 0, "rows_loaded": 0}

            for file_path in files:
                try:
                    data = json.loads(file_path.read_text(encoding="utf-8"))

                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            INSERT INTO landing_coinmarketcap_raw (api_response, status_code, source_file)
                            VALUES (%s, %s, %s)
                            RETURNING id;
                            """,
                            (
                                json.dumps(data),
                                200,  # assume success
                                str(file_path),
                            ),
                        )
                    conn.commit()

                    stats["files_processed"] += 1
                    stats["rows_loaded"] += 1
                    print(f"✓ CoinMarketCap → Postgres: {file_path.name}")

                except Exception as e:
                    print(f"✗ Fehler bei {file_path}: {e}")
                    continue

            if stats["rows_loaded"] == 0:
                raise AirflowSkipException("Keine CoinMarketCap-Daten geladen")

            return stats

        finally:
            conn.close()

    @task(trigger_rule="all_done")
    def log_summary(
        binance: Dict[str, Any], etherscan: Dict[str, Any], cmc: Dict[str, Any]
    ) -> None:
        """Loggt Zusammenfassung von Pipeline 1"""
        print("=" * 60)
        print("Pipeline 1 (Landing Zone) Summary:")
        print("=" * 60)
        print(f"Binance → MongoDB: {binance}")
        print(f"Etherscan → MongoDB: {etherscan}")
        print(f"CoinMarketCap → Postgres: {cmc}")
        print("=" * 60)

    binance_files = discover_binance_files(ds="{{ ds }}")
    etherscan_files = discover_etherscan_files(ds="{{ ds }}")
    cmc_files = discover_coinmarketcap_files(ds="{{ ds }}")

    binance_stats = load_binance_to_landing(binance_files, ds="{{ ds }}")
    etherscan_stats = load_etherscan_to_landing(etherscan_files, ds="{{ ds }}")
    cmc_stats = load_coinmarketcap_to_landing(cmc_files, ds="{{ ds }}")

    binance_cleanup = cleanup_input_files(binance_files, "BINANCE_DELETE_AFTER_LOAD")
    etherscan_cleanup = cleanup_input_files(etherscan_files, "ETHERSCAN_DELETE_AFTER_LOAD")
    cmc_cleanup = cleanup_input_files(cmc_files, "COINMARKETCAP_DELETE_AFTER_LOAD")

    binance_stats >> binance_cleanup
    etherscan_stats >> etherscan_cleanup
    cmc_stats >> cmc_cleanup

    log_summary(binance_stats, etherscan_stats, cmc_stats)

    publish_landing_dataset = EmptyOperator(
        task_id="publish_landing_dataset",
        outlets=[LANDING_DATASET],
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    [binance_stats, etherscan_stats, cmc_stats] >> publish_landing_dataset


pipeline_1_landing()
