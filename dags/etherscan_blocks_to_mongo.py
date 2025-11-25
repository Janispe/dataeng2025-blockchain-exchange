from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import pendulum
from pymongo import MongoClient, UpdateOne

try:
    from airflow.sdk.dag import dag
    from airflow.sdk.task import task
except ModuleNotFoundError:
    from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable

# Pfad, an dem etherscan_latest_blocks die JSONL-Dateien ablegt
DATA_ROOT = Path("/opt/airflow/dags/data/ethereum_blocks")

# Fallbacks für MongoDB, können in Airflow Variables überschrieben werden
DEFAULT_MONGO_URI = "mongodb://root:example@mongodb:27017"
DEFAULT_MONGO_DB = "blockchain"
DEFAULT_MONGO_COLLECTION = "etherscan_blocks"


def blocks_path(execution_date: str) -> Path:
    """Ortsübliche Ableitung des JSONL-Pfades je Ausführungstag."""
    return DATA_ROOT / execution_date / "blocks.jsonl"


@dag(
    start_date=pendulum.datetime(2025, 10, 1, tz="Europe/Paris"),
    schedule="*/5 * * * *",  # alle 5 Minuten nachziehen
    catchup=False,
    tags=["ethereum", "etherscan", "mongodb", "load"],
    default_args=dict(retries=2, retry_delay=pendulum.duration(minutes=2)),
    description="Lädt die von etherscan_latest_blocks abgelegten Blöcke in MongoDB.",
)
def etherscan_blocks_to_mongo():
    @task
    def get_settings() -> Dict[str, Any]:
        """
        Liest Verbindungsparameter aus Airflow Variables mit sinnvollen Defaults.
        """
        uri = Variable.get("MONGO_URI", default_var=DEFAULT_MONGO_URI)
        db = Variable.get("MONGO_DB", default_var=DEFAULT_MONGO_DB)
        collection = Variable.get(
            "ETHERSCAN_MONGO_COLLECTION", default_var=DEFAULT_MONGO_COLLECTION
        )
        delete_after_load_raw = Variable.get(
            "ETHERSCAN_DELETE_AFTER_LOAD", default_var="true"
        )
        delete_after_load = str(delete_after_load_raw).lower() in (
            "1",
            "true",
            "yes",
            "y",
            "on",
        )
        return {
            "uri": uri,
            "db": db,
            "collection": collection,
            "delete_after_load": delete_after_load,
        }

    @task
    def load_blocks(settings: Dict[str, Any], ds: str) -> Dict[str, int]:
        """
        Liest die JSONL-Datei für das jeweilige Ausführungsdatum und upsertet die
        Blöcke idempotent nach MongoDB (unique auf Hash/Blocknummer).
        """
        file_path = blocks_path(ds)
        if not file_path.exists():
            raise AirflowSkipException(f"Keine Blocks-Datei gefunden: {file_path}")

        operations: List[UpdateOne] = []
        with file_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    block = json.loads(line)
                except json.JSONDecodeError:
                    continue

                if isinstance(block, str):
                    try:
                        block = json.loads(block)
                    except json.JSONDecodeError:
                        continue
                if not isinstance(block, dict):
                    continue

                number_hex = block.get("number")
                block_number = int(number_hex, 16) if number_hex else None
                ts_hex = block.get("timestamp")
                ts_unix = int(ts_hex, 16) if ts_hex else None
                block_hash = block.get("hash")

                # angereicherte Blockdarstellung
                doc = {
                    "hash": block_hash,
                    "number": block_number,
                    "number_hex": number_hex,
                    "timestamp_unix": ts_unix,
                    "tx_count": len(block.get("transactions") or []),
                    "raw": block,
                    "ingestion_ds": ds,
                    "ingested_at": pendulum.now("UTC").to_iso8601_string(),
                }

                key = {"hash": block_hash} if block_hash else {"number": block_number}
                if block_hash is None and block_number is None:
                    continue

                operations.append(UpdateOne(key, {"$set": doc}, upsert=True))

        if not operations:
            raise AirflowSkipException("Keine gültigen Blöcke zum Laden gefunden.")

        client = MongoClient(settings["uri"])
        collection = client[settings["db"]][settings["collection"]]

        # Idempotenz über eindeutige Indizes sicherstellen
        collection.create_index("hash", unique=True, sparse=True)
        collection.create_index("number", unique=True, sparse=True)

        result = collection.bulk_write(operations, ordered=False)
        if settings.get("delete_after_load"):
            try:
                file_path.unlink(missing_ok=True)
            except Exception as exc:
                print(f"Warnung: Blocks-Datei konnte nicht gelöscht werden: {exc}")
            else:
                # Ordner aufräumen, falls leer
                try:
                    file_path.parent.rmdir()
                except OSError:
                    pass

        return {
            "total_operations": len(operations),
            "upserted": result.upserted_count,
            "modified": result.modified_count,
            "matched": result.matched_count,
        }

    @task(trigger_rule="all_done")
    def log_summary(stats: Dict[str, int], ds: str) -> None:
        print(f"etherscan_blocks_to_mongo für {ds}: {stats}")

    settings = get_settings()
    stats = load_blocks(settings, ds="{{ ds }}")
    log_summary(stats, ds="{{ ds }}")


etherscan_blocks_to_mongo()
