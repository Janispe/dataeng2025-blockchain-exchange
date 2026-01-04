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

# Standardpfad im Airflow-Container (Mount von dags/data/binance_klines)
DEFAULT_DATA_ROOT = Path("/opt/airflow/dags/data/binance_klines")

# Fallbacks für MongoDB – können per Airflow Variables überschrieben werden
DEFAULT_MONGO_URI = "mongodb://root:example@mongodb:27017"
DEFAULT_MONGO_DB = "blockchain"
DEFAULT_MONGO_COLLECTION = "binance_candles"
DEFAULT_BATCH_SIZE = 500


def parse_filename(file_path: Path) -> Optional[Tuple[str, str]]:
    """
    Erwartet Schema SYMBOL-INTERVAL.jsonl und gibt (symbol, interval) zurück.
    Gibt None zurück, falls das Schema nicht passt.
    """
    stem_parts = file_path.stem.split("-")
    if len(stem_parts) < 2:
        return None

    symbol = stem_parts[0]
    interval = stem_parts[1]
    if not symbol or not interval:
        return None
    return symbol, interval


def parse_json_line(line: str, source_file: str) -> Optional[Tuple[Dict[str, Any], Dict[str, Any]]]:
    """
    Wandelt eine JSON-Zeile in ein Dokument + Upsert-Key um.
    Gibt None zurück, wenn das JSON-Format nicht passt.
    """
    try:
        doc = json.loads(line.strip())
        if not isinstance(doc, dict):
            return None

        # Validiere erforderliche Felder
        required_fields = ["symbol", "interval", "open_time_ms"]
        if not all(field in doc for field in required_fields):
            return None

        # Füge source_file hinzu (überschreibe ingested_at nicht, da es vom API-DAG kommt)
        doc["source_file"] = source_file

        # Erstelle Upsert-Key
        key = {
            "symbol": doc["symbol"],
            "interval": doc["interval"],
            "open_time_ms": doc["open_time_ms"]
        }

        return doc, key
    except (json.JSONDecodeError, KeyError, TypeError):
        return None


@dag(
    start_date=pendulum.datetime(2025, 11, 24, tz="Europe/Paris"),
    schedule="@daily",
    catchup=False,
    tags=["binance", "klines", "mongodb", "load"],
    default_args=dict(retries=2, retry_delay=pendulum.duration(minutes=2)),
    description="Lädt Kline-Daten aus dags/data/binance_klines nach MongoDB (JSONL-Format).",
)
def binance_candles_to_mongo():
    @task
    def get_settings() -> Dict[str, Any]:
        data_root = Variable.get("BINANCE_DATA_ROOT", default_var=str(DEFAULT_DATA_ROOT))
        uri = Variable.get("MONGO_URI", default_var=DEFAULT_MONGO_URI)
        db = Variable.get("MONGO_DB", default_var=DEFAULT_MONGO_DB)
        collection = Variable.get("BINANCE_MONGO_COLLECTION", default_var=DEFAULT_MONGO_COLLECTION)
        delete_after_load_raw = Variable.get("BINANCE_DELETE_AFTER_LOAD", default_var="false")
        batch_size_raw = Variable.get("BINANCE_MONGO_BATCH_SIZE", default_var=str(DEFAULT_BATCH_SIZE))

        delete_after_load = str(delete_after_load_raw).lower() in ("1", "true", "yes", "on")
        try:
            batch_size = max(1, int(batch_size_raw))
        except ValueError:
            batch_size = DEFAULT_BATCH_SIZE

        return {
            "data_root": data_root,
            "uri": uri,
            "db": db,
            "collection": collection,
            "delete_after_load": delete_after_load,
            "batch_size": batch_size,
        }

    @task
    def find_files(settings: Dict[str, Any]) -> List[str]:
        data_root = Path(settings["data_root"])
        if not data_root.exists():
            raise AirflowSkipException(f"Binance-Datenpfad existiert nicht: {data_root}")

        # Suche rekursiv nach JSONL-Dateien (auch in Unterordnern mit Datum)
        files = sorted(p for p in data_root.rglob("*.jsonl") if p.is_file())
        if not files:
            raise AirflowSkipException(f"Keine JSONL-Dateien unter {data_root} gefunden.")
        return [str(p) for p in files]

    @task
    def load_candles(settings: Dict[str, Any], files: List[str]) -> Dict[str, Any]:
        client = MongoClient(settings["uri"])
        collection = client[settings["db"]][settings["collection"]]
        collection.create_index([("symbol", 1), ("interval", 1), ("open_time_ms", 1)], unique=True)

        stats: Dict[str, Any] = {
            "files_seen": len(files),
            "files_loaded": 0,
            "rows_total": 0,
            "rows_skipped": 0,
            "upserted": 0,
            "modified": 0,
            "matched": 0,
        }

        batch: List[UpdateOne] = []

        for file_str in files:
            path = Path(file_str)
            parsed = parse_filename(path)
            if parsed is None:
                print(f"Überspringe Datei mit ungültigem Namen: {path.name}")
                continue

            symbol, interval = parsed
            try:
                with path.open("r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue

                        parsed_line = parse_json_line(line, path.name)
                        if parsed_line is None:
                            stats["rows_skipped"] += 1
                            continue

                        doc, key = parsed_line
                        batch.append(UpdateOne(key, {"$set": doc}, upsert=True))
                        stats["rows_total"] += 1

                        if len(batch) >= settings["batch_size"]:
                            result = collection.bulk_write(batch, ordered=False)
                            stats["upserted"] += result.upserted_count
                            stats["modified"] += result.modified_count
                            stats["matched"] += result.matched_count
                            batch.clear()
            except FileNotFoundError:
                print(f"Datei nicht gefunden: {path}")
                continue
            except Exception as exc:
                print(f"Fehler beim Verarbeiten von {path}: {exc}")
                continue

            # Schreibe verbleibende Batch-Daten nach jeder Datei
            if batch:
                result = collection.bulk_write(batch, ordered=False)
                stats["upserted"] += result.upserted_count
                stats["modified"] += result.modified_count
                stats["matched"] += result.matched_count
                batch.clear()

            stats["files_loaded"] += 1

            if settings.get("delete_after_load"):
                try:
                    path.unlink(missing_ok=True)
                    print(f"Datei gelöscht: {path}")
                except Exception as exc:
                    print(f"Warnung: Konnte Datei nicht löschen {path}: {exc}")

        client.close()

        if stats["rows_total"] == 0:
            raise AirflowSkipException("Keine gültigen Kline-Daten gefunden.")

        return stats

    @task(trigger_rule="all_done")
    def log_summary(stats: Dict[str, Any]) -> None:
        print(f"Binance Candles → MongoDB: {stats}")

    settings = get_settings()
    files = find_files(settings)
    stats = load_candles(settings, files)
    log_summary(stats)


binance_candles_to_mongo()
