from __future__ import annotations

import csv
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

# Standardpfad im Airflow-Container (Mount von dags/data/binance)
DEFAULT_DATA_ROOT = Path("/opt/airflow/dags/data/binance")

# Fallbacks für MongoDB – können per Airflow Variables überschrieben werden
DEFAULT_MONGO_URI = "mongodb://root:example@mongodb:27017"
DEFAULT_MONGO_DB = "blockchain"
DEFAULT_MONGO_COLLECTION = "binance_candles"
DEFAULT_BATCH_SIZE = 500


def parse_filename(file_path: Path) -> Optional[Tuple[str, str, str]]:
    """
    Erwartet Schema SYMBOL-INTERVAL-YYYY-MM-DD.csv und gibt (symbol, interval, datum) zurück.
    Gibt None zurück, falls das Schema nicht passt.
    """
    stem_parts = file_path.stem.split("-")
    if len(stem_parts) < 3:
        return None

    symbol = stem_parts[0]
    interval = stem_parts[1]
    trading_day = "-".join(stem_parts[2:])
    if not symbol or not interval:
        return None
    return symbol, interval, trading_day


def normalize_to_ms(epoch_value: int) -> int:
    """
    Konvertiert einen Zeitstempel (Sekunden/ms/mikrosekunden) heuristisch in Millisekunden.
    """
    if epoch_value >= 10**15:  # Mikrosekunden
        return epoch_value // 1000
    if epoch_value >= 10**12:  # Millisekunden
        return epoch_value
    if epoch_value >= 10**9:  # Sekunden
        return epoch_value * 1000
    return epoch_value


def parse_row(
    row: List[str], symbol: str, interval: str, trading_day: str, source_file: str, ingested_at: str
) -> Optional[Tuple[Dict[str, Any], Dict[str, Any]]]:
    """
    Wandelt eine CSV-Zeile in ein Dokument + Upsert-Key um.
    Gibt None zurück, wenn das Zeilenformat nicht passt.
    """
    if len(row) < 11:
        return None

    try:
        open_time_raw = int(float(row[0]))
        close_time_raw = int(float(row[6]))
        open_time_ms = normalize_to_ms(open_time_raw)
        close_time_ms = normalize_to_ms(close_time_raw)
        open_time_iso = pendulum.from_timestamp(open_time_ms / 1000, tz="UTC").to_iso8601_string()
        close_time_iso = pendulum.from_timestamp(close_time_ms / 1000, tz="UTC").to_iso8601_string()
        doc = {
            "symbol": symbol,
            "interval": interval,
            "trading_day": trading_day,
            "open_time_ms": open_time_ms,
            "open_time_raw": open_time_raw,
            "open_time_iso": open_time_iso,
            "close_time_ms": close_time_ms,
            "close_time_raw": close_time_raw,
            "close_time_iso": close_time_iso,
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4]),
            "volume": float(row[5]),
            "quote_asset_volume": float(row[7]),
            "number_of_trades": int(float(row[8])),
            "taker_buy_base_volume": float(row[9]),
            "taker_buy_quote_volume": float(row[10]),
            "source_file": source_file,
            "ingested_at": ingested_at,
        }
    except (ValueError, TypeError, IndexError):
        return None

    key = {"symbol": symbol, "interval": interval, "open_time_ms": open_time_ms}
    return doc, key


@dag(
    start_date=pendulum.datetime(2025, 11, 24, tz="Europe/Paris"),
    schedule="@daily",
    catchup=False,
    tags=["binance", "candles", "mongodb", "load"],
    default_args=dict(retries=2, retry_delay=pendulum.duration(minutes=2)),
    description="Lädt Candle-Sticks aus dags/data/binance nach MongoDB (einmal pro Tag).",
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

        files = sorted(p for p in data_root.glob("*.csv") if p.is_file())
        if not files:
            raise AirflowSkipException(f"Keine CSV-Dateien unter {data_root} gefunden.")
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
        ingest_ts = pendulum.now("UTC").to_iso8601_string()

        for file_str in files:
            path = Path(file_str)
            parsed = parse_filename(path)
            if parsed is None:
                stats["rows_skipped"] += 1
                continue

            symbol, interval, trading_day = parsed
            try:
                with path.open("r", encoding="utf-8", newline="") as f:
                    reader = csv.reader(f)
                    for row in reader:
                        parsed_row = parse_row(row, symbol, interval, trading_day, path.name, ingest_ts)
                        if parsed_row is None:
                            stats["rows_skipped"] += 1
                            continue
                        doc, key = parsed_row
                        batch.append(UpdateOne(key, {"$set": doc}, upsert=True))
                        stats["rows_total"] += 1

                        if len(batch) >= settings["batch_size"]:
                            result = collection.bulk_write(batch, ordered=False)
                            stats["upserted"] += result.upserted_count
                            stats["modified"] += result.modified_count
                            stats["matched"] += result.matched_count
                            batch.clear()
            except FileNotFoundError:
                continue

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
                except Exception as exc:
                    print(f"Warnung: Konnte Datei nicht löschen {path}: {exc}")

        if stats["rows_total"] == 0 and stats["upserted"] == 0 and stats["modified"] == 0:
            raise AirflowSkipException("Keine gültigen Candle-Daten gefunden.")

        return stats

    @task(trigger_rule="all_done")
    def log_summary(stats: Dict[str, Any]) -> None:
        print(f"Binance Candles → MongoDB: {stats}")

    settings = get_settings()
    files = find_files(settings)
    stats = load_candles(settings, files)
    log_summary(stats)


binance_candles_to_mongo()
