# dags/binance_api_fetch.py
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Dict, List, Any

import pendulum
import requests
try:
    from airflow.sdk.dag import dag
    from airflow.sdk.task import task
except ModuleNotFoundError:
    from airflow.decorators import dag, task
from airflow.models import Variable

# -------- Konfiguration --------
BINANCE_BASE_URL = "https://api.binance.com"
# Binance erlaubt 1200 requests/minute für API (Weight-basiert)
# Wir bleiben konservativ
REQ_PAUSE_SEC = 0.1

# Standard Trading Pairs
DEFAULT_SYMBOLS = ["ETHUSDT"]
# Kline/Candlestick Intervalle: 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M
DEFAULT_INTERVAL = "1m"
# Wie viele Candles pro Request (max 1000)
DEFAULT_LIMIT = 1000

# Zielpfad relativ zum Container
def out_path(execution_date_str: str, symbol: str, interval: str) -> Path:
    return Path(f"/opt/airflow/dags/data/binance_klines/{execution_date_str}/{symbol}-{interval}.jsonl")


@dag(
    start_date=pendulum.datetime(2025, 11, 1, tz="Europe/Paris"),
    schedule="@hourly",  # stündlich
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["binance", "api", "klines", "ingestion"],
    default_args=dict(retries=2, retry_delay=pendulum.duration(minutes=2)),
    description="Lädt Kline/Candlestick-Daten von der Binance API und speichert sie als JSONL-Dateien.",
)
def binance_api_fetch():
    @task
    def get_settings() -> Dict[str, Any]:
        """Liest Konfiguration aus Airflow Variables mit Fallback auf Defaults."""
        symbols_raw = Variable.get("BINANCE_SYMBOLS", default_var=",".join(DEFAULT_SYMBOLS))
        symbols = [s.strip().upper() for s in symbols_raw.split(",") if s.strip()]

        interval = Variable.get("BINANCE_INTERVAL", default_var=DEFAULT_INTERVAL)
        limit = int(Variable.get("BINANCE_LIMIT", default_var=str(DEFAULT_LIMIT)))

        return {
            "symbols": symbols,
            "interval": interval,
            "limit": min(limit, 1000),  # Binance max ist 1000
        }

    @task
    def get_last_timestamps(settings: Dict[str, Any]) -> Dict[str, int]:
        """
        Nutzt BINANCE_LAST_TIMESTAMP_{SYMBOL}_{INTERVAL} (Airflow Variable) als Startpunkt.
        Falls nicht vorhanden: hole die letzten 24 Stunden.
        """
        last_timestamps = {}
        for symbol in settings["symbols"]:
            var_name = f"BINANCE_LAST_TIMESTAMP_{symbol}_{settings['interval']}"
            last_ts = Variable.get(var_name, default_var=None)

            if last_ts is None:
                # Erster Lauf: hole Daten der letzten 24 Stunden
                last_timestamps[symbol] = int((pendulum.now("UTC").subtract(days=1).timestamp()) * 1000)
            else:
                last_timestamps[symbol] = int(last_ts)

        return last_timestamps

    @task
    def fetch_klines(settings: Dict[str, Any], last_timestamps: Dict[str, int], ds: str) -> Dict[str, Any]:
        """
        Holt Kline-Daten von der Binance API für alle konfigurierten Symbole
        und speichert sie als JSONL-Dateien (eine Zeile pro Candle).
        Binance API Endpoint: /api/v3/klines

        Returns: Statistiken über die abgerufenen Daten
        """
        stats = {
            "total_candles": 0,
            "per_symbol": {},
            "files": [],
            "last_timestamps": {},
        }

        ingest_ts = pendulum.now("UTC").to_iso8601_string()

        for symbol in settings["symbols"]:
            start_time = last_timestamps.get(symbol)
            if not start_time:
                # Fallback: letzter Tag
                start_time = int((pendulum.now("UTC").subtract(days=1).timestamp()) * 1000)

            candles_fetched = 0
            last_close_time = start_time

            # Erstelle Ausgabedatei
            out_file = out_path(ds, symbol, settings["interval"])
            out_file.parent.mkdir(parents=True, exist_ok=True)

            with out_file.open("a", encoding="utf-8") as f:
                # Hole Daten in Chunks (Binance gibt max 1000 Candles pro Request)
                while True:
                    params = {
                        "symbol": symbol,
                        "interval": settings["interval"],
                        "startTime": start_time,
                        "limit": settings["limit"],
                    }

                    try:
                        resp = requests.get(f"{BINANCE_BASE_URL}/api/v3/klines", params=params, timeout=30)

                        if resp.status_code == 429:
                            # Rate limit - warte und retry
                            time.sleep(2.0)
                            resp = requests.get(f"{BINANCE_BASE_URL}/api/v3/klines", params=params, timeout=30)

                        resp.raise_for_status()
                        klines = resp.json()

                        if not klines:
                            # Keine weiteren Daten
                            break

                        # Verarbeite jede Kline
                        for kline in klines:
                            # Binance Kline Format:
                            # [0] Open time, [1] Open, [2] High, [3] Low, [4] Close, [5] Volume,
                            # [6] Close time, [7] Quote asset volume, [8] Number of trades,
                            # [9] Taker buy base volume, [10] Taker buy quote volume, [11] Ignore

                            open_time_ms = int(kline[0])
                            close_time_ms = int(kline[6])

                            doc = {
                                "symbol": symbol,
                                "interval": settings["interval"],
                                "open_time_ms": open_time_ms,
                                "open_time_iso": pendulum.from_timestamp(open_time_ms / 1000, tz="UTC").to_iso8601_string(),
                                "close_time_ms": close_time_ms,
                                "close_time_iso": pendulum.from_timestamp(close_time_ms / 1000, tz="UTC").to_iso8601_string(),
                                "open": float(kline[1]),
                                "high": float(kline[2]),
                                "low": float(kline[3]),
                                "close": float(kline[4]),
                                "volume": float(kline[5]),
                                "quote_asset_volume": float(kline[7]),
                                "number_of_trades": int(kline[8]),
                                "taker_buy_base_volume": float(kline[9]),
                                "taker_buy_quote_volume": float(kline[10]),
                                "ingested_at": ingest_ts,
                            }

                            # Schreibe eine Zeile pro Candle
                            f.write(json.dumps(doc, ensure_ascii=False) + "\n")
                            candles_fetched += 1

                        # Aktualisiere start_time für nächsten Request
                        last_close_time = int(klines[-1][6])

                        # Wenn wir weniger als das Limit bekommen haben, sind wir am aktuellen Ende
                        if len(klines) < settings["limit"]:
                            break

                        # Ansonsten: nächster Chunk startet nach dem letzten close_time
                        start_time = last_close_time + 1

                        # Rate Limiting
                        time.sleep(REQ_PAUSE_SEC)

                    except requests.exceptions.RequestException as e:
                        print(f"Fehler beim Abrufen von {symbol}: {e}")
                        break

            stats["per_symbol"][symbol] = candles_fetched
            stats["total_candles"] += candles_fetched
            stats["files"].append(str(out_file))
            stats["last_timestamps"][symbol] = last_close_time

        return stats

    @task
    def update_checkpoints(stats: Dict[str, Any], settings: Dict[str, Any]) -> None:
        """Aktualisiert die Checkpoint-Variablen für jedes Symbol."""
        for symbol, last_ts in stats.get("last_timestamps", {}).items():
            var_name = f"BINANCE_LAST_TIMESTAMP_{symbol}_{settings['interval']}"
            Variable.set(var_name, str(last_ts))

    @task(trigger_rule="all_done")
    def log_summary(stats: Dict[str, Any], settings: Dict[str, Any]):
        """Gibt eine Zusammenfassung der abgerufenen Daten aus."""
        if stats["total_candles"] == 0:
            print("Keine neuen Candles in diesem Run.")
        else:
            print(f"Binance API Fetch abgeschlossen für Interval: {settings['interval']}")
            print(f"Gesamtzahl Candles: {stats['total_candles']}")
            for symbol, count in stats["per_symbol"].items():
                print(f"  {symbol}: {count} Candles")
            print(f"\nGespeicherte Dateien:")
            for file_path in stats["files"]:
                print(f"  {file_path}")

    settings = get_settings()
    last_timestamps = get_last_timestamps(settings)
    stats = fetch_klines(settings, last_timestamps, ds="{{ ds }}")
    update_checkpoints(stats, settings)
    log_summary(stats, settings)


binance_api_fetch()
