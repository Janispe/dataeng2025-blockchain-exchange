"""
API Ingestion DAG
=================
Fetches data from external APIs and stores it as raw data (JSON/JSONL).
This DAG is OPTIONAL - it can be skipped when working with sample data.

APIs:
- Binance (kline/candle data)
- Etherscan (Ethereum block data)
- CoinMarketCap (cryptocurrency data)
"""
from __future__ import annotations

import json
import os
import re
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List

import pendulum
import requests

try:
    from airflow.sdk.dag import dag
    from airflow.sdk.task import task
except ModuleNotFoundError:
    from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable

# Storage locations (read by Pipeline 1)
BINANCE_OUTPUT_DIR = Path("/opt/airflow/dags/data/binance_klines")
ETHERSCAN_OUTPUT_DIR = Path("/opt/airflow/dags/data/ethereum_blocks")
COINMARKETCAP_OUTPUT_DIR = Path("/opt/airflow/dags/data/coinmarketcap")

# API endpoints
BINANCE_API_URL = "https://api.binance.com/api/v3/klines"
ETHERSCAN_API_URL = "https://api.etherscan.io/v2/api"
ETHERSCAN_CHAIN_ID = "1"  # Ethereum Mainnet
ETHERSCAN_REQ_PAUSE_SEC = 0.3
ETHERSCAN_FALLBACK_LAST_N_BLOCKS = 100
COINMARKETCAP_API_URL = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
COINMARKETCAP_HISTORICAL_API_URL = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/historical"

# Default symbols/parameters
DEFAULT_BINANCE_SYMBOLS = ["ETHUSDT", "BTCUSDT"]
DEFAULT_BINANCE_INTERVAL = "1m"
DEFAULT_BINANCE_LIMIT = 100

DEFAULT_COINMARKETCAP_LIMIT = 100


@dag(
    start_date=pendulum.datetime(2025, 10, 1, tz="Europe/Paris"),
    schedule="* * * * *",
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,  # paused by default (optional; requires API keys)
    tags=["api", "ingestion", "optional"],
    default_args=dict(retries=2, retry_delay=pendulum.duration(minutes=5)),
    description="API Ingestion: Ruft Daten von Binance, Etherscan und CoinMarketCap ab (optional, kann mit Sample-Daten übersprungen werden).",
)
def api_ingestion():
    @task
    def fetch_binance_klines(ds: str) -> Dict[str, Any]:
        """
        Fetches Binance kline data and stores it as JSONL (incremental).
        Uses per-symbol/interval Airflow Variables as checkpoints (last close_time_ms).
        Skips if the API is unavailable (or request fails).
        """
        symbols_raw = Variable.get("BINANCE_SYMBOLS", default_var=",".join(DEFAULT_BINANCE_SYMBOLS))
        symbols = [s.strip() for s in symbols_raw.split(",") if s.strip()]
        interval = Variable.get("BINANCE_INTERVAL", default_var=DEFAULT_BINANCE_INTERVAL)
        limit = int(Variable.get("BINANCE_LIMIT", default_var=str(DEFAULT_BINANCE_LIMIT)))

        output_dir = BINANCE_OUTPUT_DIR / ds
        output_dir.mkdir(parents=True, exist_ok=True)

        stats = {"symbols_processed": 0, "total_candles": 0, "files_created": []}

        def checkpoint_key(symbol: str, interval_value: str) -> str:
            sym = re.sub(r"[^A-Za-z0-9]+", "_", symbol).strip("_")
            itv = re.sub(r"[^A-Za-z0-9]+", "_", interval_value).strip("_")
            return f"BINANCE_LAST_KLINE_CLOSE_MS__{sym}__{itv}"

        for symbol in symbols:
            try:
                output_file = output_dir / f"{symbol}-{interval}.jsonl"
                ingestion_time = pendulum.now("UTC").to_iso8601_string()
                ck_key = checkpoint_key(symbol, interval)
                last_close_ms_raw = Variable.get(ck_key, default_var=None)
                start_time_ms = int(last_close_ms_raw) + 1 if last_close_ms_raw is not None else None

                total_new = 0
                last_written_close_ms: int | None = None
                max_pages = 1000

                mode = "a" if start_time_ms is not None else "w"
                with output_file.open(mode, encoding="utf-8") as f:
                    for _ in range(max_pages):
                        now_ms = int(time.time() * 1000)
                        params: Dict[str, Any] = {"symbol": symbol, "interval": interval, "limit": limit}
                        if start_time_ms is not None:
                            params["startTime"] = start_time_ms

                        response = requests.get(BINANCE_API_URL, params=params, timeout=30)
                        response.raise_for_status()
                        klines = response.json() or []
                        if not klines:
                            break

                        for kline in klines:
                            if len(kline) < 7:
                                continue
                            close_time_ms = int(kline[6])
                            # Don't checkpoint / persist an open candle.
                            if close_time_ms > now_ms:
                                continue
                            doc = {
                                "symbol": symbol,
                                "interval": interval,
                                "open_time_ms": int(kline[0]),
                                "open": float(kline[1]),
                                "high": float(kline[2]),
                                "low": float(kline[3]),
                                "close": float(kline[4]),
                                "volume": float(kline[5]),
                                "close_time_ms": close_time_ms,
                                "quote_asset_volume": float(kline[7]) if len(kline) > 7 else None,
                                "number_of_trades": int(kline[8]) if len(kline) > 8 else None,
                                "taker_buy_base_volume": float(kline[9]) if len(kline) > 9 else None,
                                "taker_buy_quote_volume": float(kline[10]) if len(kline) > 10 else None,
                                "trading_day": ds,
                                "ingested_at": ingestion_time,
                            }
                            f.write(json.dumps(doc, ensure_ascii=False) + "\n")
                            total_new += 1
                            last_written_close_ms = close_time_ms

                        if last_written_close_ms is not None:
                            start_time_ms = last_written_close_ms + 1
                        else:
                            # Only open candles returned; wait for the next run.
                            break
                        if len(klines) < limit:
                            break

                if total_new > 0 and last_written_close_ms is not None:
                    Variable.set(ck_key, str(last_written_close_ms))
                    stats["symbols_processed"] += 1
                    stats["total_candles"] += total_new
                    stats["files_created"].append(str(output_file))
                    print(f"✓ Binance: {symbol} - {total_new} neue Candles → {output_file}")
                else:
                    print(f"• Binance: {symbol} - keine neuen Candles (Checkpoint: {last_close_ms_raw})")

            except requests.exceptions.RequestException as e:
                print(f"✗ Binance API-Fehler für {symbol}: {e}")
                continue

        if stats["symbols_processed"] == 0:
            raise AirflowSkipException("Keine Binance-Daten abgerufen (API-Fehler oder kein API-Key)")

        return stats

    @task
    def fetch_etherscan_blocks(ds: str) -> Dict[str, Any]:
        """
        Fetches Ethereum block data from Etherscan (incremental).
        Uses ETHERSCAN_LAST_BLOCK (Airflow Variable) as a checkpoint.
        """
        api_key = Variable.get("ETHERSCAN_API_KEY", default_var="")
        if not api_key or api_key == "***":
            raise AirflowSkipException("ETHERSCAN_API_KEY nicht konfiguriert - überspringe API-Aufruf")

        try:
            # Fetch latest block number
            response = requests.get(
                ETHERSCAN_API_URL,
                params={
                    "module": "proxy",
                    "action": "eth_blockNumber",
                    "chainid": ETHERSCAN_CHAIN_ID,
                    "apikey": api_key,
                },
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            latest_block_hex = data.get("result")
            if not isinstance(latest_block_hex, str) or not latest_block_hex.startswith("0x"):
                raise AirflowSkipException(
                    f"Etherscan eth_blockNumber lieferte kein Hex-Result (API-Key/Rate-Limit/Endpoint?): {data}"
                )

            try:
                latest_block = int(latest_block_hex, 16)
            except ValueError as e:
                raise AirflowSkipException(
                    f"Etherscan eth_blockNumber konnte nicht geparst werden: {latest_block_hex}"
                ) from e

            last_done = Variable.get("ETHERSCAN_LAST_BLOCK", default_var=None)
            if last_done is None:
                start_block = max(0, latest_block - ETHERSCAN_FALLBACK_LAST_N_BLOCKS + 1)
            else:
                start_block = int(last_done) + 1

            if start_block > latest_block:
                return {"start": -1, "end": -1, "blocks_fetched": 0, "file": ""}

            output_dir = ETHERSCAN_OUTPUT_DIR / ds
            output_dir.mkdir(parents=True, exist_ok=True)
            output_file = output_dir / "blocks.jsonl"

            blocks_fetched = 0
            with output_file.open("a", encoding="utf-8") as f:
                for block_num in range(start_block, latest_block + 1):
                    params = {
                        "module": "proxy",
                        "action": "eth_getBlockByNumber",
                        "tag": hex(block_num),
                        "boolean": "false",
                        "chainid": ETHERSCAN_CHAIN_ID,
                        "apikey": api_key,
                    }
                    resp = requests.get(ETHERSCAN_API_URL, params=params, timeout=60)
                    if resp.status_code == 429:
                        time.sleep(1.0)
                        resp = requests.get(ETHERSCAN_API_URL, params=params, timeout=60)
                    resp.raise_for_status()
                    payload = resp.json()

                    block_obj = payload.get("result")
                    if block_obj is None:
                        continue
                    if not isinstance(block_obj, dict):
                        print(f"Skip block {block_num} with unexpected result: {block_obj!r}")
                        continue

                    f.write(json.dumps(block_obj, ensure_ascii=False) + "\n")
                    blocks_fetched += 1
                    time.sleep(ETHERSCAN_REQ_PAUSE_SEC)

            Variable.set("ETHERSCAN_LAST_BLOCK", str(latest_block))

            print(f"✓ Etherscan: {blocks_fetched} Blöcke ({start_block}..{latest_block}) → {output_file}")
            return {
                "start": start_block,
                "end": latest_block,
                "blocks_fetched": blocks_fetched,
                "file": str(output_file),
            }

        except requests.exceptions.RequestException as e:
            raise AirflowSkipException(f"Etherscan API-Fehler: {e}")

    @task
    def fetch_coinmarketcap_data(ds: str) -> Dict[str, Any]:
        """
        Fetches CoinMarketCap data.
        Ensures per-minute snapshots without Airflow catchup:
        - Uses COINMARKETCAP_LAST_MINUTE_UTC as a checkpoint (ISO timestamp, minute precision).
        - On first run (no checkpoint), backfills COINMARKETCAP_BACKFILL_MINUTES minutes (default 0).
        - On subsequent runs, fetches missing minutes since the checkpoint (capped by COINMARKETCAP_MAX_CATCHUP_MINUTES).
        Skips if the API key is missing/invalid.
        """
        # Prefer env to avoid stale DB Variables in local dev (AIRFLOW_VAR_* is an Airflow convention)
        api_key = (
            os.getenv("COINMARKETCAP_API_KEY")
            or os.getenv("AIRFLOW_VAR_COINMARKETCAP_API_KEY")
            or Variable.get("COINMARKETCAP_API_KEY", default_var=None)
        )
        if not api_key or api_key == "***":
            raise AirflowSkipException("COINMARKETCAP_API_KEY nicht konfiguriert - überspringe API-Aufruf")

        limit = int(Variable.get("COINMARKETCAP_LIMIT", default_var=str(DEFAULT_COINMARKETCAP_LIMIT)))
        try:
            initial_backfill_minutes = max(0, int(Variable.get("COINMARKETCAP_BACKFILL_MINUTES", default_var="0")))
        except (TypeError, ValueError):
            initial_backfill_minutes = 0
        try:
            max_catchup_minutes = max(1, int(Variable.get("COINMARKETCAP_MAX_CATCHUP_MINUTES", default_var="120")))
        except (TypeError, ValueError):
            max_catchup_minutes = 120

        try:
            headers = {
                "Accepts": "application/json",
                "X-CMC_PRO_API_KEY": api_key,
                "User-Agent": "dataeng2025-blockchain-exchange/airflow",
            }
            base_params = {"start": "1", "limit": str(limit), "convert": "USD"}

            checkpoint_raw = Variable.get("COINMARKETCAP_LAST_MINUTE_UTC", default_var=None)
            checkpoint_minute: pendulum.DateTime | None = None
            if checkpoint_raw:
                try:
                    checkpoint_minute = pendulum.parse(str(checkpoint_raw)).in_timezone("UTC").start_of("minute")
                except Exception:
                    checkpoint_minute = None

            end_minute = pendulum.now("UTC").subtract(minutes=1).start_of("minute")
            if checkpoint_minute is None:
                start_minute = (
                    end_minute.subtract(minutes=max(0, initial_backfill_minutes - 1))
                    if initial_backfill_minutes
                    else end_minute
                )
            else:
                start_minute = checkpoint_minute.add(minutes=1)

            if start_minute > end_minute:
                return {
                    "crypto_count": 0,
                    "files_created": [],
                    "checkpoint": checkpoint_raw,
                    "note": "No new minutes to fetch",
                }

            total_minutes = int((end_minute - start_minute).total_seconds() // 60) + 1
            if total_minutes > max_catchup_minutes:
                end_minute = start_minute.add(minutes=max_catchup_minutes - 1)
                total_minutes = max_catchup_minutes

            files_created: List[str] = []
            crypto_count = 0
            last_success_minute: pendulum.DateTime | None = None

            # Try historical per-minute snapshots; if the plan/key forbids it (403), fall back to `listings/latest`.
            mode = str(Variable.get("COINMARKETCAP_MODE", default_var="historical")).strip().lower()
            if mode not in {"historical", "latest"}:
                mode = "historical"

            minutes_to_fetch = [start_minute.add(minutes=i) for i in range(total_minutes)]
            for idx, minute in enumerate(minutes_to_fetch):
                minute_ds = minute.to_date_string()
                minute_label = minute.format("YYYYMMDDTHHmm")
                out_dir = COINMARKETCAP_OUTPUT_DIR / minute_ds
                out_dir.mkdir(parents=True, exist_ok=True)
                out_file = out_dir / f"listings_{minute_label}.json"

                if out_file.exists():
                    last_success_minute = minute
                    continue

                if mode == "historical":
                    params = dict(
                        base_params,
                        time_start=minute.to_iso8601_string(),
                        time_end=minute.add(minutes=1).subtract(seconds=1).to_iso8601_string(),
                    )
                    resp = requests.get(COINMARKETCAP_HISTORICAL_API_URL, headers=headers, params=params, timeout=30)
                    try:
                        resp.raise_for_status()
                    except requests.exceptions.HTTPError:
                        status = getattr(resp, "status_code", None)
                        if status in {401, 403}:
                            # Typically means: invalid key (401) or key has no access to historical endpoint (403)
                            mode = "latest"
                            minute = end_minute
                            minute_ds = minute.to_date_string()
                            minute_label = minute.format("YYYYMMDDTHHmm")
                            out_dir = COINMARKETCAP_OUTPUT_DIR / minute_ds
                            out_dir.mkdir(parents=True, exist_ok=True)
                            out_file = out_dir / f"listings_{minute_label}.json"
                            if out_file.exists():
                                last_success_minute = minute
                                break
                            resp = requests.get(COINMARKETCAP_API_URL, headers=headers, params=base_params, timeout=30)
                            resp.raise_for_status()
                            payload = resp.json()
                            out_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")
                            files_created.append(str(out_file))
                            last_success_minute = minute
                            total_minutes = 1
                            break
                        raise
                    payload = resp.json()
                else:
                    resp = requests.get(COINMARKETCAP_API_URL, headers=headers, params=base_params, timeout=30)
                    resp.raise_for_status()
                    payload = resp.json()

                out_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")
                files_created.append(str(out_file))
                last_success_minute = minute

            if last_success_minute is not None:
                last_file = (
                    COINMARKETCAP_OUTPUT_DIR
                    / last_success_minute.to_date_string()
                    / f"listings_{last_success_minute.format('YYYYMMDDTHHmm')}.json"
                )
                try:
                    last_payload = json.loads(last_file.read_text(encoding="utf-8"))
                    if isinstance(last_payload, dict):
                        crypto_count = len(last_payload.get("data", []) or [])
                except Exception:
                    crypto_count = 0

            if last_success_minute is not None:
                Variable.set("COINMARKETCAP_LAST_MINUTE_UTC", last_success_minute.to_iso8601_string())

            print(
                f"✓ CoinMarketCap ({mode}): {total_minutes} minute(s) fetched ({start_minute.to_iso8601_string()}..{end_minute.to_iso8601_string()})"
            )

            return {
                "crypto_count": crypto_count,
                "files_created": files_created,
                "minutes_fetched": total_minutes,
                "checkpoint": last_success_minute.to_iso8601_string() if last_success_minute else checkpoint_raw,
            }

        except requests.exceptions.RequestException as e:
            raise AirflowSkipException(f"CoinMarketCap API-Fehler: {e}")

    @task(trigger_rule="all_done")
    def log_summary(binance: Dict[str, Any], etherscan: Dict[str, Any], cmc: Dict[str, Any]) -> None:
        """Logs a summary of the API calls."""
        print("=" * 60)
        print("API Ingestion Summary:")
        print("=" * 60)
        print(f"Binance: {binance}")
        print(f"Etherscan: {etherscan}")
        print(f"CoinMarketCap: {cmc}")
        print("=" * 60)
        print("Hinweis: Diese Daten werden von Pipeline 1 (Landing) gelesen.")
        print("=" * 60)

    binance_stats = fetch_binance_klines(ds="{{ ds }}")
    etherscan_stats = fetch_etherscan_blocks(ds="{{ ds }}")
    cmc_stats = fetch_coinmarketcap_data(ds="{{ ds }}")

    log_summary(binance_stats, etherscan_stats, cmc_stats)


api_ingestion()
