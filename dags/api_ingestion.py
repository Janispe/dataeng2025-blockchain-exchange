"""
API Ingestion DAG
=================
Fetches data from external APIs and stores it as raw data (JSON/JSONL).
This DAG is OPTIONAL - can be skipped when working with sample data.

APIs:
- Binance (Kline/Candle data) - runs every minute
- Etherscan (Ethereum block data) - runs every minute
- CoinMarketCap (Cryptocurrency data) - runs every 15 minutes (Smart Skip)

Scheduling Strategy:
--------------------
- DAG schedule: Every minute (*/1 * * * *)
- Binance & Etherscan: Execute on every DAG run (every minute)
- CoinMarketCap: Uses "Smart Skip" to only run every 15 minutes
  - Checks COINMARKETCAP_LAST_RUN Airflow Variable
  - Skips if less than 15 minutes since last successful run
  - Respects API rate limits: ~96 calls/day (within 333/day free tier)
"""
from __future__ import annotations

import json
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

# Data storage locations (read by Pipeline 1)
BINANCE_OUTPUT_DIR = Path("/opt/airflow/dags/data/binance_klines")
ETHERSCAN_OUTPUT_DIR = Path("/opt/airflow/dags/data/ethereum_blocks")
COINMARKETCAP_OUTPUT_DIR = Path("/opt/airflow/dags/data/coinmarketcap")

# API endpoints
BINANCE_API_URL = "https://api.binance.com/api/v3/klines"
ETHERSCAN_API_URL = "https://api.etherscan.io/api"
COINMARKETCAP_API_URL = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"

# Default symbols/parameters
DEFAULT_BINANCE_SYMBOLS = ["ETHUSDT", "BTCUSDT"]
DEFAULT_BINANCE_INTERVAL = "1h"
DEFAULT_BINANCE_LIMIT = 100

DEFAULT_COINMARKETCAP_LIMIT = 100


@dag(
    start_date=pendulum.datetime(2025, 10, 1, tz="Europe/Paris"),
    schedule="*/1 * * * *",  # Every minute (Binance + Etherscan), CoinMarketCap uses Smart Skip (every 15min)
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,  # Paused by default - optional, only with API keys
    tags=["api", "ingestion", "optional"],
    default_args=dict(retries=2, retry_delay=pendulum.duration(minutes=5)),
    description="API Ingestion: Fetches data from Binance, Etherscan and CoinMarketCap (optional, can be skipped with sample data).",
)
def api_ingestion():
    @task
    def fetch_binance_klines(ds: str) -> Dict[str, Any]:
        """
        Fetches Binance kline data and stores it as JSONL.
        Skips if API key is missing/invalid.
        """
        symbols_raw = Variable.get("BINANCE_SYMBOLS", default_var=",".join(DEFAULT_BINANCE_SYMBOLS))
        symbols = [s.strip() for s in symbols_raw.split(",") if s.strip()]
        interval = Variable.get("BINANCE_INTERVAL", default_var=DEFAULT_BINANCE_INTERVAL)
        limit = int(Variable.get("BINANCE_LIMIT", default_var=str(DEFAULT_BINANCE_LIMIT)))

        output_dir = BINANCE_OUTPUT_DIR / ds
        output_dir.mkdir(parents=True, exist_ok=True)

        stats = {"symbols_processed": 0, "total_candles": 0, "files_created": []}

        for symbol in symbols:
            try:
                params = {
                    "symbol": symbol,
                    "interval": interval,
                    "limit": limit,
                }
                response = requests.get(BINANCE_API_URL, params=params, timeout=30)
                response.raise_for_status()
                klines = response.json()

                # Save as JSONL
                output_file = output_dir / f"{symbol}-{interval}.jsonl"
                ingestion_time = pendulum.now("UTC").to_iso8601_string()

                with output_file.open("w", encoding="utf-8") as f:
                    for kline in klines:
                        # Binance kline format: [open_time, open, high, low, close, volume, close_time, ...]
                        if len(kline) < 7:
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
                            "close_time_ms": int(kline[6]),
                            "quote_asset_volume": float(kline[7]) if len(kline) > 7 else None,
                            "number_of_trades": int(kline[8]) if len(kline) > 8 else None,
                            "taker_buy_base_volume": float(kline[9]) if len(kline) > 9 else None,
                            "taker_buy_quote_volume": float(kline[10]) if len(kline) > 10 else None,
                            "trading_day": ds,
                            "ingested_at": ingestion_time,
                        }
                        f.write(json.dumps(doc) + "\n")

                stats["symbols_processed"] += 1
                stats["total_candles"] += len(klines)
                stats["files_created"].append(str(output_file))
                print(f"✓ Binance: {symbol} - {len(klines)} Candles → {output_file}")

            except requests.exceptions.RequestException as e:
                print(f"✗ Binance API error for {symbol}: {e}")
                continue

        if stats["symbols_processed"] == 0:
            raise AirflowSkipException("No Binance data fetched (API error or no API key)")

        return stats

    @task
    def fetch_etherscan_blocks(ds: str) -> Dict[str, Any]:
        """
        Fetches Ethereum block data from Etherscan.
        Skips if API key is missing/invalid.
        """
        api_key = Variable.get("ETHERSCAN_API_KEY", default_var="")
        if not api_key or api_key == "***":
            raise AirflowSkipException("ETHERSCAN_API_KEY not configured - skipping API call")

        output_dir = ETHERSCAN_OUTPUT_DIR / ds
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / "blocks.jsonl"

        # Get latest block number
        try:
            response = requests.get(
                ETHERSCAN_API_URL,
                params={
                    "module": "proxy",
                    "action": "eth_blockNumber",
                    "apikey": api_key,
                },
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            latest_block_hex = data.get("result")
            if not latest_block_hex:
                raise AirflowSkipException("No block number received from Etherscan")

            latest_block = int(latest_block_hex, 16)
            start_block = max(0, latest_block - 10)  # Get last 10 blocks

            blocks_fetched = 0
            ingestion_time = pendulum.now("UTC").to_iso8601_string()

            with output_file.open("w", encoding="utf-8") as f:
                for block_num in range(start_block, latest_block + 1):
                    try:
                        response = requests.get(
                            ETHERSCAN_API_URL,
                            params={
                                "module": "proxy",
                                "action": "eth_getBlockByNumber",
                                "tag": hex(block_num),
                                "boolean": "true",
                                "apikey": api_key,
                            },
                            timeout=30,
                        )
                        response.raise_for_status()
                        block = response.json().get("result")
                        if block:
                            f.write(json.dumps(block) + "\n")
                            blocks_fetched += 1
                    except Exception as e:
                        print(f"Error fetching block {block_num}: {e}")
                        continue

            if blocks_fetched == 0:
                raise AirflowSkipException("No Ethereum blocks fetched")

            print(f"✓ Etherscan: {blocks_fetched} blocks → {output_file}")
            return {"blocks_fetched": blocks_fetched, "file": str(output_file)}

        except requests.exceptions.RequestException as e:
            raise AirflowSkipException(f"Etherscan API error: {e}")

    @task
    def fetch_coinmarketcap_data(ds: str) -> Dict[str, Any]:
        """
        Fetches CoinMarketCap data.
        Skips if API key is missing/invalid.
        Uses Smart Skip: Only runs every 15 minutes (even though DAG runs every minute).
        """
        # Smart Skip: Check if 15 minutes have passed since last run
        last_run_str = Variable.get("COINMARKETCAP_LAST_RUN", default_var="")
        if last_run_str:
            try:
                last_run = pendulum.parse(last_run_str)
                now = pendulum.now("UTC")
                minutes_since_last_run = (now - last_run).total_minutes()

                if minutes_since_last_run < 15:
                    raise AirflowSkipException(
                        f"CoinMarketCap: Skipping - only {minutes_since_last_run:.1f} minutes since last run "
                        f"(requires 15 minutes). Last run: {last_run_str}"
                    )
            except Exception as e:
                print(f"Warning: Could not parse COINMARKETCAP_LAST_RUN timestamp: {e}")

        api_key = Variable.get("COINMARKETCAP_API_KEY", default_var="")
        if not api_key:
            raise AirflowSkipException("COINMARKETCAP_API_KEY not configured - skipping API call")

        output_dir = COINMARKETCAP_OUTPUT_DIR / ds
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / "listings.json"

        limit = int(Variable.get("COINMARKETCAP_LIMIT", default_var=str(DEFAULT_COINMARKETCAP_LIMIT)))

        try:
            headers = {
                "Accepts": "application/json",
                "X-CMC_PRO_API_KEY": api_key,
            }
            params = {
                "start": "1",
                "limit": str(limit),
                "convert": "USD",
            }

            response = requests.get(COINMARKETCAP_API_URL, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            # Save complete response
            output_file.write_text(json.dumps(data, indent=2), encoding="utf-8")

            crypto_count = len(data.get("data", []))
            print(f"✓ CoinMarketCap: {crypto_count} cryptocurrencies → {output_file}")

            # Update last run timestamp for Smart Skip
            now = pendulum.now("UTC").to_iso8601_string()
            Variable.set("COINMARKETCAP_LAST_RUN", now)
            print(f"✓ Updated COINMARKETCAP_LAST_RUN to {now}")

            return {
                "crypto_count": crypto_count,
                "file": str(output_file),
                "status_code": response.status_code,
            }

        except requests.exceptions.RequestException as e:
            raise AirflowSkipException(f"CoinMarketCap API error: {e}")

    @task(trigger_rule="all_done")
    def log_summary(binance: Dict[str, Any], etherscan: Dict[str, Any], cmc: Dict[str, Any]) -> None:
        """Logs summary of API calls"""
        print("=" * 60)
        print("API Ingestion Summary:")
        print("=" * 60)
        print(f"Binance: {binance}")
        print(f"Etherscan: {etherscan}")
        print(f"CoinMarketCap: {cmc}")
        print("=" * 60)
        print("Note: This data will be read by Pipeline 1 (Landing).")
        print("=" * 60)

    binance_stats = fetch_binance_klines(ds="{{ ds }}")
    etherscan_stats = fetch_etherscan_blocks(ds="{{ ds }}")
    cmc_stats = fetch_coinmarketcap_data(ds="{{ ds }}")

    log_summary(binance_stats, etherscan_stats, cmc_stats)


api_ingestion()
