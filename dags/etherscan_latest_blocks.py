# dags/etherscan_latest_blocks.py
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Dict

import pendulum
import requests
try:
    from airflow.sdk.dag import dag
    from airflow.sdk.task import task
except ModuleNotFoundError:
    from airflow.decorators import dag, task
from airflow.models import Variable

# -------- Konfiguration --------
ETHERSCAN_BASE = "https://api.etherscan.io/v2/api"
ETH_CHAIN_ID = "1"  # Ethereum Mainnet
# Free-Tier Limit ~5 req/s -> wir bleiben konservativ bei 3-4 req/s
REQ_PAUSE_SEC = 0.3
# Fallback: Wieviele neueste Blöcke beim allerersten Lauf holen?
FALLBACK_LAST_N_BLOCKS = 1000

# Zielpfad relativ zum Container
def out_path(execution_date_str: str) -> Path:
    return Path(f"/opt/airflow/dags/data/ethereum_blocks/{execution_date_str}/blocks.jsonl")


@dag(
    start_date=pendulum.datetime(2025, 10, 1, tz="Europe/Paris"),
    schedule="* * * * *",  # minütlich
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["ethereum", "etherscan", "ingestion"],
    default_args=dict(retries=2, retry_delay=pendulum.duration(minutes=2)),
    description="Sammelt inkrementell die neuesten Ethereum-Blöcke von Etherscan und speichert sie als JSONL.",
)
def etherscan_latest_blocks():
    @task
    def get_api_key() -> str:
        api_key = Variable.get("ETHERSCAN_API_KEY", default_var=None)
        if not api_key:
            raise RuntimeError(
                "Airflow Variable 'ETHERSCAN_API_KEY' fehlt. Lege sie unter Admin → Variables an."
            )
        return api_key

    @task
    def get_latest_block(api_key: str) -> int:
        """
        Fragt die aktuelle Blocknummer (hex) ab und gibt sie als int zurück.
        Etherscan Proxy API: module=proxy&action=eth_blockNumber
        """
        params = {
            "module": "proxy",
            "action": "eth_blockNumber",
            "chainid": ETH_CHAIN_ID,
            "apikey": api_key,
        }
        r = requests.get(ETHERSCAN_BASE, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        # data["result"] ist hex, z.B. "0x12ab34..."
        latest_hex = data.get("result")
        if not latest_hex:
            raise RuntimeError(f"Unerwartete Antwort: {data}")
        latest_int = int(latest_hex, 16)
        return latest_int

    @task
    def compute_block_range(latest_block: int) -> Dict[str, int]:
        """
        Nutzt ETHERSCAN_LAST_BLOCK (Airflow Variable) als Startpunkt.
        Falls nicht vorhanden: hole die letzten FALLBACK_LAST_N_BLOCKS.
        """
        last_done = Variable.get("ETHERSCAN_LAST_BLOCK", default_var=None)
        if last_done is None:
            start = max(0, latest_block - FALLBACK_LAST_N_BLOCKS + 1)
        else:
            start = int(last_done) + 1

        if start > latest_block:
            # nichts Neues – Airflow mag leere Runs, aber wir kommunizieren es explizit
            return {"start": -1, "end": -1}
        return {"start": start, "end": latest_block}

    @task
    def fetch_blocks(api_key: str, block_range: Dict[str, int], ds: str) -> str:
        """
        Holt Blöcke in [start, end] (inkl.) und speichert sie als JSON Lines.
        Etherscan Proxy API: module=proxy&action=eth_getBlockByNumber&tag=0x...&boolean=true
        Rückgabe: Pfad zur JSONL-Datei (string)
        """
        start, end = block_range["start"], block_range["end"]
        if start == -1:
            return ""  # nichts zu tun

        out_file = out_path(ds)
        out_file.parent.mkdir(parents=True, exist_ok=True)

        with out_file.open("a", encoding="utf-8") as f:
            for number in range(start, end + 1):
                tag = hex(number)  # z.B. 0x1b4
                params = {
                    "module": "proxy",
                    "action": "eth_getBlockByNumber",
                    "tag": tag,
                    "boolean": "false",  # inkl. vollständiger Tx-Objekte
                    "chainid": ETH_CHAIN_ID,
                    "apikey": api_key,
                }
                resp = requests.get(ETHERSCAN_BASE, params=params, timeout=60)
                if resp.status_code == 429:
                    # Rate Limit getroffen – kurz warten und retry derselben Nummer
                    time.sleep(1.0)
                    resp = requests.get(ETHERSCAN_BASE, params=params, timeout=60)
                resp.raise_for_status()
                payload = resp.json()

                # Etherscan liefert {"jsonrpc":"2.0","id":..., "result": {...}} oder {"result": None} wenn noch nicht vorhanden
                block_obj = payload.get("result")
                if block_obj is None:
                    # z.B. Reorg / noch nicht verfügbar – wir loggen und überspringen
                    # (Alternativ: retry-Mechanik einbauen)
                    continue
                if not isinstance(block_obj, dict):
                    # Unerwartetes Format, z.B. Rate-Limit-Hinweis
                    print(f"Skip block {number} with unexpected result: {block_obj!r}")
                    continue

                # eine Zeile pro Block
                f.write(json.dumps(block_obj, ensure_ascii=False) + "\n")

                # sanft throttlen
                time.sleep(REQ_PAUSE_SEC)

        return str(out_file)

    @task
    def update_checkpoint(block_range: Dict[str, int]) -> None:
        start, end = block_range["start"], block_range["end"]
        if start == -1:
            return
        # setze den Checkpoint auf den zuletzt erfolgreich abgefragten Block
        Variable.set("ETHERSCAN_LAST_BLOCK", str(end))

    api_key = get_api_key()
    latest = get_latest_block(api_key)
    block_range = compute_block_range(latest)
    outfile = fetch_blocks(api_key, block_range, ds="{{ ds }}")
    update_checkpoint(block_range)

    # Optional: kleine Erfolgsmeldung ins Log (Airflow UI)
    @task(trigger_rule="all_done")
    def log_summary(path: str, block_range: Dict[str, int]):
        if not path:
            print("Keine neuen Blöcke in diesem Run.")
        else:
            print(f"Gespeichert: {path}")
            print(f"Blockbereich: {block_range['start']}..{block_range['end']}")

    log_summary(outfile, block_range)


etherscan_latest_blocks()
