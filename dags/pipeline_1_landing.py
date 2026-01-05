"""
Pipeline 1: Landing Zone
========================
Loads raw data (from API ingestion or sample files) into the Landing Zone.
Works OFFLINE with sample data (without API keys).

Data flow (granular steps):
1. Read: Reads JSON/JSONL files from filesystem
2. Validate: Validates JSON structure and required fields
3. Load: Writes validated data to Landing Zone databases

Landing Zones:
- MongoDB: binance_candles, etherscan_blocks
- Postgres: landing_coinmarketcap_raw

Input: JSON/JSONL files from data/
Output: Landing Zone databases
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import pendulum
from pymongo import MongoClient, UpdateOne

try:
    from airflow.sdk.dag import dag
    from airflow.sdk.task import task
except ModuleNotFoundError:
    from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable

# Input directories (from API ingestion or manually placed)
BINANCE_INPUT_DIR = Path("/opt/airflow/dags/data/binance_klines")
ETHERSCAN_INPUT_DIR = Path("/opt/airflow/dags/data/ethereum_blocks")
COINMARKETCAP_INPUT_DIR = Path("/opt/airflow/dags/data/coinmarketcap")

# MongoDB defaults
DEFAULT_MONGO_URI = "mongodb://root:example@mongodb:27017"
DEFAULT_MONGO_DB = "blockchain"
DEFAULT_BINANCE_COLLECTION = "binance_candles"
DEFAULT_ETHERSCAN_COLLECTION = "etherscan_blocks"

# Postgres defaults (for CoinMarketCap Landing)
DEFAULT_PG_HOST = "postgres"
DEFAULT_PG_PORT = 5432
DEFAULT_PG_DB = "airflow"
DEFAULT_PG_USER = "airflow"
DEFAULT_PG_PASSWORD = "airflow"

DEFAULT_BATCH_SIZE = 500


def _pg_connect_landing():
    """PostgreSQL connection for Landing Zone (Airflow DB)"""
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
    schedule="*/15 * * * *",  # Every 15 minutes
    catchup=False,
    tags=["pipeline-1", "landing", "ingestion"],
    default_args=dict(retries=2, retry_delay=pendulum.duration(minutes=2)),
    description="Pipeline 1: Loads raw data (API results or sample files) into Landing Zone (MongoDB/Postgres).",
)
def pipeline_1_landing():

    # ========== SETUP ==========

    @task
    def create_landing_tables() -> None:
        """Creates Landing Zone tables and indexes"""
        # MongoDB indexes
        mongo_uri = Variable.get("MONGO_URI", default_var=DEFAULT_MONGO_URI)
        mongo_db = Variable.get("MONGO_DB", default_var=DEFAULT_MONGO_DB)

        client = MongoClient(mongo_uri)
        try:
            # Binance Collection
            binance_coll = client[mongo_db][DEFAULT_BINANCE_COLLECTION]
            binance_coll.create_index([("symbol", 1), ("interval", 1), ("open_time_ms", 1)], unique=True)

            # Etherscan Collection
            etherscan_coll = client[mongo_db][DEFAULT_ETHERSCAN_COLLECTION]
            etherscan_coll.create_index("hash", unique=True, sparse=True)
            etherscan_coll.create_index("number", unique=True, sparse=True)

            print("✓ MongoDB: Indexes for Binance and Etherscan created")
        finally:
            client.close()

        # Postgres Landing table
        conn = _pg_connect_landing()
        try:
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
            print("✓ Postgres: landing_coinmarketcap_raw table created")
        finally:
            conn.close()

    # ========== BINANCE PIPELINE ==========

    @task
    def read_binance_files() -> List[Dict[str, Any]]:
        """Step 1: Reads Binance JSONL files and parses JSON"""
        input_dir = BINANCE_INPUT_DIR
        files = sorted(input_dir.rglob("*.jsonl"))

        if not files:
            raise AirflowSkipException(f"No Binance JSONL files found in {input_dir}")

        raw_documents: List[Dict[str, Any]] = []
        files_read = 0
        parse_errors = 0

        for file_path in files:
            try:
                with file_path.open("r", encoding="utf-8") as f:
                    for line_num, line in enumerate(f, 1):
                        line = line.strip()
                        if not line:
                            continue

                        try:
                            doc = json.loads(line)
                            if isinstance(doc, dict):
                                # Remember source file
                                doc["_source_file"] = str(file_path)
                                doc["_source_line"] = line_num
                                raw_documents.append(doc)
                            else:
                                parse_errors += 1
                        except json.JSONDecodeError:
                            parse_errors += 1
                            continue

                files_read += 1
                print(f"✓ Read: {file_path.name} ({len(raw_documents)} documents)")

            except Exception as e:
                print(f"✗ Error reading {file_path}: {e}")
                continue

        if not raw_documents:
            raise AirflowSkipException("No valid Binance documents found")

        print(f"📂 Binance: {files_read} files read, {len(raw_documents)} documents, {parse_errors} parse errors")
        return raw_documents

    @task
    def validate_binance_data(raw_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Step 2: Validates Binance data and required fields"""
        valid_documents: List[Dict[str, Any]] = []
        validation_errors = 0

        required_fields = ["symbol", "interval", "open_time_ms"]

        for doc in raw_data:
            # Required field check
            if not all(k in doc for k in required_fields):
                validation_errors += 1
                continue

            # Symbol and interval must be strings
            if not isinstance(doc.get("symbol"), str) or not isinstance(doc.get("interval"), str):
                validation_errors += 1
                continue

            # open_time_ms must be numeric
            try:
                int(doc["open_time_ms"])
            except (ValueError, TypeError):
                validation_errors += 1
                continue

            valid_documents.append(doc)

        if not valid_documents:
            raise AirflowSkipException("No valid Binance documents after validation")

        print(f"✅ Binance: {len(valid_documents)} validated documents, {validation_errors} validation errors")
        return {
            "valid_documents": valid_documents,
            "validation_errors": validation_errors,
            "total_input": len(raw_data)
        }

    @task
    def load_binance_to_mongodb(validated_result: Dict[str, Any]) -> Dict[str, Any]:
        """Step 3: Loads validated Binance data into MongoDB Landing Zone"""
        valid_documents = validated_result["valid_documents"]

        mongo_uri = Variable.get("MONGO_URI", default_var=DEFAULT_MONGO_URI)
        mongo_db = Variable.get("MONGO_DB", default_var=DEFAULT_MONGO_DB)
        collection_name = Variable.get("BINANCE_MONGO_COLLECTION", default_var=DEFAULT_BINANCE_COLLECTION)
        batch_size = int(Variable.get("LANDING_BATCH_SIZE", default_var=str(DEFAULT_BATCH_SIZE)))

        client = MongoClient(mongo_uri)
        collection = client[mongo_db][collection_name]

        try:
            batch: List[UpdateOne] = []
            rows_loaded = 0
            files_processed = set()

            for doc in valid_documents:
                # Upsert key (prevents duplicates)
                key = {
                    "symbol": doc["symbol"],
                    "interval": doc["interval"],
                    "open_time_ms": doc["open_time_ms"],
                }

                # Remove internal metadata
                source_file = doc.pop("_source_file", None)
                doc.pop("_source_line", None)

                if source_file:
                    files_processed.add(source_file)

                batch.append(UpdateOne(key, {"$set": doc}, upsert=True))
                rows_loaded += 1

                if len(batch) >= batch_size:
                    collection.bulk_write(batch, ordered=False)
                    batch.clear()

            # Write remaining batch
            if batch:
                collection.bulk_write(batch, ordered=False)
                batch.clear()

            print(f"💾 Binance → MongoDB: {rows_loaded} documents loaded into {collection_name}")
            return {
                "files_processed": len(files_processed),
                "rows_loaded": rows_loaded,
                "validation_errors": validated_result["validation_errors"]
            }

        finally:
            client.close()

    # ========== ETHERSCAN PIPELINE ==========

    @task
    def read_etherscan_files(ds: str) -> List[Dict[str, Any]]:
        """Step 1: Reads Ethereum block JSONL files and parses JSON"""
        input_dir = ETHERSCAN_INPUT_DIR
        files = sorted(input_dir.rglob("*.jsonl"))

        if not files:
            raise AirflowSkipException(f"No Etherscan JSONL files found in {input_dir}")

        raw_blocks: List[Dict[str, Any]] = []
        files_read = 0
        parse_errors = 0

        for file_path in files:
            try:
                with file_path.open("r", encoding="utf-8") as f:
                    for line_num, line in enumerate(f, 1):
                        line = line.strip()
                        if not line:
                            continue

                        try:
                            block = json.loads(line)
                            if isinstance(block, dict):
                                block["_source_file"] = str(file_path)
                                block["_source_line"] = line_num
                                block["_ingestion_ds"] = ds
                                raw_blocks.append(block)
                            else:
                                parse_errors += 1
                        except json.JSONDecodeError:
                            parse_errors += 1
                            continue

                files_read += 1
                print(f"✓ Read: {file_path.name} ({len(raw_blocks)} blocks)")

            except Exception as e:
                print(f"✗ Error reading {file_path}: {e}")
                continue

        if not raw_blocks:
            raise AirflowSkipException("No valid Ethereum blocks found")

        print(f"📂 Etherscan: {files_read} files read, {len(raw_blocks)} blocks, {parse_errors} parse errors")
        return raw_blocks

    @task
    def validate_etherscan_data(raw_blocks: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Step 2: Validates Ethereum blocks and converts hex fields"""
        valid_blocks: List[Dict[str, Any]] = []
        validation_errors = 0

        for block in raw_blocks:
            # Required fields
            block_hash = block.get("hash")
            number_hex = block.get("number")

            if not block_hash or not number_hex:
                validation_errors += 1
                continue

            try:
                # Hex → Integer conversion
                block_number = int(number_hex, 16) if number_hex else None
                ts_hex = block.get("timestamp")
                ts_unix = int(ts_hex, 16) if ts_hex else None

                if block_number is None:
                    validation_errors += 1
                    continue

                # Enriched document
                enriched_block = {
                    "hash": block_hash,
                    "number": block_number,
                    "number_hex": number_hex,
                    "timestamp_unix": ts_unix,
                    "tx_count": len(block.get("transactions") or []),
                    "raw": block,
                    "ingestion_ds": block.get("_ingestion_ds"),
                    "ingested_at": pendulum.now("UTC").to_iso8601_string(),
                    "_source_file": block.get("_source_file"),
                }

                valid_blocks.append(enriched_block)

            except (ValueError, TypeError) as e:
                validation_errors += 1
                continue

        if not valid_blocks:
            raise AirflowSkipException("No valid Ethereum blocks after validation")

        print(f"✅ Etherscan: {len(valid_blocks)} validated blocks, {validation_errors} validation errors")
        return {
            "valid_blocks": valid_blocks,
            "validation_errors": validation_errors,
            "total_input": len(raw_blocks)
        }

    @task
    def load_etherscan_to_mongodb(validated_result: Dict[str, Any]) -> Dict[str, Any]:
        """Step 3: Loads validated Ethereum blocks into MongoDB Landing Zone"""
        valid_blocks = validated_result["valid_blocks"]

        mongo_uri = Variable.get("MONGO_URI", default_var=DEFAULT_MONGO_URI)
        mongo_db = Variable.get("MONGO_DB", default_var=DEFAULT_MONGO_DB)
        collection_name = Variable.get("ETHERSCAN_MONGO_COLLECTION", default_var=DEFAULT_ETHERSCAN_COLLECTION)

        client = MongoClient(mongo_uri)
        collection = client[mongo_db][collection_name]

        try:
            operations: List[UpdateOne] = []
            files_processed = set()

            for block in valid_blocks:
                # Upsert key
                key = {"hash": block["hash"]}

                source_file = block.pop("_source_file", None)
                if source_file:
                    files_processed.add(source_file)

                operations.append(UpdateOne(key, {"$set": block}, upsert=True))

            # Write batch
            if operations:
                collection.bulk_write(operations, ordered=False)

            print(f"💾 Etherscan → MongoDB: {len(operations)} blocks loaded into {collection_name}")
            return {
                "files_processed": len(files_processed),
                "blocks_loaded": len(operations),
                "validation_errors": validated_result["validation_errors"]
            }

        finally:
            client.close()

    # ========== COINMARKETCAP PIPELINE ==========

    @task
    def read_coinmarketcap_files() -> List[Dict[str, Any]]:
        """Step 1: Reads CoinMarketCap JSON files"""
        input_dir = COINMARKETCAP_INPUT_DIR
        files = sorted(input_dir.rglob("*.json"))

        if not files:
            raise AirflowSkipException(f"No CoinMarketCap JSON files found in {input_dir}")

        file_data_list: List[Dict[str, Any]] = []
        files_read = 0
        parse_errors = 0

        for file_path in files:
            try:
                data = json.loads(file_path.read_text(encoding="utf-8"))
                file_data_list.append({
                    "file_path": str(file_path),
                    "data": data
                })
                files_read += 1
                print(f"✓ Read: {file_path.name}")

            except json.JSONDecodeError:
                parse_errors += 1
                print(f"✗ JSON parse error: {file_path}")
                continue
            except Exception as e:
                parse_errors += 1
                print(f"✗ Error reading {file_path}: {e}")
                continue

        if not file_data_list:
            raise AirflowSkipException("No valid CoinMarketCap files found")

        print(f"📂 CoinMarketCap: {files_read} files read, {parse_errors} parse errors")
        return file_data_list

    @task
    def validate_coinmarketcap_data(file_data_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Step 2: Validates CoinMarketCap data"""
        valid_files: List[Dict[str, Any]] = []
        validation_errors = 0

        for file_data in file_data_list:
            data = file_data["data"]

            # Check if API response is valid
            if not isinstance(data, dict):
                validation_errors += 1
                continue

            # CoinMarketCap API should have "data" or "status"
            if "data" not in data and "status" not in data:
                validation_errors += 1
                continue

            valid_files.append(file_data)

        if not valid_files:
            raise AirflowSkipException("No valid CoinMarketCap files after validation")

        print(f"✅ CoinMarketCap: {len(valid_files)} validated files, {validation_errors} validation errors")
        return {
            "valid_files": valid_files,
            "validation_errors": validation_errors,
            "total_input": len(file_data_list)
        }

    @task
    def load_coinmarketcap_to_postgres(validated_result: Dict[str, Any]) -> Dict[str, Any]:
        """Step 3: Loads validated CoinMarketCap data into Postgres Landing Zone"""
        valid_files = validated_result["valid_files"]

        conn = _pg_connect_landing()
        try:
            rows_loaded = 0
            files_processed = 0

            for file_data in valid_files:
                data = file_data["data"]
                file_path = file_data["file_path"]

                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO landing_coinmarketcap_raw (api_response, status_code, source_file)
                        VALUES (%s, %s, %s)
                        RETURNING id;
                        """,
                        (
                            json.dumps(data),
                            200,  # Assumption: successful
                            file_path,
                        ),
                    )
                conn.commit()

                rows_loaded += 1
                files_processed += 1
                print(f"✓ CoinMarketCap → Postgres: {Path(file_path).name}")

            print(f"💾 CoinMarketCap → Postgres: {rows_loaded} records loaded into landing_coinmarketcap_raw")
            return {
                "files_processed": files_processed,
                "rows_loaded": rows_loaded,
                "validation_errors": validated_result["validation_errors"]
            }

        finally:
            conn.close()

    # ========== SUMMARY ==========

    @task(trigger_rule="all_done")
    def log_summary(
        binance: Dict[str, Any], etherscan: Dict[str, Any], cmc: Dict[str, Any]
    ) -> None:
        """Logs summary of Pipeline 1"""
        print("=" * 70)
        print("Pipeline 1 (Landing Zone) - Summary:")
        print("=" * 70)
        print(f"📊 Binance → MongoDB:       {binance}")
        print(f"⛓️  Etherscan → MongoDB:     {etherscan}")
        print(f"💰 CoinMarketCap → Postgres: {cmc}")
        print("=" * 70)
        print("✅ Raw data successfully loaded into Landing Zone!")
        print("=" * 70)

    # ========== DAG WORKFLOW ==========

    # Setup
    tables = create_landing_tables()

    # Binance: Read → Validate → Load
    binance_raw = read_binance_files()
    binance_validated = validate_binance_data(binance_raw)
    binance_result = load_binance_to_mongodb(binance_validated)

    # Etherscan: Read → Validate → Load
    etherscan_raw = read_etherscan_files(ds="{{ ds }}")
    etherscan_validated = validate_etherscan_data(etherscan_raw)
    etherscan_result = load_etherscan_to_mongodb(etherscan_validated)

    # CoinMarketCap: Read → Validate → Load
    cmc_raw = read_coinmarketcap_files()
    cmc_validated = validate_coinmarketcap_data(cmc_raw)
    cmc_result = load_coinmarketcap_to_postgres(cmc_validated)

    # Summary
    summary = log_summary(binance_result, etherscan_result, cmc_result)

    # Dependencies
    tables >> [binance_raw, etherscan_raw, cmc_raw]
    [binance_result, etherscan_result, cmc_result] >> summary


pipeline_1_landing()
