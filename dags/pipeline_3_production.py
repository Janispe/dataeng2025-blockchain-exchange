from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pendulum

try:
    from airflow.sdk.dag import dag
    from airflow.sdk.task import task
except ModuleNotFoundError:
    from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from pipeline_datasets import DW_DATASET, STAGING_DATASET

# Dedicated project Postgres (compose.yml: service "postgres-data")
DEFAULT_PG_HOST = "postgres-data"
DEFAULT_PG_PORT = 5432
DEFAULT_PG_DB = "datadb"
DEFAULT_PG_USER = "datauser"
DEFAULT_PG_PASSWORD = "datapass"

DW_SCHEMA = "dw"
STAGING_SCHEMA = "staging"

BINANCE_EXCHANGE_NAME = "Binance"
ETHEREUM_CHAIN_NAME = "Ethereum"
ETHEREUM_CHAIN_ID = 1

DEFAULT_BATCH_SIZE_CANDLES = 2000
DEFAULT_BATCH_SIZE_BLOCKS = 500
DEFAULT_BATCH_SIZE_CMC = 2000

CHECKPOINT_BINANCE_VAR = "DW_LAST_BINANCE_STAGING_AT"
CHECKPOINT_ETHERSCAN_VAR = "DW_LAST_ETHERSCAN_STAGING_AT"
CHECKPOINT_CMC_VAR = "DW_LAST_CMC_METRICS_ID"


def _pg_connect(settings: "Settings"):
    try:
        import psycopg2  # type: ignore

        return psycopg2.connect(
            host=settings.pg_host,
            port=settings.pg_port,
            dbname=settings.pg_db,
            user=settings.pg_user,
            password=settings.pg_password,
        )
    except ModuleNotFoundError:
        import psycopg  # type: ignore

        return psycopg.connect(
            host=settings.pg_host,
            port=settings.pg_port,
            dbname=settings.pg_db,
            user=settings.pg_user,
            password=settings.pg_password,
        )


def _chunks(seq: Sequence[Any], size: int) -> Iterable[Sequence[Any]]:
    for idx in range(0, len(seq), size):
        yield seq[idx : idx + size]


def _parse_hex_int(value: Any) -> Optional[int]:
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


KNOWN_QUOTES = ["USDT", "USDC", "BUSD", "USD", "EUR", "BTC", "ETH", "BNB", "TRY"]


def _split_symbol(symbol: str) -> Tuple[Optional[str], Optional[str]]:
    sym = (symbol or "").strip().upper()
    for quote in sorted(KNOWN_QUOTES, key=len, reverse=True):
        if sym.endswith(quote) and len(sym) > len(quote):
            return sym[: -len(quote)], quote
    return None, None


def _interval_to_seconds(interval: Optional[str]) -> Optional[int]:
    if not interval:
        return None
    s = interval.strip().lower()
    if len(s) < 2:
        return None
    num_part, unit = s[:-1], s[-1]
    try:
        n = int(num_part)
    except ValueError:
        return None
    factor = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}.get(unit)
    return n * factor if factor else None


def _to_utc_dt_from_ms(ms: Any) -> Optional[datetime]:
    try:
        ms_int = int(ms)
    except (TypeError, ValueError):
        return None
    return datetime.fromtimestamp(ms_int / 1000.0, tz=timezone.utc)


def _to_utc_dt_from_unix_seconds(sec: Any) -> Optional[datetime]:
    try:
        sec_int = int(sec)
    except (TypeError, ValueError):
        return None
    return datetime.fromtimestamp(sec_int, tz=timezone.utc)


def _time_dim_row(ts_utc: datetime) -> Tuple[Any, ...]:
    # Postgres-style day-of-week: Sunday=0 .. Saturday=6
    py_weekday = ts_utc.weekday()  # Monday=0..Sunday=6
    day_of_week = (py_weekday + 1) % 7
    iso = ts_utc.isocalendar()
    quarter = (ts_utc.month - 1) // 3 + 1
    return (
        ts_utc,
        ts_utc.date(),
        ts_utc.year,
        quarter,
        ts_utc.month,
        ts_utc.day,
        ts_utc.hour,
        ts_utc.minute,
        day_of_week,
        iso.week,
        day_of_week in (0, 6),
    )


def _ensure_dw_schema(conn) -> None:
    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS {DW_SCHEMA};

    CREATE TABLE IF NOT EXISTS {DW_SCHEMA}.dim_time (
      time_key BIGSERIAL PRIMARY KEY,
      ts_utc TIMESTAMPTZ NOT NULL UNIQUE,
      date DATE NOT NULL,
      year SMALLINT NOT NULL,
      quarter SMALLINT NOT NULL,
      month SMALLINT NOT NULL,
      day SMALLINT NOT NULL,
      hour SMALLINT NOT NULL,
      minute SMALLINT NOT NULL,
      day_of_week SMALLINT NOT NULL,
      week_of_year SMALLINT NOT NULL,
      is_weekend BOOLEAN NOT NULL
    );

    CREATE TABLE IF NOT EXISTS {DW_SCHEMA}.dim_exchange (
      exchange_key SERIAL PRIMARY KEY,
      exchange_name TEXT NOT NULL UNIQUE
    );

    CREATE TABLE IF NOT EXISTS {DW_SCHEMA}.dim_asset (
      asset_key SERIAL PRIMARY KEY,
      symbol TEXT NOT NULL UNIQUE,
      base_asset TEXT,
      quote_asset TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS {DW_SCHEMA}.dim_interval (
      interval_key SERIAL PRIMARY KEY,
      interval TEXT NOT NULL UNIQUE,
      interval_seconds INTEGER
    );

    CREATE TABLE IF NOT EXISTS {DW_SCHEMA}.dim_chain (
      chain_key SERIAL PRIMARY KEY,
      chain_name TEXT NOT NULL UNIQUE,
      chain_id INTEGER UNIQUE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS {DW_SCHEMA}.fact_binance_candle (
      candle_key BIGSERIAL PRIMARY KEY,
      exchange_key INTEGER NOT NULL REFERENCES {DW_SCHEMA}.dim_exchange(exchange_key),
      asset_key INTEGER NOT NULL REFERENCES {DW_SCHEMA}.dim_asset(asset_key),
      interval_key INTEGER NOT NULL REFERENCES {DW_SCHEMA}.dim_interval(interval_key),
      open_time_key BIGINT NOT NULL REFERENCES {DW_SCHEMA}.dim_time(time_key),
      close_time_key BIGINT NOT NULL REFERENCES {DW_SCHEMA}.dim_time(time_key),
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

      UNIQUE (exchange_key, asset_key, interval_key, open_time_key)
    );

    CREATE INDEX IF NOT EXISTS ix_fact_binance_candle_open_time_key
      ON {DW_SCHEMA}.fact_binance_candle (open_time_key);

    CREATE TABLE IF NOT EXISTS {DW_SCHEMA}.fact_ethereum_block (
      block_key BIGSERIAL PRIMARY KEY,
      chain_key INTEGER NOT NULL REFERENCES {DW_SCHEMA}.dim_chain(chain_key),
      block_time_key BIGINT NOT NULL REFERENCES {DW_SCHEMA}.dim_time(time_key),
      block_number BIGINT,
      block_hash TEXT,
      tx_count INTEGER,
      miner TEXT,
      gas_used BIGINT,
      gas_limit BIGINT,
      size_bytes INTEGER,
      base_fee_per_gas BIGINT,
      blob_gas_used BIGINT,
      excess_blob_gas BIGINT,
      ingested_at TIMESTAMPTZ,
      ingestion_ds DATE,

      UNIQUE (chain_key, block_number),
      UNIQUE (block_hash)
    );

    CREATE INDEX IF NOT EXISTS ix_fact_ethereum_block_time_key
      ON {DW_SCHEMA}.fact_ethereum_block (block_time_key);

    CREATE TABLE IF NOT EXISTS {DW_SCHEMA}.dim_crypto (
      crypto_id INTEGER PRIMARY KEY,
      symbol TEXT NOT NULL,
      name TEXT NOT NULL,
      slug TEXT,
      last_updated TIMESTAMP,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS ix_dim_crypto_symbol
      ON {DW_SCHEMA}.dim_crypto (symbol);

    CREATE TABLE IF NOT EXISTS {DW_SCHEMA}.fact_crypto_market_snapshot (
      crypto_id INTEGER NOT NULL REFERENCES {DW_SCHEMA}.dim_crypto(crypto_id),
      time_key BIGINT NOT NULL REFERENCES {DW_SCHEMA}.dim_time(time_key),
      staged_metric_id BIGINT,

      price_usd NUMERIC,
      volume_24h NUMERIC,
      market_cap NUMERIC,
      percent_change_1h NUMERIC,
      percent_change_24h NUMERIC,
      percent_change_7d NUMERIC,
      circulating_supply NUMERIC,
      total_supply NUMERIC,
      max_supply NUMERIC,

      PRIMARY KEY (crypto_id, time_key)
    );

    CREATE UNIQUE INDEX IF NOT EXISTS ux_fact_crypto_market_snapshot_metric_id
      ON {DW_SCHEMA}.fact_crypto_market_snapshot (staged_metric_id)
      WHERE staged_metric_id IS NOT NULL;
    """
    with conn.cursor() as cur:
        cur.execute(ddl)

        cur.execute(
            f"""
            INSERT INTO {DW_SCHEMA}.dim_exchange (exchange_name)
            VALUES (%s)
            ON CONFLICT (exchange_name) DO NOTHING
            """,
            (BINANCE_EXCHANGE_NAME,),
        )
        cur.execute(
            f"""
            INSERT INTO {DW_SCHEMA}.dim_chain (chain_name, chain_id)
            VALUES (%s, %s)
            ON CONFLICT (chain_name) DO UPDATE SET chain_id = EXCLUDED.chain_id
            """,
            (ETHEREUM_CHAIN_NAME, ETHEREUM_CHAIN_ID),
        )
    conn.commit()


def _fetch_one_int(cur, sql: str, params: Tuple[Any, ...]) -> int:
    cur.execute(sql, params)
    row = cur.fetchone()
    if not row or row[0] is None:
        raise RuntimeError("Expected a single-row result")
    return int(row[0])


def _fetch_mapping_any(
    cur, table: str, key_col: str, value_col: str, keys: Sequence[Any]
) -> Dict[Any, Any]:
    if not keys:
        return {}
    cur.execute(
        f"SELECT {key_col}, {value_col} FROM {DW_SCHEMA}.{table} WHERE {key_col} = ANY(%s)",
        (list(keys),),
    )
    return {k: v for (k, v) in cur.fetchall()}


def _ensure_dim_time(conn, timestamps: Sequence[datetime], *, commit: bool = True) -> None:
    if not timestamps:
        return
    sql = f"""
    INSERT INTO {DW_SCHEMA}.dim_time (
      ts_utc, date, year, quarter, month, day, hour, minute, day_of_week, week_of_year, is_weekend
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (ts_utc) DO NOTHING
    """
    rows = [_time_dim_row(ts) for ts in timestamps]
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    if commit:
        conn.commit()


def _ensure_dim_assets(conn, symbols: Sequence[str], *, commit: bool = True) -> None:
    if not symbols:
        return
    sql = f"""
    INSERT INTO {DW_SCHEMA}.dim_asset (symbol, base_asset, quote_asset)
    VALUES (%s, %s, %s)
    ON CONFLICT (symbol) DO UPDATE SET
      base_asset = COALESCE(EXCLUDED.base_asset, {DW_SCHEMA}.dim_asset.base_asset),
      quote_asset = COALESCE(EXCLUDED.quote_asset, {DW_SCHEMA}.dim_asset.quote_asset)
    """
    rows = []
    for s in symbols:
        base, quote = _split_symbol(s)
        rows.append((s.strip().upper(), base, quote))
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    if commit:
        conn.commit()


def _ensure_dim_intervals(conn, intervals: Sequence[str], *, commit: bool = True) -> None:
    if not intervals:
        return
    sql = f"""
    INSERT INTO {DW_SCHEMA}.dim_interval (interval, interval_seconds)
    VALUES (%s, %s)
    ON CONFLICT (interval) DO UPDATE SET
      interval_seconds = COALESCE(EXCLUDED.interval_seconds, {DW_SCHEMA}.dim_interval.interval_seconds)
    """
    rows = [(i, _interval_to_seconds(i)) for i in intervals]
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    if commit:
        conn.commit()


@dataclass(frozen=True)
class Settings:
    pg_host: str
    pg_port: int
    pg_db: str
    pg_user: str
    pg_password: str
    batch_candles: int
    batch_blocks: int
    batch_cmc: int


@dag(
    start_date=pendulum.datetime(2025, 10, 1, tz="Europe/Paris"),
    schedule=[STAGING_DATASET],
    catchup=False,
    max_active_runs=1,
    tags=["pipeline-3", "production", "star-schema", "dw"],
    default_args=dict(retries=2, retry_delay=pendulum.duration(minutes=2)),
    description="Pipeline 3: Lädt Staging-Daten (Binance, Ethereum, CoinMarketCap) in Star-Schema (Production Zone / Data Warehouse).",
)
def pipeline_3_production():
    @task
    def get_settings() -> Dict[str, Any]:
        pg_host = Variable.get("DW_PG_HOST", default_var=DEFAULT_PG_HOST)
        pg_port_raw = Variable.get("DW_PG_PORT", default_var=str(DEFAULT_PG_PORT))
        pg_db = Variable.get("DW_PG_DB", default_var=DEFAULT_PG_DB)
        pg_user = Variable.get("DW_PG_USER", default_var=DEFAULT_PG_USER)
        pg_password = Variable.get("DW_PG_PASSWORD", default_var=DEFAULT_PG_PASSWORD)

        batch_candles_raw = Variable.get(
            "DW_BATCH_SIZE_CANDLES", default_var=str(DEFAULT_BATCH_SIZE_CANDLES)
        )
        batch_blocks_raw = Variable.get(
            "DW_BATCH_SIZE_BLOCKS", default_var=str(DEFAULT_BATCH_SIZE_BLOCKS)
        )
        batch_cmc_raw = Variable.get(
            "DW_BATCH_SIZE_CMC", default_var=str(DEFAULT_BATCH_SIZE_CMC)
        )

        try:
            pg_port = int(pg_port_raw)
        except ValueError:
            pg_port = DEFAULT_PG_PORT
        try:
            batch_candles = max(1, int(batch_candles_raw))
        except ValueError:
            batch_candles = DEFAULT_BATCH_SIZE_CANDLES
        try:
            batch_blocks = max(1, int(batch_blocks_raw))
        except ValueError:
            batch_blocks = DEFAULT_BATCH_SIZE_BLOCKS
        try:
            batch_cmc = max(1, int(batch_cmc_raw))
        except ValueError:
            batch_cmc = DEFAULT_BATCH_SIZE_CMC

        s = Settings(
            pg_host=pg_host,
            pg_port=pg_port,
            pg_db=pg_db,
            pg_user=pg_user,
            pg_password=pg_password,
            batch_candles=batch_candles,
            batch_blocks=batch_blocks,
            batch_cmc=batch_cmc,
        )
        return s.__dict__

    @task
    def create_dw_schema(settings_dict: Dict[str, Any]) -> None:
        settings = Settings(**settings_dict)
        conn = _pg_connect(settings)
        try:
            _ensure_dw_schema(conn)
        finally:
            conn.close()

    @task
    def get_exchange_key(settings_dict: Dict[str, Any]) -> int:
        settings = Settings(**settings_dict)
        conn = _pg_connect(settings)
        try:
            with conn.cursor() as cur:
                return _fetch_one_int(
                    cur,
                    f"SELECT exchange_key FROM {DW_SCHEMA}.dim_exchange WHERE exchange_name=%s",
                    (BINANCE_EXCHANGE_NAME,),
                )
        finally:
            conn.close()

    @task
    def get_chain_key(settings_dict: Dict[str, Any]) -> int:
        settings = Settings(**settings_dict)
        conn = _pg_connect(settings)
        try:
            with conn.cursor() as cur:
                return _fetch_one_int(
                    cur,
                    f"SELECT chain_key FROM {DW_SCHEMA}.dim_chain WHERE chain_name=%s",
                    (ETHEREUM_CHAIN_NAME,),
                )
        finally:
            conn.close()

    @task
    def load_binance_candles(settings_dict: Dict[str, Any], exchange_key: int) -> Dict[str, Any]:
        settings = Settings(**settings_dict)
        checkpoint = Variable.get(CHECKPOINT_BINANCE_VAR, default_var=None)

        # Query staging table
        where_clause = ""
        params_list: List[Any] = []
        if checkpoint:
            where_clause = f"WHERE staged_at > %s"
            params_list.append(checkpoint)

        conn = _pg_connect(settings)
        try:
            stats = {"seen": 0, "inserted_or_updated": 0, "batches": 0, "checkpoint": checkpoint}
            batch: List[Dict[str, Any]] = []
            last_staged_at: Optional[str] = checkpoint

            def process_batch(docs: List[Dict[str, Any]]) -> int:
                nonlocal last_staged_at
                symbols = sorted({(d.get("symbol") or "").strip().upper() for d in docs if d.get("symbol")})
                intervals = sorted({(d.get("interval") or "").strip() for d in docs if d.get("interval")})

                ts_set = set()
                for d in docs:
                    ot = _to_utc_dt_from_ms(d.get("open_time_ms"))
                    ct = _to_utc_dt_from_ms(d.get("close_time_ms"))
                    if ot:
                        ts_set.add(ot)
                    if ct:
                        ts_set.add(ct)
                    sa = d.get("staged_at")
                    if sa:
                        last_staged_at = str(sa)

                _ensure_dim_assets(conn, symbols, commit=False)
                _ensure_dim_intervals(conn, intervals, commit=False)
                _ensure_dim_time(conn, sorted(ts_set), commit=False)

                with conn.cursor() as cur:
                    asset_map = _fetch_mapping_any(cur, "dim_asset", "symbol", "asset_key", symbols)
                    interval_map = _fetch_mapping_any(
                        cur, "dim_interval", "interval", "interval_key", intervals
                    )
                    time_map = _fetch_mapping_any(cur, "dim_time", "ts_utc", "time_key", list(ts_set))

                    rows: List[Tuple[Any, ...]] = []
                    for d in docs:
                        symbol = (d.get("symbol") or "").strip().upper()
                        interval = (d.get("interval") or "").strip()
                        ot = _to_utc_dt_from_ms(d.get("open_time_ms"))
                        ct = _to_utc_dt_from_ms(d.get("close_time_ms"))
                        if not symbol or not interval or not ot or not ct:
                            continue
                        asset_key = asset_map.get(symbol)
                        interval_key = interval_map.get(interval)
                        open_time_key = time_map.get(ot)
                        close_time_key = time_map.get(ct)
                        if not asset_key or not interval_key or not open_time_key or not close_time_key:
                            continue

                        trading_day = d.get("trading_day")
                        if isinstance(trading_day, str):
                            try:
                                trading_day = date.fromisoformat(trading_day)
                            except ValueError:
                                trading_day = None

                        ingested_at_dt = d.get("ingested_at")
                        if isinstance(ingested_at_dt, str):
                            try:
                                ingested_at_dt = pendulum.parse(ingested_at_dt).in_timezone("UTC")
                            except Exception:
                                ingested_at_dt = None

                        rows.append(
                            (
                                exchange_key,
                                asset_key,
                                interval_key,
                                open_time_key,
                                close_time_key,
                                trading_day,
                                d.get("open"),
                                d.get("high"),
                                d.get("low"),
                                d.get("close"),
                                d.get("volume"),
                                d.get("quote_asset_volume"),
                                d.get("number_of_trades"),
                                d.get("taker_buy_base_volume"),
                                d.get("taker_buy_quote_volume"),
                                d.get("source_file"),
                                ingested_at_dt,
                            )
                        )

                    if not rows:
                        return 0

                    insert_sql = f"""
                    INSERT INTO {DW_SCHEMA}.fact_binance_candle (
                      exchange_key, asset_key, interval_key, open_time_key, close_time_key, trading_day,
                      open, high, low, close, volume, quote_asset_volume, number_of_trades,
                      taker_buy_base_volume, taker_buy_quote_volume,
                      source_file, ingested_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (exchange_key, asset_key, interval_key, open_time_key)
                    DO UPDATE SET
                      close_time_key = EXCLUDED.close_time_key,
                      trading_day = COALESCE(EXCLUDED.trading_day, {DW_SCHEMA}.fact_binance_candle.trading_day),
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
                      ingested_at = EXCLUDED.ingested_at
                    """
                    cur.executemany(insert_sql, rows)
                    conn.commit()
                    return len(rows)

            # Read data from staging table
            with conn.cursor() as cur:
                query = f"""
                SELECT
                    symbol, interval, open_time_ms, close_time_ms, trading_day,
                    open, high, low, close, volume, quote_asset_volume,
                    number_of_trades, taker_buy_base_volume, taker_buy_quote_volume,
                    source_file, ingested_at, staged_at
                FROM {STAGING_SCHEMA}.binance_candles
                {where_clause}
                ORDER BY staged_at
                """
                cur.execute(query, params_list)

                cur.arraysize = max(1, settings.batch_candles)
                while True:
                    rows = cur.fetchmany(settings.batch_candles)
                    if not rows:
                        break
                    batch = []
                    for row in rows:
                        stats["seen"] += 1
                        batch.append(
                            {
                                "symbol": row[0],
                                "interval": row[1],
                                "open_time_ms": row[2],
                                "close_time_ms": row[3],
                                "trading_day": row[4],
                                "open": row[5],
                                "high": row[6],
                                "low": row[7],
                                "close": row[8],
                                "volume": row[9],
                                "quote_asset_volume": row[10],
                                "number_of_trades": row[11],
                                "taker_buy_base_volume": row[12],
                                "taker_buy_quote_volume": row[13],
                                "source_file": row[14],
                                "ingested_at": row[15],
                                "staged_at": row[16],
                            }
                        )
                    stats["inserted_or_updated"] += process_batch(batch)
                    stats["batches"] += 1

            if stats["inserted_or_updated"] == 0:
                raise AirflowSkipException("Keine neuen/validen Binance-Candles zum Laden gefunden.")

            stats["new_checkpoint"] = last_staged_at

            return stats
        finally:
            conn.close()

    @task
    def load_etherscan_blocks(settings_dict: Dict[str, Any], chain_key: int) -> Dict[str, Any]:
        settings = Settings(**settings_dict)
        checkpoint = Variable.get(CHECKPOINT_ETHERSCAN_VAR, default_var=None)

        # Query staging table
        where_clause = ""
        params_list: List[Any] = []
        if checkpoint:
            where_clause = f"WHERE staged_at > %s"
            params_list.append(checkpoint)

        conn = _pg_connect(settings)
        try:
            stats = {"seen": 0, "inserted_or_updated": 0, "batches": 0, "checkpoint": checkpoint}
            batch: List[Dict[str, Any]] = []
            last_staged_at: Optional[str] = checkpoint

            def process_batch(docs: List[Dict[str, Any]]) -> int:
                nonlocal last_staged_at
                ts_set = set()
                for d in docs:
                    bt = _to_utc_dt_from_unix_seconds(d.get("timestamp_unix"))
                    if bt:
                        ts_set.add(bt)
                    sa = d.get("staged_at")
                    if sa:
                        last_staged_at = str(sa)

                _ensure_dim_time(conn, sorted(ts_set), commit=False)

                with conn.cursor() as cur:
                    time_map = _fetch_mapping_any(cur, "dim_time", "ts_utc", "time_key", list(ts_set))

                    rows: List[Tuple[Any, ...]] = []
                    for d in docs:
                        block_time = _to_utc_dt_from_unix_seconds(d.get("timestamp_unix"))
                        if not block_time:
                            continue
                        block_time_key = time_map.get(block_time)
                        if not block_time_key:
                            continue

                        ingested_at_dt = d.get("ingested_at")
                        if isinstance(ingested_at_dt, str):
                            try:
                                ingested_at_dt = pendulum.parse(ingested_at_dt).in_timezone("UTC")
                            except Exception:
                                ingested_at_dt = None

                        ingestion_ds = d.get("ingestion_ds")
                        if isinstance(ingestion_ds, str):
                            try:
                                ingestion_ds = date.fromisoformat(ingestion_ds)
                            except ValueError:
                                ingestion_ds = None

                        rows.append(
                            (
                                chain_key,
                                block_time_key,
                                d.get("block_number"),
                                d.get("block_hash"),
                                d.get("tx_count"),
                                d.get("miner"),
                                d.get("gas_used"),
                                d.get("gas_limit"),
                                d.get("size_bytes"),
                                d.get("base_fee_per_gas"),
                                d.get("blob_gas_used"),
                                d.get("excess_blob_gas"),
                                ingested_at_dt,
                                ingestion_ds,
                            )
                        )

                    if not rows:
                        return 0

                    insert_sql = f"""
                    INSERT INTO {DW_SCHEMA}.fact_ethereum_block (
                      chain_key, block_time_key, block_number, block_hash, tx_count,
                      miner, gas_used, gas_limit, size_bytes,
                      base_fee_per_gas, blob_gas_used, excess_blob_gas,
                      ingested_at, ingestion_ds
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (chain_key, block_number)
                    DO UPDATE SET
                      block_time_key = EXCLUDED.block_time_key,
                      block_hash = EXCLUDED.block_hash,
                      tx_count = EXCLUDED.tx_count,
                      miner = EXCLUDED.miner,
                      gas_used = EXCLUDED.gas_used,
                      gas_limit = EXCLUDED.gas_limit,
                      size_bytes = EXCLUDED.size_bytes,
                      base_fee_per_gas = EXCLUDED.base_fee_per_gas,
                      blob_gas_used = EXCLUDED.blob_gas_used,
                      excess_blob_gas = EXCLUDED.excess_blob_gas,
                      ingested_at = EXCLUDED.ingested_at,
                      ingestion_ds = COALESCE(EXCLUDED.ingestion_ds, {DW_SCHEMA}.fact_ethereum_block.ingestion_ds)
                    """
                    cur.executemany(insert_sql, rows)
                    conn.commit()
                    return len(rows)

            # Read data from staging table
            with conn.cursor() as cur:
                query = f"""
                SELECT
                    block_hash, block_number, timestamp_unix, tx_count,
                    miner, gas_used, gas_limit, size_bytes,
                    base_fee_per_gas, blob_gas_used, excess_blob_gas,
                    ingestion_ds, ingested_at, staged_at
                FROM {STAGING_SCHEMA}.ethereum_blocks
                {where_clause}
                ORDER BY staged_at
                """
                cur.execute(query, params_list)

                cur.arraysize = max(1, settings.batch_blocks)
                while True:
                    rows = cur.fetchmany(settings.batch_blocks)
                    if not rows:
                        break
                    batch = []
                    for row in rows:
                        stats["seen"] += 1
                        batch.append(
                            {
                                "block_hash": row[0],
                                "block_number": row[1],
                                "timestamp_unix": row[2],
                                "tx_count": row[3],
                                "miner": row[4],
                                "gas_used": row[5],
                                "gas_limit": row[6],
                                "size_bytes": row[7],
                                "base_fee_per_gas": row[8],
                                "blob_gas_used": row[9],
                                "excess_blob_gas": row[10],
                                "ingestion_ds": row[11],
                                "ingested_at": row[12],
                                "staged_at": row[13],
                            }
                        )
                    stats["inserted_or_updated"] += process_batch(batch)
                    stats["batches"] += 1

            if stats["inserted_or_updated"] == 0:
                raise AirflowSkipException("Keine neuen/validen Etherscan-Blöcke zum Laden gefunden.")

            stats["new_checkpoint"] = last_staged_at

            return stats
        finally:
            conn.close()

    @task
    def update_binance_checkpoint(stats: Dict[str, Any]) -> Dict[str, Any]:
        previous = stats.get("checkpoint")
        new_val = stats.get("new_checkpoint")
        if new_val and new_val != previous:
            Variable.set(CHECKPOINT_BINANCE_VAR, str(new_val))
            return {"checkpoint": str(new_val)}
        return {"checkpoint": previous}

    @task
    def update_etherscan_checkpoint(stats: Dict[str, Any]) -> Dict[str, Any]:
        previous = stats.get("checkpoint")
        new_val = stats.get("new_checkpoint")
        if new_val and new_val != previous:
            Variable.set(CHECKPOINT_ETHERSCAN_VAR, str(new_val))
            return {"checkpoint": str(new_val)}
        return {"checkpoint": previous}

    @task
    def load_coinmarketcap_metrics(settings_dict: Dict[str, Any]) -> Dict[str, Any]:
        settings = Settings(**settings_dict)
        checkpoint_raw = Variable.get(CHECKPOINT_CMC_VAR, default_var="0")
        try:
            checkpoint_id = int(checkpoint_raw)
        except ValueError:
            checkpoint_id = 0

        conn = _pg_connect(settings)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass(%s)", (f"{STAGING_SCHEMA}.crypto_metrics",))
                metrics_reg = cur.fetchone()[0]
                cur.execute("SELECT to_regclass(%s)", (f"{STAGING_SCHEMA}.cryptocurrencies",))
                cryptos_reg = cur.fetchone()[0]
                if not metrics_reg or not cryptos_reg:
                    raise AirflowSkipException(
                        f"Missing staging tables for CoinMarketCap ({STAGING_SCHEMA}.crypto_metrics / {STAGING_SCHEMA}.cryptocurrencies)."
                    )

                cur.execute(
                    f"""
                    SELECT
                      m.id,
                      m.crypto_id,
                      c.symbol,
                      c.name,
                      c.slug,
                      c.last_updated,
                      m.recorded_at,
                      m.price_usd,
                      m.volume_24h,
                      m.market_cap,
                      m.percent_change_1h,
                      m.percent_change_24h,
                      m.percent_change_7d,
                      m.circulating_supply,
                      m.total_supply,
                      m.max_supply
                    FROM {STAGING_SCHEMA}.crypto_metrics m
                    JOIN {STAGING_SCHEMA}.cryptocurrencies c ON c.crypto_id = m.crypto_id
                    WHERE m.id > %s
                    ORDER BY m.id
                    """,
                    (checkpoint_id,),
                )

                cur.arraysize = max(1, settings.batch_cmc)
                stats = {"seen": 0, "inserted_or_updated": 0, "batches": 0, "checkpoint_id": checkpoint_id}
                last_id = checkpoint_id

                while True:
                    rows = cur.fetchmany(settings.batch_cmc)
                    if not rows:
                        break

                    stats["batches"] += 1
                    stats["seen"] += len(rows)
                    last_id = int(rows[-1][0])

                    dim_crypto_rows: List[Tuple[Any, ...]] = []
                    timestamps: List[datetime] = []
                    fact_rows: List[Tuple[Any, ...]] = []

                    for r in rows:
                        metric_id = int(r[0])
                        crypto_id = int(r[1])
                        symbol = (r[2] or "").strip().upper()
                        name = (r[3] or "").strip()
                        slug = r[4] or None
                        last_updated = r[5]
                        recorded_at = r[6]

                        if not symbol or not name or not isinstance(recorded_at, datetime):
                            continue

                        if recorded_at.tzinfo is None:
                            ts_utc = recorded_at.replace(tzinfo=timezone.utc)
                        else:
                            ts_utc = recorded_at.astimezone(timezone.utc)

                        dim_crypto_rows.append((crypto_id, symbol, name, slug, last_updated))
                        timestamps.append(ts_utc)
                        fact_rows.append(
                            (
                                crypto_id,
                                ts_utc,
                                metric_id,
                                r[7],
                                r[8],
                                r[9],
                                r[10],
                                r[11],
                                r[12],
                                r[13],
                                r[14],
                                r[15],
                            )
                        )

                    if not fact_rows:
                        continue

                    dim_upsert_sql = f"""
                    INSERT INTO {DW_SCHEMA}.dim_crypto (crypto_id, symbol, name, slug, last_updated)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (crypto_id) DO UPDATE SET
                      symbol = EXCLUDED.symbol,
                      name = EXCLUDED.name,
                      slug = EXCLUDED.slug,
                      last_updated = EXCLUDED.last_updated
                    """

                    with conn.cursor() as cur2:
                        cur2.executemany(dim_upsert_sql, dim_crypto_rows)

                    _ensure_dim_time(conn, timestamps, commit=False)

                    with conn.cursor() as cur2:
                        time_map = _fetch_mapping_any(cur2, "dim_time", "ts_utc", "time_key", timestamps)

                        insert_rows: List[Tuple[Any, ...]] = []
                        for (
                            crypto_id,
                            ts_utc,
                            metric_id,
                            price_usd,
                            volume_24h,
                            market_cap,
                            pc_1h,
                            pc_24h,
                            pc_7d,
                            circulating_supply,
                            total_supply,
                            max_supply,
                        ) in fact_rows:
                            time_key = time_map.get(ts_utc)
                            if not time_key:
                                continue
                            insert_rows.append(
                                (
                                    crypto_id,
                                    time_key,
                                    metric_id,
                                    price_usd,
                                    volume_24h,
                                    market_cap,
                                    pc_1h,
                                    pc_24h,
                                    pc_7d,
                                    circulating_supply,
                                    total_supply,
                                    max_supply,
                                )
                            )

                        if not insert_rows:
                            continue

                        fact_sql = f"""
                        INSERT INTO {DW_SCHEMA}.fact_crypto_market_snapshot (
                          crypto_id, time_key, staged_metric_id,
                          price_usd, volume_24h, market_cap,
                          percent_change_1h, percent_change_24h, percent_change_7d,
                          circulating_supply, total_supply, max_supply
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (crypto_id, time_key) DO UPDATE SET
                          staged_metric_id = EXCLUDED.staged_metric_id,
                          price_usd = EXCLUDED.price_usd,
                          volume_24h = EXCLUDED.volume_24h,
                          market_cap = EXCLUDED.market_cap,
                          percent_change_1h = EXCLUDED.percent_change_1h,
                          percent_change_24h = EXCLUDED.percent_change_24h,
                          percent_change_7d = EXCLUDED.percent_change_7d,
                          circulating_supply = EXCLUDED.circulating_supply,
                          total_supply = EXCLUDED.total_supply,
                          max_supply = EXCLUDED.max_supply
                        """
                        cur2.executemany(fact_sql, insert_rows)
                        stats["inserted_or_updated"] += len(insert_rows)

                    conn.commit()

            if stats["inserted_or_updated"] == 0:
                raise AirflowSkipException("No new CoinMarketCap metrics to load.")

            stats["new_checkpoint_id"] = last_id
            return stats
        finally:
            conn.close()

    @task
    def update_coinmarketcap_checkpoint(stats: Dict[str, Any]) -> Dict[str, Any]:
        previous = stats.get("checkpoint_id")
        new_val = stats.get("new_checkpoint_id")
        try:
            prev_int = int(previous) if previous is not None else 0
        except (TypeError, ValueError):
            prev_int = 0
        try:
            new_int = int(new_val) if new_val is not None else prev_int
        except (TypeError, ValueError):
            new_int = prev_int
        if new_int > prev_int:
            Variable.set(CHECKPOINT_CMC_VAR, str(new_int))
            return {"checkpoint_id": new_int}
        return {"checkpoint_id": prev_int}

    @task(trigger_rule="all_done")
    def log_summary(
        binance_stats: Dict[str, Any],
        etherscan_stats: Dict[str, Any],
        binance_checkpoint: Dict[str, Any],
        etherscan_checkpoint: Dict[str, Any],
        cmc_stats: Dict[str, Any],
        cmc_checkpoint: Dict[str, Any],
    ) -> None:
        print("=" * 60)
        print("Pipeline 3 (Production / Data Warehouse) Summary:")
        print("=" * 60)
        print(f"Binance: {binance_stats}")
        print(f"Ethereum: {etherscan_stats}")
        print(f"CoinMarketCap: {cmc_stats}")
        print(f"Binance checkpoint: {binance_checkpoint}")
        print(f"Ethereum checkpoint: {etherscan_checkpoint}")
        print(f"CoinMarketCap checkpoint: {cmc_checkpoint}")
        print("=" * 60)
        print("Star Schema in postgres-data:dw erfolgreich aktualisiert!")
        print("=" * 60)

    settings = get_settings()
    created = create_dw_schema(settings)
    exchange_key = get_exchange_key(settings)
    chain_key = get_chain_key(settings)
    created >> [exchange_key, chain_key]

    binance_stats = load_binance_candles(settings, exchange_key)
    etherscan_stats = load_etherscan_blocks(settings, chain_key)
    exchange_key >> binance_stats
    chain_key >> etherscan_stats

    binance_checkpoint = update_binance_checkpoint(binance_stats)
    etherscan_checkpoint = update_etherscan_checkpoint(etherscan_stats)
    binance_stats >> binance_checkpoint
    etherscan_stats >> etherscan_checkpoint

    cmc_stats = load_coinmarketcap_metrics(settings)
    created >> cmc_stats
    cmc_checkpoint = update_coinmarketcap_checkpoint(cmc_stats)
    cmc_stats >> cmc_checkpoint

    log_summary(
        binance_stats,
        etherscan_stats,
        binance_checkpoint,
        etherscan_checkpoint,
        cmc_stats,
        cmc_checkpoint,
    )

    publish_dw_dataset = EmptyOperator(
        task_id="publish_dw_dataset",
        outlets=[DW_DATASET],
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    [binance_stats, etherscan_stats, cmc_stats] >> publish_dw_dataset


pipeline_3_production()
