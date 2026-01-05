from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import pendulum

try:
    from airflow.sdk.dag import dag
    from airflow.sdk.task import task
except ModuleNotFoundError:
    from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable

from pipeline_datasets import DW_DATASET

DEFAULT_PG_HOST = "postgres-data"
DEFAULT_PG_PORT = 5432
DEFAULT_PG_DB = "datadb"
DEFAULT_PG_USER = "datauser"
DEFAULT_PG_PASSWORD = "datapass"

DW_SCHEMA = "dw"

DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_ASSET_SYMBOL = "ETHUSDT"
DEFAULT_BINANCE_INTERVAL = "1m"
DEFAULT_EXCHANGE_NAME = "Binance"
DEFAULT_CHAIN_NAME = "Ethereum"
DEFAULT_OUTPUT_DIR = "/opt/airflow/data/analysis/eth_price_vs_gas_fee"
DEFAULT_CMC_TOP_N = 10
DEFAULT_CMC_BASE_SYMBOL = "ETH"


@dataclass(frozen=True)
class Settings:
    pg_host: str
    pg_port: int
    pg_db: str
    pg_user: str
    pg_password: str
    lookback_days: int
    asset_symbol: str
    binance_interval: str
    exchange_name: str
    chain_name: str
    output_dir: str


def _pg_connect(settings: Settings):
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


@dag(
    start_date=pendulum.datetime(2025, 10, 1, tz="Europe/Paris"),
    schedule=[DW_DATASET],
    catchup=False,
    max_active_runs=1,
    tags=["pipeline-4", "analytics", "analysis", "correlation"],
    default_args=dict(retries=2, retry_delay=pendulum.duration(minutes=2)),
    description="Pipeline 4: Analytics - Analysiert Korrelation zwischen ETH-Preis (Binance) und Gas-Gebühren (Ethereum).",
)
def pipeline_4_analytics():
    @task
    def get_settings() -> Dict[str, Any]:
        pg_host = Variable.get("DW_PG_HOST", default_var=DEFAULT_PG_HOST)
        pg_port_raw = Variable.get("DW_PG_PORT", default_var=str(DEFAULT_PG_PORT))
        pg_db = Variable.get("DW_PG_DB", default_var=DEFAULT_PG_DB)
        pg_user = Variable.get("DW_PG_USER", default_var=DEFAULT_PG_USER)
        pg_password = Variable.get("DW_PG_PASSWORD", default_var=DEFAULT_PG_PASSWORD)

        lookback_days_raw = Variable.get(
            "ANALYSIS_LOOKBACK_DAYS", default_var=str(DEFAULT_LOOKBACK_DAYS)
        )
        asset_symbol = Variable.get(
            "ANALYSIS_ASSET_SYMBOL", default_var=DEFAULT_ASSET_SYMBOL
        )
        binance_interval = Variable.get(
            "ANALYSIS_BINANCE_INTERVAL", default_var=DEFAULT_BINANCE_INTERVAL
        )
        exchange_name = Variable.get(
            "ANALYSIS_EXCHANGE_NAME", default_var=DEFAULT_EXCHANGE_NAME
        )
        chain_name = Variable.get("ANALYSIS_CHAIN_NAME", default_var=DEFAULT_CHAIN_NAME)
        output_dir = Variable.get("ANALYSIS_OUTPUT_DIR", default_var=DEFAULT_OUTPUT_DIR)
        cmc_base_symbol = Variable.get("ANALYSIS_CMC_BASE_SYMBOL", default_var=DEFAULT_CMC_BASE_SYMBOL)

        try:
            pg_port = int(pg_port_raw)
        except ValueError:
            pg_port = DEFAULT_PG_PORT
        try:
            lookback_days = max(1, int(lookback_days_raw))
        except ValueError:
            lookback_days = DEFAULT_LOOKBACK_DAYS

        settings = Settings(
            pg_host=pg_host,
            pg_port=pg_port,
            pg_db=pg_db,
            pg_user=pg_user,
            pg_password=pg_password,
            lookback_days=lookback_days,
            asset_symbol=str(asset_symbol).strip().upper(),
            binance_interval=str(binance_interval).strip(),
            exchange_name=str(exchange_name).strip(),
            chain_name=str(chain_name).strip(),
            output_dir=str(output_dir).strip(),
        )
        # stash extra config alongside settings without expanding the dataclass
        d = settings.__dict__
        d["cmc_base_symbol"] = str(cmc_base_symbol).strip().upper()
        return d

    @task
    def run_coinmarketcap_analysis(settings_dict: Dict[str, Any]) -> Dict[str, Any]:
        settings = Settings(**settings_dict)
        conn = _pg_connect(settings)
        try:
            top_n_raw = Variable.get("ANALYSIS_CMC_TOP_N", default_var=str(DEFAULT_CMC_TOP_N))
            try:
                top_n = max(1, int(top_n_raw))
            except ValueError:
                top_n = DEFAULT_CMC_TOP_N

            with conn.cursor() as cur:
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {DW_SCHEMA};")
                cur.execute("SELECT to_regclass(%s)", (f"{DW_SCHEMA}.fact_crypto_market_snapshot",))
                fact_reg = cur.fetchone()[0]
                cur.execute("SELECT to_regclass(%s)", (f"{DW_SCHEMA}.dim_crypto",))
                dim_reg = cur.fetchone()[0]
                if not fact_reg or not dim_reg:
                    raise AirflowSkipException(
                        f"Missing DW tables for CoinMarketCap ({DW_SCHEMA}.fact_crypto_market_snapshot / {DW_SCHEMA}.dim_crypto)."
                    )

                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {DW_SCHEMA}.analysis_coinmarketcap_top_assets (
                      snapshot_ts TIMESTAMP NOT NULL,
                      rank INTEGER NOT NULL,
                      crypto_id INTEGER NOT NULL,
                      symbol TEXT NOT NULL,
                      name TEXT NOT NULL,
                      price_usd NUMERIC,
                      volume_24h NUMERIC,
                      market_cap NUMERIC,
                      percent_change_24h NUMERIC,
                      computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                      PRIMARY KEY (snapshot_ts, crypto_id)
                    );
                    """
                )
            conn.commit()

            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    WITH latest_per_crypto AS (
                      SELECT crypto_id, MAX(time_key) AS time_key
                      FROM {DW_SCHEMA}.fact_crypto_market_snapshot
                      GROUP BY 1
                    ),
                    snapshot AS (
                      SELECT
                        t.ts_utc AS snapshot_ts,
                        ROW_NUMBER() OVER (
                          ORDER BY f.market_cap DESC NULLS LAST,
                                   f.volume_24h DESC NULLS LAST,
                                   d.symbol
                        ) AS rank,
                        d.crypto_id,
                        d.symbol,
                        d.name,
                        f.price_usd,
                        f.volume_24h,
                        f.market_cap,
                        f.percent_change_24h
                      FROM latest_per_crypto l
                      JOIN {DW_SCHEMA}.fact_crypto_market_snapshot f
                        ON f.crypto_id = l.crypto_id AND f.time_key = l.time_key
                      JOIN {DW_SCHEMA}.dim_time t ON t.time_key = f.time_key
                      JOIN {DW_SCHEMA}.dim_crypto d ON d.crypto_id = f.crypto_id
                      ORDER BY f.market_cap DESC NULLS LAST, f.volume_24h DESC NULLS LAST, d.symbol
                      LIMIT %(top_n)s
                    )
                    INSERT INTO {DW_SCHEMA}.analysis_coinmarketcap_top_assets (
                      snapshot_ts, rank, crypto_id, symbol, name,
                      price_usd, volume_24h, market_cap, percent_change_24h
                    )
                    SELECT
                      snapshot_ts, rank, crypto_id, symbol, name,
                      price_usd, volume_24h, market_cap, percent_change_24h
                    FROM snapshot
                    ON CONFLICT (snapshot_ts, crypto_id) DO UPDATE SET
                      rank = EXCLUDED.rank,
                      symbol = EXCLUDED.symbol,
                      name = EXCLUDED.name,
                      price_usd = EXCLUDED.price_usd,
                      volume_24h = EXCLUDED.volume_24h,
                      market_cap = EXCLUDED.market_cap,
                      percent_change_24h = EXCLUDED.percent_change_24h,
                      computed_at = NOW()
                    """,
                    {"top_n": top_n},
                )
                cur.execute(
                    f"""
                    SELECT snapshot_ts, COUNT(*) AS n_rows
                    FROM {DW_SCHEMA}.analysis_coinmarketcap_top_assets
                    GROUP BY 1
                    ORDER BY snapshot_ts DESC
                    LIMIT 1
                    """
                )
                row = cur.fetchone()
            conn.commit()

            if not row:
                raise AirflowSkipException("CoinMarketCap analysis produced no rows.")

            return {
                "table": f"{DW_SCHEMA}.analysis_coinmarketcap_top_assets",
                "snapshot_ts": str(row[0]),
                "top_n": top_n,
                "rows_in_snapshot": int(row[1]),
            }
        finally:
            conn.close()

    @task
    def run_integrated_daily_analysis(settings_dict: Dict[str, Any]) -> Dict[str, Any]:
        settings = Settings(**settings_dict)
        base_symbol = str(settings_dict.get("cmc_base_symbol") or DEFAULT_CMC_BASE_SYMBOL).strip().upper()

        conn = _pg_connect(settings)
        try:
            with conn.cursor() as cur:
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {DW_SCHEMA};")
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {DW_SCHEMA}.analysis_integrated_daily (
                      trading_date DATE PRIMARY KEY,
                      base_symbol TEXT NOT NULL,
                      cmc_price_usd NUMERIC,
                      cmc_market_cap NUMERIC,
                      binance_avg_close_usdt NUMERIC,
                      binance_avg_volume_usdt NUMERIC,
                      eth_avg_base_fee_gwei NUMERIC,
                      computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                    """
                )
            conn.commit()

            window_end = pendulum.now("UTC")
            window_start = window_end.subtract(days=settings.lookback_days)

            params = {
                "base_symbol": base_symbol,
                "exchange_name": settings.exchange_name,
                "asset_symbol": settings.asset_symbol,
                "binance_interval": settings.binance_interval,
                "chain_name": settings.chain_name,
                "window_start": window_start,
                "window_end": window_end,
            }

            upsert_sql = f"""
            WITH
            cmc_crypto AS (
              SELECT crypto_id
              FROM {DW_SCHEMA}.dim_crypto
              WHERE symbol = %(base_symbol)s
              LIMIT 1
            ),
            cmc_daily AS (
              SELECT DISTINCT ON (t.date)
                t.date AS trading_date,
                s.price_usd AS cmc_price_usd,
                s.market_cap AS cmc_market_cap
              FROM {DW_SCHEMA}.fact_crypto_market_snapshot s
              JOIN {DW_SCHEMA}.dim_time t ON t.time_key = s.time_key
              JOIN cmc_crypto c ON c.crypto_id = s.crypto_id
              WHERE t.ts_utc >= %(window_start)s
                AND t.ts_utc < %(window_end)s
              ORDER BY t.date, t.ts_utc DESC
            ),
            binance_daily AS (
              SELECT
                t.date AS trading_date,
                AVG(f.close) AS binance_avg_close_usdt,
                AVG(f.quote_asset_volume) AS binance_avg_volume_usdt
              FROM {DW_SCHEMA}.fact_binance_candle f
              JOIN {DW_SCHEMA}.dim_time t ON t.time_key = f.open_time_key
              JOIN {DW_SCHEMA}.dim_asset a ON a.asset_key = f.asset_key
              JOIN {DW_SCHEMA}.dim_exchange e ON e.exchange_key = f.exchange_key
              JOIN {DW_SCHEMA}.dim_interval i ON i.interval_key = f.interval_key
              WHERE e.exchange_name = %(exchange_name)s
                AND a.symbol = %(asset_symbol)s
                AND i.interval = %(binance_interval)s
                AND f.close IS NOT NULL
                AND t.ts_utc >= %(window_start)s
                AND t.ts_utc < %(window_end)s
              GROUP BY 1
            ),
            eth_daily AS (
              SELECT
                t.date AS trading_date,
                AVG(f.base_fee_per_gas::numeric) / 1e9 AS eth_avg_base_fee_gwei
              FROM {DW_SCHEMA}.fact_ethereum_block f
              JOIN {DW_SCHEMA}.dim_time t ON t.time_key = f.block_time_key
              JOIN {DW_SCHEMA}.dim_chain c ON c.chain_key = f.chain_key
              WHERE c.chain_name = %(chain_name)s
                AND f.base_fee_per_gas IS NOT NULL
                AND t.ts_utc >= %(window_start)s
                AND t.ts_utc < %(window_end)s
              GROUP BY 1
            ),
            joined AS (
              SELECT
                COALESCE(b.trading_date, e.trading_date, c.trading_date) AS trading_date,
                %(base_symbol)s AS base_symbol,
                c.cmc_price_usd,
                c.cmc_market_cap,
                b.binance_avg_close_usdt,
                b.binance_avg_volume_usdt,
                e.eth_avg_base_fee_gwei
              FROM binance_daily b
              FULL OUTER JOIN eth_daily e USING (trading_date)
              FULL OUTER JOIN cmc_daily c USING (trading_date)
            )
            INSERT INTO {DW_SCHEMA}.analysis_integrated_daily (
              trading_date, base_symbol,
              cmc_price_usd, cmc_market_cap,
              binance_avg_close_usdt, binance_avg_volume_usdt,
              eth_avg_base_fee_gwei
            )
            SELECT
              trading_date, base_symbol,
              cmc_price_usd, cmc_market_cap,
              binance_avg_close_usdt, binance_avg_volume_usdt,
              eth_avg_base_fee_gwei
            FROM joined
            WHERE trading_date IS NOT NULL
            ON CONFLICT (trading_date) DO UPDATE SET
              base_symbol = EXCLUDED.base_symbol,
              cmc_price_usd = EXCLUDED.cmc_price_usd,
              cmc_market_cap = EXCLUDED.cmc_market_cap,
              binance_avg_close_usdt = EXCLUDED.binance_avg_close_usdt,
              binance_avg_volume_usdt = EXCLUDED.binance_avg_volume_usdt,
              eth_avg_base_fee_gwei = EXCLUDED.eth_avg_base_fee_gwei,
              computed_at = NOW()
            """

            with conn.cursor() as cur:
                cur.execute(upsert_sql, params)
                cur.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM {DW_SCHEMA}.analysis_integrated_daily
                    WHERE trading_date >= %(window_start)s::date
                      AND trading_date < %(window_end)s::date
                    """,
                    params,
                )
                n_rows = int(cur.fetchone()[0])
            conn.commit()

            if n_rows < 1:
                raise AirflowSkipException("Integrated daily analysis produced no rows.")

            return {
                "table": f"{DW_SCHEMA}.analysis_integrated_daily",
                "base_symbol": base_symbol,
                "lookback_days": settings.lookback_days,
                "rows_in_window": n_rows,
            }
        finally:
            conn.close()

    @task
    def run_analysis(settings_dict: Dict[str, Any]) -> Dict[str, Any]:
        settings = Settings(**settings_dict)
        conn = _pg_connect(settings)
        try:
            with conn.cursor() as cur:
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {DW_SCHEMA};")
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {DW_SCHEMA}.analysis_eth_price_vs_gas_fee_hourly (
                      hour_ts TIMESTAMPTZ PRIMARY KEY,
                      avg_close_usdt NUMERIC,
                      avg_volume_usdt NUMERIC,
                      avg_base_fee_gwei NUMERIC,
                      computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                    """
                )
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {DW_SCHEMA}.analysis_eth_price_vs_gas_fee_summary (
                      run_key BIGSERIAL PRIMARY KEY,
                      run_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                      window_start TIMESTAMPTZ NOT NULL,
                      window_end TIMESTAMPTZ NOT NULL,
                      asset_symbol TEXT NOT NULL,
                      binance_interval TEXT NOT NULL,
                      exchange_name TEXT NOT NULL,
                      chain_name TEXT NOT NULL,
                      n_hours INTEGER NOT NULL,
                      corr_price_vs_base_fee_gwei NUMERIC,
                      corr_volume_vs_base_fee_gwei NUMERIC
                    );
                    """
                )
            conn.commit()

            window_end = pendulum.now("UTC")
            window_start = window_end.subtract(days=settings.lookback_days)

            upsert_hourly_sql = f"""
            WITH
            hourly_price AS (
              SELECT
                date_trunc('hour', t.ts_utc) AS hour_ts,
                AVG(f.close) AS avg_close_usdt,
                AVG(f.quote_asset_volume) AS avg_volume_usdt
              FROM {DW_SCHEMA}.fact_binance_candle f
              JOIN {DW_SCHEMA}.dim_time t ON t.time_key = f.open_time_key
              JOIN {DW_SCHEMA}.dim_asset a ON a.asset_key = f.asset_key
              JOIN {DW_SCHEMA}.dim_exchange e ON e.exchange_key = f.exchange_key
              JOIN {DW_SCHEMA}.dim_interval i ON i.interval_key = f.interval_key
              WHERE e.exchange_name = %(exchange_name)s
                AND a.symbol = %(asset_symbol)s
                AND i.interval = %(binance_interval)s
                AND f.close IS NOT NULL
                AND t.ts_utc >= %(window_start)s
                AND t.ts_utc < %(window_end)s
              GROUP BY 1
            ),
            hourly_gas AS (
              SELECT
                date_trunc('hour', t.ts_utc) AS hour_ts,
                AVG(f.base_fee_per_gas::numeric) / 1e9 AS avg_base_fee_gwei
              FROM {DW_SCHEMA}.fact_ethereum_block f
              JOIN {DW_SCHEMA}.dim_time t ON t.time_key = f.block_time_key
              JOIN {DW_SCHEMA}.dim_chain c ON c.chain_key = f.chain_key
              WHERE c.chain_name = %(chain_name)s
                AND f.base_fee_per_gas IS NOT NULL
                AND t.ts_utc >= %(window_start)s
                AND t.ts_utc < %(window_end)s
              GROUP BY 1
            ),
            joined AS (
              SELECT p.hour_ts, p.avg_close_usdt, p.avg_volume_usdt, g.avg_base_fee_gwei
              FROM hourly_price p
              JOIN hourly_gas g USING (hour_ts)
            )
            INSERT INTO {DW_SCHEMA}.analysis_eth_price_vs_gas_fee_hourly (
              hour_ts, avg_close_usdt, avg_volume_usdt, avg_base_fee_gwei
            )
            SELECT hour_ts, avg_close_usdt, avg_volume_usdt, avg_base_fee_gwei
            FROM joined
            ON CONFLICT (hour_ts) DO UPDATE SET
              avg_close_usdt = EXCLUDED.avg_close_usdt,
              avg_volume_usdt = EXCLUDED.avg_volume_usdt,
              avg_base_fee_gwei = EXCLUDED.avg_base_fee_gwei,
              computed_at = NOW()
            """

            params = {
                "exchange_name": settings.exchange_name,
                "asset_symbol": settings.asset_symbol,
                "binance_interval": settings.binance_interval,
                "chain_name": settings.chain_name,
                "window_start": window_start,
                "window_end": window_end,
            }

            with conn.cursor() as cur:
                cur.execute(upsert_hourly_sql, params)
            conn.commit()

            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                      corr(avg_close_usdt, avg_base_fee_gwei) AS corr_price_vs_base_fee,
                      corr(avg_volume_usdt, avg_base_fee_gwei) AS corr_volume_vs_base_fee,
                      COUNT(*) AS n_hours
                    FROM {DW_SCHEMA}.analysis_eth_price_vs_gas_fee_hourly
                    WHERE hour_ts >= %(window_start)s
                      AND hour_ts < %(window_end)s
                    """,
                    params,
                )
                row = cur.fetchone()
                corr_price_val: Optional[float] = row[0] if row else None
                corr_volume_val: Optional[float] = row[1] if row else None
                n_hours = int(row[2]) if row and row[2] is not None else 0

                if n_hours < 3:
                    raise AirflowSkipException(
                        f"Not enough overlapping hours to compute correlation (n_hours={n_hours})."
                    )

                cur.execute(
                    f"""
                    INSERT INTO {DW_SCHEMA}.analysis_eth_price_vs_gas_fee_summary (
                      window_start, window_end,
                      asset_symbol, binance_interval, exchange_name, chain_name,
                      n_hours, corr_price_vs_base_fee_gwei, corr_volume_vs_base_fee_gwei
                    )
                    VALUES (
                      %(window_start)s, %(window_end)s,
                      %(asset_symbol)s, %(binance_interval)s, %(exchange_name)s, %(chain_name)s,
                      %(n_hours)s, %(corr_price_val)s, %(corr_volume_val)s
                    )
                    """,
                    {**params, "n_hours": n_hours, "corr_price_val": corr_price_val, "corr_volume_val": corr_volume_val},
                )
            conn.commit()

            return {
                "window_start_utc": window_start.to_iso8601_string(),
                "window_end_utc": window_end.to_iso8601_string(),
                "asset_symbol": settings.asset_symbol,
                "binance_interval": settings.binance_interval,
                "n_hours": n_hours,
                "corr_price_vs_base_fee_gwei": corr_price_val,
                "corr_volume_vs_base_fee_gwei": corr_volume_val,
                "hourly_table": f"{DW_SCHEMA}.analysis_eth_price_vs_gas_fee_hourly",
                "summary_table": f"{DW_SCHEMA}.analysis_eth_price_vs_gas_fee_summary",
            }
        finally:
            conn.close()

    @task
    def create_plot(settings_dict: Dict[str, Any], result: Dict[str, Any]) -> Dict[str, Any]:
        settings = Settings(**settings_dict)
        conn = _pg_connect(settings)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT hour_ts, avg_close_usdt, avg_volume_usdt, avg_base_fee_gwei
                    FROM {DW_SCHEMA}.analysis_eth_price_vs_gas_fee_hourly
                    WHERE hour_ts >= %(window_start)s
                      AND hour_ts < %(window_end)s
                    ORDER BY hour_ts
                    """,
                    {
                        "window_start": result["window_start_utc"],
                        "window_end": result["window_end_utc"],
                    },
                )
                rows = cur.fetchall() or []

            if len(rows) < 3:
                raise AirflowSkipException(
                    f"Not enough data points to create plot (n={len(rows)})."
                )

            try:
                import matplotlib

                matplotlib.use("Agg")  # headless (Docker/Airflow)
                import matplotlib.pyplot as plt  # noqa: E402
                import numpy as np  # noqa: E402
            except ModuleNotFoundError as exc:
                raise AirflowSkipException(f"Missing plotting libraries: {exc}") from exc

            hour_ts = [r[0] for r in rows]
            price = [float(r[1]) if r[1] is not None else float("nan") for r in rows]
            volume = [float(r[2]) if r[2] is not None else float("nan") for r in rows]
            base_fee = [
                float(r[3]) if r[3] is not None else float("nan") for r in rows
            ]

            out_dir = Path(settings.output_dir)
            out_dir.mkdir(parents=True, exist_ok=True)
            run_ts = pendulum.now("UTC").format("YYYYMMDDTHHmmss") + "Z"
            out_path = out_dir / f"{run_ts}_{settings.asset_symbol}_{settings.binance_interval}.png"

            fig = plt.figure(figsize=(16, 10))

            corr_price = result.get("corr_price_vs_base_fee_gwei")
            corr_volume = result.get("corr_volume_vs_base_fee_gwei")

            # (1) Price vs Base Fee - Time series
            ax1 = fig.add_subplot(2, 2, 1)
            ax1.plot(hour_ts, price, color="tab:blue", linewidth=1.2, label="ETH close (USDT)")
            ax1.set_ylabel("ETH close (USDT)", color="tab:blue")
            ax1.tick_params(axis="y", labelcolor="tab:blue")
            ax1.grid(True, alpha=0.25)
            ax1.set_title(f"Price vs Base Fee | corr={corr_price:.3f}" if corr_price else "Price vs Base Fee")

            ax1b = ax1.twinx()
            ax1b.plot(hour_ts, base_fee, color="tab:orange", linewidth=1.2, label="Base fee (gwei)")
            ax1b.set_ylabel("Base fee (gwei)", color="tab:orange")
            ax1b.tick_params(axis="y", labelcolor="tab:orange")

            # (2) Volume vs Base Fee - Time series
            ax2 = fig.add_subplot(2, 2, 2)
            ax2.plot(hour_ts, volume, color="tab:green", linewidth=1.2, label="Volume (USDT)")
            ax2.set_ylabel("Volume (USDT)", color="tab:green")
            ax2.tick_params(axis="y", labelcolor="tab:green")
            ax2.grid(True, alpha=0.25)
            ax2.set_title(f"Volume vs Base Fee | corr={corr_volume:.3f}" if corr_volume else "Volume vs Base Fee")

            ax2b = ax2.twinx()
            ax2b.plot(hour_ts, base_fee, color="tab:orange", linewidth=1.2, label="Base fee (gwei)")
            ax2b.set_ylabel("Base fee (gwei)", color="tab:orange")
            ax2b.tick_params(axis="y", labelcolor="tab:orange")

            # (3) Price vs Base Fee - Scatter
            ax3 = fig.add_subplot(2, 2, 3)
            ax3.scatter(base_fee, price, s=20, alpha=0.6, color="tab:blue")
            ax3.set_xlabel("Avg base fee (gwei)")
            ax3.set_ylabel("Avg close (USDT)")
            ax3.grid(True, alpha=0.25)

            x_price = np.array(base_fee, dtype=float)
            y_price = np.array(price, dtype=float)
            mask_price = np.isfinite(x_price) & np.isfinite(y_price)
            if mask_price.sum() >= 2:
                m, b = np.polyfit(x_price[mask_price], y_price[mask_price], 1)
                x_line = np.linspace(x_price[mask_price].min(), x_price[mask_price].max(), 100)
                ax3.plot(x_line, m * x_line + b, color="black", linewidth=1.2, alpha=0.8)

            # (4) Volume vs Base Fee - Scatter
            ax4 = fig.add_subplot(2, 2, 4)
            ax4.scatter(base_fee, volume, s=20, alpha=0.6, color="tab:green")
            ax4.set_xlabel("Avg base fee (gwei)")
            ax4.set_ylabel("Avg volume (USDT)")
            ax4.grid(True, alpha=0.25)

            x_vol = np.array(base_fee, dtype=float)
            y_vol = np.array(volume, dtype=float)
            mask_vol = np.isfinite(x_vol) & np.isfinite(y_vol)
            if mask_vol.sum() >= 2:
                m, b = np.polyfit(x_vol[mask_vol], y_vol[mask_vol], 1)
                x_line = np.linspace(x_vol[mask_vol].min(), x_vol[mask_vol].max(), 100)
                ax4.plot(x_line, m * x_line + b, color="black", linewidth=1.2, alpha=0.8)

            fig.tight_layout()
            fig.savefig(out_path, dpi=150)
            plt.close(fig)

            return {"plot_path": str(out_path)}
        finally:
            conn.close()

    @task(trigger_rule="all_done")
    def log_summary(
        result: Dict[str, Any],
        plot_result: Dict[str, Any],
        cmc_result: Dict[str, Any],
        integrated_result: Dict[str, Any],
    ) -> None:
        print(
            f"analysis_eth_price_vs_gas_fee: {result} | plot: {plot_result} | cmc: {cmc_result} | integrated: {integrated_result}"
        )

    settings = get_settings()
    result = run_analysis(settings)
    plot_result = create_plot(settings, result)
    cmc_result = run_coinmarketcap_analysis(settings)
    integrated_result = run_integrated_daily_analysis(settings)
    log_summary(result, plot_result, cmc_result, integrated_result)


pipeline_4_analytics()
