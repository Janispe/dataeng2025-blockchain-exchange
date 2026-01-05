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
    schedule="@daily",
    catchup=False,
    tags=["pipeline-4", "analytics", "analysis", "correlation"],
    default_args=dict(retries=2, retry_delay=pendulum.duration(minutes=2)),
    description="Pipeline 4: Analytics - Analysiert Korrelation zwischen ETH-Preis (Binance, CoinMarketCap) und Gas-Gebühren (Ethereum).",
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
        return settings.__dict__

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
                      avg_cmc_price_usd NUMERIC,
                      avg_cmc_market_cap NUMERIC,
                      avg_cmc_volume_24h NUMERIC,
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
                      corr_volume_vs_base_fee_gwei NUMERIC,
                      corr_cmc_price_vs_base_fee_gwei NUMERIC,
                      corr_cmc_market_cap_vs_base_fee_gwei NUMERIC,
                      corr_binance_vs_cmc_price NUMERIC
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
            hourly_cmc AS (
              SELECT
                date_trunc('hour', t.ts_utc) AS hour_ts,
                AVG(f.price_usd) AS avg_cmc_price_usd,
                AVG(f.market_cap) AS avg_cmc_market_cap,
                AVG(f.volume_24h) AS avg_cmc_volume_24h
              FROM {DW_SCHEMA}.fact_crypto_market f
              JOIN {DW_SCHEMA}.dim_time t ON t.time_key = f.recorded_time_key
              JOIN {DW_SCHEMA}.dim_asset a ON a.asset_key = f.asset_key
              WHERE a.base_asset = 'ETH'
                AND f.price_usd IS NOT NULL
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
              SELECT
                COALESCE(p.hour_ts, cmc.hour_ts, g.hour_ts) AS hour_ts,
                p.avg_close_usdt,
                p.avg_volume_usdt,
                cmc.avg_cmc_price_usd,
                cmc.avg_cmc_market_cap,
                cmc.avg_cmc_volume_24h,
                g.avg_base_fee_gwei
              FROM hourly_price p
              FULL OUTER JOIN hourly_cmc cmc USING (hour_ts)
              FULL OUTER JOIN hourly_gas g USING (hour_ts)
              WHERE COALESCE(p.hour_ts, cmc.hour_ts, g.hour_ts) IS NOT NULL
            )
            INSERT INTO {DW_SCHEMA}.analysis_eth_price_vs_gas_fee_hourly (
              hour_ts, avg_close_usdt, avg_volume_usdt,
              avg_cmc_price_usd, avg_cmc_market_cap, avg_cmc_volume_24h,
              avg_base_fee_gwei
            )
            SELECT
              hour_ts, avg_close_usdt, avg_volume_usdt,
              avg_cmc_price_usd, avg_cmc_market_cap, avg_cmc_volume_24h,
              avg_base_fee_gwei
            FROM joined
            ON CONFLICT (hour_ts) DO UPDATE SET
              avg_close_usdt = EXCLUDED.avg_close_usdt,
              avg_volume_usdt = EXCLUDED.avg_volume_usdt,
              avg_cmc_price_usd = EXCLUDED.avg_cmc_price_usd,
              avg_cmc_market_cap = EXCLUDED.avg_cmc_market_cap,
              avg_cmc_volume_24h = EXCLUDED.avg_cmc_volume_24h,
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
                      corr(avg_cmc_price_usd, avg_base_fee_gwei) AS corr_cmc_price_vs_base_fee,
                      corr(avg_cmc_market_cap, avg_base_fee_gwei) AS corr_cmc_market_cap_vs_base_fee,
                      corr(avg_close_usdt, avg_cmc_price_usd) AS corr_binance_vs_cmc_price,
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
                corr_cmc_price_val: Optional[float] = row[2] if row else None
                corr_cmc_market_cap_val: Optional[float] = row[3] if row else None
                corr_binance_vs_cmc_val: Optional[float] = row[4] if row else None
                n_hours = int(row[5]) if row and row[5] is not None else 0

                if n_hours < 3:
                    raise AirflowSkipException(
                        f"Not enough overlapping hours to compute correlation (n_hours={n_hours})."
                    )

                cur.execute(
                    f"""
                    INSERT INTO {DW_SCHEMA}.analysis_eth_price_vs_gas_fee_summary (
                      window_start, window_end,
                      asset_symbol, binance_interval, exchange_name, chain_name,
                      n_hours, corr_price_vs_base_fee_gwei, corr_volume_vs_base_fee_gwei,
                      corr_cmc_price_vs_base_fee_gwei, corr_cmc_market_cap_vs_base_fee_gwei,
                      corr_binance_vs_cmc_price
                    )
                    VALUES (
                      %(window_start)s, %(window_end)s,
                      %(asset_symbol)s, %(binance_interval)s, %(exchange_name)s, %(chain_name)s,
                      %(n_hours)s, %(corr_price_val)s, %(corr_volume_val)s,
                      %(corr_cmc_price_val)s, %(corr_cmc_market_cap_val)s, %(corr_binance_vs_cmc_val)s
                    )
                    """,
                    {
                        **params,
                        "n_hours": n_hours,
                        "corr_price_val": corr_price_val,
                        "corr_volume_val": corr_volume_val,
                        "corr_cmc_price_val": corr_cmc_price_val,
                        "corr_cmc_market_cap_val": corr_cmc_market_cap_val,
                        "corr_binance_vs_cmc_val": corr_binance_vs_cmc_val,
                    },
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
                "corr_cmc_price_vs_base_fee_gwei": corr_cmc_price_val,
                "corr_cmc_market_cap_vs_base_fee_gwei": corr_cmc_market_cap_val,
                "corr_binance_vs_cmc_price": corr_binance_vs_cmc_val,
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
                    SELECT hour_ts, avg_close_usdt, avg_volume_usdt,
                           avg_cmc_price_usd, avg_cmc_market_cap, avg_cmc_volume_24h,
                           avg_base_fee_gwei
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
            cmc_price = [float(r[3]) if r[3] is not None else float("nan") for r in rows]
            cmc_market_cap = [float(r[4]) if r[4] is not None else float("nan") for r in rows]
            cmc_volume_24h = [float(r[5]) if r[5] is not None else float("nan") for r in rows]
            base_fee = [float(r[6]) if r[6] is not None else float("nan") for r in rows]

            out_dir = Path(settings.output_dir)
            out_dir.mkdir(parents=True, exist_ok=True)
            run_ts = pendulum.now("UTC").format("YYYYMMDDTHHmmss") + "Z"
            out_path = out_dir / f"{run_ts}_{settings.asset_symbol}_{settings.binance_interval}.png"

            fig = plt.figure(figsize=(20, 12))

            corr_price = result.get("corr_price_vs_base_fee_gwei")
            corr_volume = result.get("corr_volume_vs_base_fee_gwei")
            corr_cmc_price = result.get("corr_cmc_price_vs_base_fee_gwei")
            corr_cmc_market_cap = result.get("corr_cmc_market_cap_vs_base_fee_gwei")
            corr_binance_vs_cmc = result.get("corr_binance_vs_cmc_price")

            # ===== ROW 1: TIME SERIES =====

            # (1) Binance Price vs Base Fee - Time series
            ax1 = fig.add_subplot(2, 3, 1)
            ax1.plot(hour_ts, price, color="tab:blue", linewidth=1.2, label="Binance ETH (USDT)")
            ax1.set_ylabel("Binance ETH (USDT)", color="tab:blue")
            ax1.tick_params(axis="y", labelcolor="tab:blue")
            ax1.grid(True, alpha=0.25)
            ax1.set_title(f"Binance Price vs Gas | corr={corr_price:.3f}" if corr_price else "Binance Price vs Gas")

            ax1b = ax1.twinx()
            ax1b.plot(hour_ts, base_fee, color="tab:orange", linewidth=1.2, label="Base fee (gwei)")
            ax1b.set_ylabel("Base fee (gwei)", color="tab:orange")
            ax1b.tick_params(axis="y", labelcolor="tab:orange")

            # (2) CMC Price vs Base Fee - Time series
            ax2 = fig.add_subplot(2, 3, 2)
            ax2.plot(hour_ts, cmc_price, color="tab:purple", linewidth=1.2, label="CMC ETH (USD)")
            ax2.set_ylabel("CMC ETH (USD)", color="tab:purple")
            ax2.tick_params(axis="y", labelcolor="tab:purple")
            ax2.grid(True, alpha=0.25)
            ax2.set_title(f"CMC Price vs Gas | corr={corr_cmc_price:.3f}" if corr_cmc_price else "CMC Price vs Gas")

            ax2b = ax2.twinx()
            ax2b.plot(hour_ts, base_fee, color="tab:orange", linewidth=1.2, label="Base fee (gwei)")
            ax2b.set_ylabel("Base fee (gwei)", color="tab:orange")
            ax2b.tick_params(axis="y", labelcolor="tab:orange")

            # (3) Binance vs CMC Price Comparison - Time series
            ax3 = fig.add_subplot(2, 3, 3)
            ax3.plot(hour_ts, price, color="tab:blue", linewidth=1.2, label="Binance", alpha=0.7)
            ax3.plot(hour_ts, cmc_price, color="tab:purple", linewidth=1.2, label="CoinMarketCap", alpha=0.7)
            ax3.set_ylabel("Price (USD/USDT)")
            ax3.grid(True, alpha=0.25)
            ax3.legend(loc="upper left")
            ax3.set_title(f"Binance vs CMC | corr={corr_binance_vs_cmc:.3f}" if corr_binance_vs_cmc else "Binance vs CMC")

            # ===== ROW 2: SCATTER PLOTS =====

            # (4) Binance Price vs Base Fee - Scatter
            ax4 = fig.add_subplot(2, 3, 4)
            ax4.scatter(base_fee, price, s=20, alpha=0.6, color="tab:blue")
            ax4.set_xlabel("Avg base fee (gwei)")
            ax4.set_ylabel("Binance close (USDT)")
            ax4.grid(True, alpha=0.25)

            x_price = np.array(base_fee, dtype=float)
            y_price = np.array(price, dtype=float)
            mask_price = np.isfinite(x_price) & np.isfinite(y_price)
            if mask_price.sum() >= 2:
                m, b = np.polyfit(x_price[mask_price], y_price[mask_price], 1)
                x_line = np.linspace(x_price[mask_price].min(), x_price[mask_price].max(), 100)
                ax4.plot(x_line, m * x_line + b, color="black", linewidth=1.2, alpha=0.8)

            # (5) CMC Price vs Base Fee - Scatter
            ax5 = fig.add_subplot(2, 3, 5)
            ax5.scatter(base_fee, cmc_price, s=20, alpha=0.6, color="tab:purple")
            ax5.set_xlabel("Avg base fee (gwei)")
            ax5.set_ylabel("CMC price (USD)")
            ax5.grid(True, alpha=0.25)

            x_cmc = np.array(base_fee, dtype=float)
            y_cmc = np.array(cmc_price, dtype=float)
            mask_cmc = np.isfinite(x_cmc) & np.isfinite(y_cmc)
            if mask_cmc.sum() >= 2:
                m, b = np.polyfit(x_cmc[mask_cmc], y_cmc[mask_cmc], 1)
                x_line = np.linspace(x_cmc[mask_cmc].min(), x_cmc[mask_cmc].max(), 100)
                ax5.plot(x_line, m * x_line + b, color="black", linewidth=1.2, alpha=0.8)

            # (6) Binance vs CMC Price - Scatter
            ax6 = fig.add_subplot(2, 3, 6)
            ax6.scatter(cmc_price, price, s=20, alpha=0.6, color="tab:cyan")
            ax6.set_xlabel("CMC price (USD)")
            ax6.set_ylabel("Binance close (USDT)")
            ax6.grid(True, alpha=0.25)

            x_comp = np.array(cmc_price, dtype=float)
            y_comp = np.array(price, dtype=float)
            mask_comp = np.isfinite(x_comp) & np.isfinite(y_comp)
            if mask_comp.sum() >= 2:
                m, b = np.polyfit(x_comp[mask_comp], y_comp[mask_comp], 1)
                x_line = np.linspace(x_comp[mask_comp].min(), x_comp[mask_comp].max(), 100)
                ax6.plot(x_line, m * x_line + b, color="black", linewidth=1.2, alpha=0.8)

            fig.tight_layout()
            fig.savefig(out_path, dpi=150)
            plt.close(fig)

            return {"plot_path": str(out_path)}
        finally:
            conn.close()

    @task(trigger_rule="all_done")
    def log_summary(result: Dict[str, Any], plot_result: Dict[str, Any]) -> None:
        print(f"analysis_eth_price_vs_gas_fee: {result} | plot: {plot_result}")

    settings = get_settings()
    result = run_analysis(settings)
    plot_result = create_plot(settings, result)
    log_summary(result, plot_result)


pipeline_4_analytics()
