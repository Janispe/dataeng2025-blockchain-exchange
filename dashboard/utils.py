import os
from typing import Optional, Any
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np


@st.cache_resource
def get_db_connection():
    """Create and cache database connection."""
    try:
        import psycopg2
        return psycopg2.connect(
            host=os.getenv("DW_PG_HOST", "postgres-data"),
            port=int(os.getenv("DW_PG_PORT", "5432")),
            dbname=os.getenv("DW_PG_DB", "datadb"),
            user=os.getenv("DW_PG_USER", "datauser"),
            password=os.getenv("DW_PG_PASSWORD", "datapass"),
        )
    except ModuleNotFoundError:
        import psycopg
        return psycopg.connect(
            host=os.getenv("DW_PG_HOST", "postgres-data"),
            port=int(os.getenv("DW_PG_PORT", "5432")),
            dbname=os.getenv("DW_PG_DB", "datadb"),
            user=os.getenv("DW_PG_USER", "datauser"),
            password=os.getenv("DW_PG_PASSWORD", "datapass"),
        )


@st.cache_data(ttl=300)
def fetch_hourly_data(
    start_date: str,
    end_date: str,
    _conn: Optional[Any] = None,
) -> pd.DataFrame:
    """Fetch hourly aggregated data from analysis table."""
    if _conn is None:
        _conn = get_db_connection()

    query = """
        SELECT
            hour_ts,
            avg_close_usdt,
            avg_volume_usdt,
            avg_base_fee_gwei,
            computed_at
        FROM dw.analysis_eth_price_vs_gas_fee_hourly
        WHERE hour_ts >= %(start_date)s
          AND hour_ts < %(end_date)s
        ORDER BY hour_ts
    """

    df = pd.read_sql(
        query,
        _conn,
        params={"start_date": start_date, "end_date": end_date},
    )

    if not df.empty:
        df['hour_ts'] = pd.to_datetime(df['hour_ts'])

    return df


@st.cache_data(ttl=300)
def fetch_summary_stats(
    start_date: str,
    end_date: str,
    _conn: Optional[Any] = None,
) -> dict:
    """Fetch summary statistics and correlation values."""
    if _conn is None:
        _conn = get_db_connection()

    query = """
        SELECT
            corr(avg_close_usdt, avg_base_fee_gwei) AS corr_price_vs_base_fee,
            corr(avg_volume_usdt, avg_base_fee_gwei) AS corr_volume_vs_base_fee,
            COUNT(*) AS n_hours,
            MIN(hour_ts) AS first_hour,
            MAX(hour_ts) AS last_hour,
            AVG(avg_close_usdt) AS avg_price,
            AVG(avg_volume_usdt) AS avg_volume,
            AVG(avg_base_fee_gwei) AS avg_base_fee
        FROM dw.analysis_eth_price_vs_gas_fee_hourly
        WHERE hour_ts >= %(start_date)s
          AND hour_ts < %(end_date)s
    """

    df = pd.read_sql(
        query,
        _conn,
        params={"start_date": start_date, "end_date": end_date},
    )

    if df.empty:
        return {}

    return df.iloc[0].to_dict()


@st.cache_data(ttl=300)
def fetch_historical_trends(
    lookback_runs: int = 10,
    _conn: Optional[Any] = None,
) -> pd.DataFrame:
    """Fetch historical correlation trends from summary table."""
    if _conn is None:
        _conn = get_db_connection()

    query = """
        SELECT
            run_ts,
            window_start,
            window_end,
            asset_symbol,
            binance_interval,
            exchange_name,
            chain_name,
            n_hours,
            corr_price_vs_base_fee_gwei,
            corr_volume_vs_base_fee_gwei
        FROM dw.analysis_eth_price_vs_gas_fee_summary
        ORDER BY run_ts DESC
        LIMIT %(limit)s
    """

    df = pd.read_sql(
        query,
        _conn,
        params={"limit": lookback_runs},
    )

    if not df.empty:
        df['run_ts'] = pd.to_datetime(df['run_ts'])
        df['window_start'] = pd.to_datetime(df['window_start'])
        df['window_end'] = pd.to_datetime(df['window_end'])
        df = df.sort_values('run_ts')

    return df


def create_dual_axis_chart(
    df: pd.DataFrame,
    y1_col: str,
    y2_col: str,
    y1_name: str,
    y2_name: str,
    title: str,
    corr_value: Optional[float] = None,
    y1_color: str = "blue",
    y2_color: str = "orange",
) -> go.Figure:
    """Create dual-axis time series chart with Plotly."""
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Primary axis
    fig.add_trace(
        go.Scatter(
            x=df['hour_ts'],
            y=df[y1_col],
            name=y1_name,
            line=dict(color=y1_color, width=2),
            mode='lines',
        ),
        secondary_y=False,
    )

    # Secondary axis
    fig.add_trace(
        go.Scatter(
            x=df['hour_ts'],
            y=df[y2_col],
            name=y2_name,
            line=dict(color=y2_color, width=2),
            mode='lines',
        ),
        secondary_y=True,
    )

    # Update layout
    title_text = title
    if corr_value is not None:
        title_text += f" | Correlation: {corr_value:.3f}"

    fig.update_layout(
        title=title_text,
        hovermode='x unified',
        height=500,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
    )

    fig.update_xaxes(title_text="Time")
    fig.update_yaxes(title_text=y1_name, secondary_y=False)
    fig.update_yaxes(title_text=y2_name, secondary_y=True)

    return fig


def create_scatter_plot(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    x_name: str,
    y_name: str,
    title: str,
    corr_value: Optional[float] = None,
    color: str = "blue",
) -> go.Figure:
    """Create scatter plot with regression line."""
    fig = go.Figure()

    # Scatter points
    fig.add_trace(
        go.Scatter(
            x=df[x_col],
            y=df[y_col],
            mode='markers',
            name='Data Points',
            marker=dict(
                size=8,
                color=color,
                opacity=0.6,
            ),
            hovertemplate=f'<b>{x_name}</b>: %{{x:.2f}}<br><b>{y_name}</b>: %{{y:.2f}}<extra></extra>',
        )
    )

    # Add regression line if enough points
    x_data = df[x_col].dropna()
    y_data = df[y_col].dropna()

    if len(x_data) >= 2 and len(y_data) >= 2:
        # Create mask for finite values
        x_arr = np.array(x_data, dtype=float)
        y_arr = np.array(y_data, dtype=float)
        mask = np.isfinite(x_arr) & np.isfinite(y_arr)

        if mask.sum() >= 2:
            m, b = np.polyfit(x_arr[mask], y_arr[mask], 1)
            x_line = np.linspace(x_arr[mask].min(), x_arr[mask].max(), 100)
            y_line = m * x_line + b

            fig.add_trace(
                go.Scatter(
                    x=x_line,
                    y=y_line,
                    mode='lines',
                    name='Regression Line',
                    line=dict(color='black', width=2, dash='dash'),
                    hoverinfo='skip',
                )
            )

    # Update layout
    title_text = title
    if corr_value is not None:
        title_text += f" | Correlation: {corr_value:.3f}"

    fig.update_layout(
        title=title_text,
        xaxis_title=x_name,
        yaxis_title=y_name,
        height=500,
        hovermode='closest',
    )

    return fig


def format_correlation(value: Optional[float]) -> str:
    """Format correlation value with color coding."""
    if value is None:
        return "N/A"

    color_map = {
        (0.7, 1.0): "🟢",     # Strong positive
        (0.3, 0.7): "🟡",     # Moderate positive
        (-0.3, 0.3): "⚪",    # Weak
        (-0.7, -0.3): "🟠",   # Moderate negative
        (-1.0, -0.7): "🔴",   # Strong negative
    }

    emoji = "⚪"
    for (low, high), em in color_map.items():
        if low <= value < high:
            emoji = em
            break

    return f"{emoji} {value:.3f}"


def format_number(value: Optional[float], precision: int = 2, suffix: str = "") -> str:
    """Format number with human-readable notation."""
    if value is None:
        return "N/A"

    if abs(value) >= 1_000_000:
        return f"{value / 1_000_000:.{precision}f}M{suffix}"
    elif abs(value) >= 1_000:
        return f"{value / 1_000:.{precision}f}K{suffix}"
    else:
        return f"{value:.{precision}f}{suffix}"
