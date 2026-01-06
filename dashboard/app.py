import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import utils


st.set_page_config(
    page_title="BlocFlow",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("📊 Cripto Analysis")
st.markdown("Interactive dashboard for analyzing the correlation between Ethereum price and on-chain gas fees")


def main():
    conn = utils.get_db_connection()

    st.sidebar.header("Filters & Settings")

    # Default dates for sample data (adjust these to match your data range)
    default_start = datetime(2026, 1, 1).date()
    default_end = datetime(2026, 1, 7).date()

    start_date = st.sidebar.date_input(
        "Start Date",
        value=default_start,
    )

    end_date = st.sidebar.date_input(
        "End Date",
        value=default_end,
        min_value=start_date,
    )

    if st.sidebar.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.sidebar.markdown("---")
    st.sidebar.markdown("### About")
    st.sidebar.markdown(
        """
        This dashboard visualizes the correlation between:
        - **Off-chain**: ETH price and trading volume (Binance)
        - **On-chain**: Gas fees in gwei (Ethereum base fee)

        Data is aggregated hourly and sourced from the PostgreSQL data warehouse.
        """
    )

    with st.spinner("Loading data..."):
        try:
            hourly_df = utils.fetch_hourly_data(
                start_date=str(start_date),
                end_date=str(end_date),
                _conn=conn,
            )
            summary_stats = utils.fetch_summary_stats(
                start_date=str(start_date),
                end_date=str(end_date),
                _conn=conn,
            )
            if isinstance(summary_stats, pd.DataFrame):
                summary_stats = summary_stats.iloc[0].to_dict() if not summary_stats.empty else {}
            elif isinstance(summary_stats, pd.Series):
                summary_stats = summary_stats.to_dict()
            elif summary_stats is None:
                summary_stats = {}

            historical_df = utils.fetch_historical_trends(
                lookback_runs=20,
                _conn=conn,
            )

        except Exception as e:
            st.error(f"Error loading data: {e}")
            import traceback
            st.error(traceback.format_exc())
            st.stop()

    if hourly_df.empty:
        st.warning("No data available for the selected date range. Please adjust the filters or ensure the analysis DAG has run.")
        st.stop()

    tabs = st.tabs(["📈 Overview", "⏱️ Time Series", "🔗 Correlation", "🪙 CoinMarketCap", "📊 Historical Trends", "📋 Raw Data"])

    with tabs[0]:
        st.header("Overview & Key Metrics")

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(
                label="Data Points",
                value=summary_stats.get('n_hours', 0),
            )

        with col2:
            corr_price = summary_stats.get('corr_price_vs_base_fee')
            st.metric(
                label="Price ↔ Gas Fee",
                value=utils.format_correlation(corr_price),
            )

        with col3:
            corr_volume = summary_stats.get('corr_volume_vs_base_fee')
            st.metric(
                label="Volume ↔ Gas Fee",
                value=utils.format_correlation(corr_volume),
            )

        with col4:
            avg_price = summary_stats.get('avg_price')
            st.metric(
                label="Avg ETH Price",
                value=utils.format_number(avg_price, precision=2, suffix=" USDT"),
            )

        st.markdown("---")

        col1, col2, col3 = st.columns(3)

        with col1:
            avg_volume = summary_stats.get('avg_volume')
            st.metric(
                label="Avg Trading Volume",
                value=utils.format_number(avg_volume, precision=2, suffix=" USDT"),
            )

        with col2:
            avg_base_fee = summary_stats.get('avg_base_fee')
            st.metric(
                label="Avg Base Fee",
                value=utils.format_number(avg_base_fee, precision=2, suffix=" gwei"),
            )

        with col3:
            first_hour = summary_stats.get('first_hour')
            last_hour = summary_stats.get('last_hour')
            if first_hour and last_hour:
                duration_hours = (pd.to_datetime(last_hour) - pd.to_datetime(first_hour)).total_seconds() / 3600
                st.metric(
                    label="Time Span",
                    value=f"{duration_hours:.0f} hours",
                )

        st.markdown("---")
        st.subheader("Interpretation Guide")

        col1, col2 = st.columns(2)

        with col1:
            st.markdown(
                """
                **Correlation Strength:**
                - 🟢 **Strong** (|r| > 0.7): Strong linear relationship
                - 🟡 **Moderate** (0.3 < |r| < 0.7): Moderate relationship
                - ⚪ **Weak** (|r| < 0.3): Weak or no relationship

                **Sign:**
                - Positive correlation: Both variables move in the same direction
                - Negative correlation: Variables move in opposite directions
                """
            )

        with col2:
            st.markdown(
                """
                **What to Look For:**
                - **Price vs Gas Fee**: Does ETH price movement correlate with network activity?
                - **Volume vs Gas Fee**: Does trading volume correlate with on-chain congestion?
                - **Historical Trends**: How do correlations evolve over time?

                **Note:** Correlation does not imply causation.
                """
            )

    with tabs[1]:
        st.header("Time Series Analysis")

        st.subheader("ETH Price vs Base Fee Over Time")
        corr_price = summary_stats.get('corr_price_vs_base_fee')
        fig_price = utils.create_dual_axis_chart(
            df=hourly_df,
            y1_col='avg_close_usdt',
            y2_col='avg_base_fee_gwei',
            y1_name='ETH Close Price (USDT)',
            y2_name='Base Fee (gwei)',
            title='ETH Price vs Gas Fee',
            corr_value=corr_price,
            y1_color='blue',
            y2_color='orange',
        )
        st.plotly_chart(fig_price, use_container_width=True)

        st.markdown("---")

        st.subheader("Trading Volume vs Base Fee Over Time")
        corr_volume = summary_stats.get('corr_volume_vs_base_fee')
        fig_volume = utils.create_dual_axis_chart(
            df=hourly_df,
            y1_col='avg_volume_usdt',
            y2_col='avg_base_fee_gwei',
            y1_name='Trading Volume (USDT)',
            y2_name='Base Fee (gwei)',
            title='Trading Volume vs Gas Fee',
            corr_value=corr_volume,
            y1_color='green',
            y2_color='orange',
        )
        st.plotly_chart(fig_volume, use_container_width=True)

    with tabs[2]:
        st.header("Correlation Analysis")

        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Price vs Base Fee Scatter")
            corr_price = summary_stats.get('corr_price_vs_base_fee')
            fig_scatter_price = utils.create_scatter_plot(
                df=hourly_df,
                x_col='avg_base_fee_gwei',
                y_col='avg_close_usdt',
                x_name='Avg Base Fee (gwei)',
                y_name='Avg ETH Price (USDT)',
                title='Price vs Gas Fee',
                corr_value=corr_price,
                color='blue',
            )
            st.plotly_chart(fig_scatter_price, use_container_width=True)

        with col2:
            st.subheader("Volume vs Base Fee Scatter")
            corr_volume = summary_stats.get('corr_volume_vs_base_fee')
            fig_scatter_volume = utils.create_scatter_plot(
                df=hourly_df,
                x_col='avg_base_fee_gwei',
                y_col='avg_volume_usdt',
                x_name='Avg Base Fee (gwei)',
                y_name='Avg Volume (USDT)',
                title='Volume vs Gas Fee',
                corr_value=corr_volume,
                color='green',
            )
            st.plotly_chart(fig_scatter_volume, use_container_width=True)

        st.markdown("---")
        st.subheader("Statistical Summary")

        if not hourly_df.empty:
            stats_df = hourly_df[['avg_close_usdt', 'avg_volume_usdt', 'avg_base_fee_gwei']].describe()
            st.dataframe(stats_df, use_container_width=True)

    with tabs[3]:
        st.header("CoinMarketCap Analysis")
        st.markdown("Shows CoinMarketCap-derived analytics from the data warehouse (if available).")

        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Top Assets Snapshot")
            with st.spinner("Loading CoinMarketCap snapshot..."):
                try:
                    snapshot_ts = utils.fetch_cmc_latest_snapshot_ts(
                        start_date=str(start_date),
                        end_date=str(end_date),
                        _conn=conn,
                    )
                    if snapshot_ts is None:
                        cmc_top_df = pd.DataFrame()
                    else:
                        cmc_top_df = utils.fetch_cmc_top_assets_for_snapshot(
                            snapshot_ts=snapshot_ts,
                            _conn=conn,
                        )
                except Exception as e:
                    snapshot_ts = None
                    cmc_top_df = pd.DataFrame()
                    st.error(f"Error loading CoinMarketCap snapshot: {e}")

            if cmc_top_df.empty:
                st.info("No CoinMarketCap snapshot found. Run `pipeline_4_analytics` (CoinMarketCap section) to populate `dw.analysis_coinmarketcap_top_assets`.")
            else:
                st.caption(f"Snapshot: {pd.to_datetime(snapshot_ts).strftime('%Y-%m-%d %H:%M:%S UTC')}")

                display_df = cmc_top_df[[
                    "rank",
                    "symbol",
                    "name",
                    "price_usd",
                    "market_cap",
                    "volume_24h",
                    "percent_change_24h",
                ]].copy()
                display_df.columns = [
                    "Rank",
                    "Symbol",
                    "Name",
                    "Price (USD)",
                    "Market Cap (USD)",
                    "Volume 24h (USD)",
                    "% Change 24h",
                ]
                st.dataframe(display_df, use_container_width=True, hide_index=True)

                try:
                    import plotly.graph_objects as go

                    fig = go.Figure(
                        data=[
                            go.Bar(
                                x=cmc_top_df["symbol"],
                                y=cmc_top_df["market_cap"],
                                name="Market Cap",
                                hovertemplate="<b>%{x}</b><br>Market Cap: %{y:,.0f} USD<extra></extra>",
                            )
                        ]
                    )
                    fig.update_layout(
                        title="Market Cap by Asset (Top Snapshot)",
                        xaxis_title="Asset",
                        yaxis_title="Market Cap (USD)",
                        height=350,
                    )
                    st.plotly_chart(fig, use_container_width=True)
                except Exception:
                    pass

        with col2:
            st.subheader("Integrated (CMC vs Binance vs Ethereum)")
            granularity = st.radio(
                "Granularity",
                ["Daily", "10-minute"],
                horizontal=True,
                key="integrated_granularity",
            )

            with st.spinner("Loading integrated analysis..."):
                try:
                    if granularity == "Daily":
                        integrated_df = utils.fetch_integrated_daily_data(
                            start_date=str(start_date),
                            end_date=str(end_date),
                            _conn=conn,
                        )
                        ts_col = "trading_date"
                    else:
                        integrated_df = utils.fetch_integrated_10m_data(
                            start_date=str(start_date),
                            end_date=str(end_date),
                            _conn=conn,
                        )
                        ts_col = "bucket_ts"
                except Exception as e:
                    integrated_df = pd.DataFrame()
                    st.error(f"Error loading integrated analysis: {e}")

            if integrated_df.empty:
                if granularity == "Daily":
                    st.info("No integrated daily data found. Run `pipeline_4_analytics` to populate `dw.analysis_integrated_daily`.")
                else:
                    st.info("No integrated 10-minute data found. Ensure DW tables are populated (Binance/Ethereum/CMC).")
            else:
                base_symbol = "N/A"
                if "base_symbol" in integrated_df.columns:
                    non_null_symbols = integrated_df["base_symbol"].dropna()
                    if not non_null_symbols.empty:
                        base_symbol = non_null_symbols.iloc[0]
                st.caption(f"Base symbol: {base_symbol}")

                try:
                    import plotly.graph_objects as go

                    fig_price = go.Figure()
                    fig_price.add_trace(
                        go.Scatter(
                            x=integrated_df[ts_col],
                            y=integrated_df["cmc_price_usd"],
                            name="CMC Price (USD)",
                            mode="lines+markers",
                        )
                    )
                    fig_price.add_trace(
                        go.Scatter(
                            x=integrated_df[ts_col],
                            y=integrated_df["binance_avg_close_usdt"],
                            name="Binance Avg Close (USDT)",
                            mode="lines+markers",
                        )
                    )
                    fig_price.update_layout(
                        title=f"CMC vs Binance Price ({granularity})",
                        xaxis_title="Time",
                        yaxis_title="Price",
                        height=350,
                        hovermode="x unified",
                    )
                    st.plotly_chart(fig_price, use_container_width=True)

                    fig_fee = go.Figure(
                        data=[
                            go.Scatter(
                                x=integrated_df[ts_col],
                                y=integrated_df["eth_avg_base_fee_gwei"],
                                name="ETH Avg Base Fee (gwei)",
                                mode="lines+markers",
                            )
                        ]
                    )
                    fig_fee.update_layout(
                        title=f"Ethereum Base Fee ({granularity})",
                        xaxis_title="Time",
                        yaxis_title="Base Fee (gwei)",
                        height=300,
                        hovermode="x unified",
                    )
                    st.plotly_chart(fig_fee, use_container_width=True)
                except Exception:
                    pass

                if granularity == "Daily":
                    display_df = integrated_df[[
                        "trading_date",
                        "cmc_price_usd",
                        "cmc_market_cap",
                        "binance_avg_close_usdt",
                        "binance_avg_volume_usdt",
                        "eth_avg_base_fee_gwei",
                    ]].copy()
                    display_df["trading_date"] = display_df["trading_date"].dt.strftime("%Y-%m-%d")
                    display_df.columns = [
                        "Date",
                        "CMC Price (USD)",
                        "CMC Market Cap (USD)",
                        "Binance Avg Close (USDT)",
                        "Binance Avg Volume (USDT)",
                        "ETH Avg Base Fee (gwei)",
                    ]
                else:
                    display_df = integrated_df[[
                        "bucket_ts",
                        "cmc_price_usd",
                        "cmc_market_cap",
                        "binance_avg_close_usdt",
                        "binance_avg_volume_usdt",
                        "eth_avg_base_fee_gwei",
                    ]].copy()
                    display_df["bucket_ts"] = display_df["bucket_ts"].dt.strftime("%Y-%m-%d %H:%M")
                    display_df.columns = [
                        "Time (UTC)",
                        "CMC Price (USD)",
                        "CMC Market Cap (USD)",
                        "Binance Avg Close (USDT)",
                        "Binance Avg Volume (USDT)",
                        "ETH Avg Base Fee (gwei)",
                    ]
                st.dataframe(display_df, use_container_width=True, hide_index=True)

    with tabs[4]:
        st.header("Historical Correlation Trends")

        if not historical_df.empty:
            st.subheader("Correlation Evolution Over Multiple Runs")

            import plotly.graph_objects as go

            fig = go.Figure()

            fig.add_trace(
                go.Scatter(
                    x=historical_df['run_ts'],
                    y=historical_df['corr_price_vs_base_fee_gwei'],
                    mode='lines+markers',
                    name='Price vs Gas Fee',
                    line=dict(color='blue', width=2),
                    marker=dict(size=8),
                )
            )

            fig.add_trace(
                go.Scatter(
                    x=historical_df['run_ts'],
                    y=historical_df['corr_volume_vs_base_fee_gwei'],
                    mode='lines+markers',
                    name='Volume vs Gas Fee',
                    line=dict(color='green', width=2),
                    marker=dict(size=8),
                )
            )

            fig.update_layout(
                title='Historical Correlation Trends',
                xaxis_title='Analysis Run Time',
                yaxis_title='Correlation Coefficient',
                height=500,
                hovermode='x unified',
            )

            st.plotly_chart(fig, use_container_width=True)

            st.markdown("---")
            st.subheader("Historical Runs Summary")
            display_df = historical_df[[
                'run_ts',
                'window_start',
                'window_end',
                'n_hours',
                'corr_price_vs_base_fee_gwei',
                'corr_volume_vs_base_fee_gwei',
            ]].copy()

            display_df.columns = [
                'Run Time',
                'Window Start',
                'Window End',
                'Hours',
                'Price Corr',
                'Volume Corr',
            ]

            st.dataframe(display_df, use_container_width=True, hide_index=True)
        else:
            st.info("No historical data available. The analysis DAG needs to run multiple times to build historical trends.")

    with tabs[5]:
        st.header("Raw Data")

        st.subheader("Hourly Aggregated Data")
        st.markdown(f"Showing {len(hourly_df)} records from {start_date} to {end_date}")

        display_df = hourly_df.copy()
        display_df['hour_ts'] = display_df['hour_ts'].dt.strftime('%Y-%m-%d %H:%M')

        st.dataframe(display_df, use_container_width=True, hide_index=True)

        csv = hourly_df.to_csv(index=False)
        st.download_button(
            label="📥 Download CSV",
            data=csv,
            file_name=f"eth_price_vs_gas_fee_{start_date}_{end_date}.csv",
            mime="text/csv",
        )

    st.markdown("---")
    st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")


if __name__ == "__main__":
    main()
