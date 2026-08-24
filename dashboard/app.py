from datetime import datetime, time, timedelta, timezone

import plotly.graph_objects as go
import psycopg2
import streamlit as st

from database import (
    MAX_CANDLES,
    list_market_series,
    list_trade_activity_series,
    load_candles,
    load_trade_activity,
)


def _render_candles():
    st.subheader("Candles")
    try:
        series = list_market_series()
    except psycopg2.Error:
        st.error("The market-data database is currently unavailable.")
        return

    if not series:
        st.info("Gold does not contain any candles yet.")
        return

    selected_series = st.selectbox(
        "Market",
        options=series,
        format_func=lambda value: f"{value[1]} · {value[0]}",
        key="candle_market",
    )
    source, product_id, granularity_seconds, first_candle, last_candle = (
        selected_series
    )

    selected_dates = st.date_input(
        "Date range (UTC)",
        value=(first_candle.date(), last_candle.date()),
        min_value=first_candle.date(),
        max_value=last_candle.date(),
        key="candle_date_range",
    )

    if len(selected_dates) != 2:
        st.info("Select both the start and end date.")
        return

    start_date, end_date = selected_dates
    start = datetime.combine(start_date, time.min, tzinfo=timezone.utc)
    end = datetime.combine(end_date + timedelta(days=1), time.min, tzinfo=timezone.utc)

    try:
        candles = load_candles(
            source=source,
            product_id=product_id,
            granularity_seconds=granularity_seconds,
            start=start,
            end=end,
        )
    except psycopg2.Error:
        st.error("Candles could not be loaded from the database.")
        return

    if not candles:
        st.info("No candles were found for the selected market and date range.")
        return

    timestamps, opening, high, low, close, volume = zip(*candles)
    figure = go.Figure(
        data=[
            go.Candlestick(
                x=timestamps,
                open=opening,
                high=high,
                low=low,
                close=close,
                name=product_id,
            )
        ]
    )
    figure.update_layout(
        title=f"{product_id} · Daily candles",
        xaxis_title="UTC",
        yaxis_title="Price",
        xaxis_rangeslider_visible=False,
        height=650,
    )

    st.plotly_chart(figure, width="stretch")
    st.caption(f"Showing {len(candles):,} candles. Query limit: {MAX_CANDLES:,}.")


def _render_trade_activity():
    st.subheader("Trade activity")
    try:
        series = list_trade_activity_series()
    except psycopg2.Error:
        st.error("Trade activity could not be loaded from the database.")
        return

    if not series:
        st.info("Gold does not contain any trade activity yet.")
        return

    selected_series = st.selectbox(
        "Market",
        options=series,
        format_func=lambda value: f"{value[1]} · {value[0]}",
        key="trade_activity_market",
    )
    source, product_id, first_hour, last_hour = selected_series
    selected_dates = st.date_input(
        "Date range (UTC)",
        value=(first_hour.date(), last_hour.date()),
        min_value=first_hour.date(),
        max_value=last_hour.date(),
        key="trade_activity_date_range",
    )

    if len(selected_dates) != 2:
        st.info("Select both the start and end date.")
        return

    start_date, end_date = selected_dates
    start = datetime.combine(start_date, time.min, tzinfo=timezone.utc)
    end = datetime.combine(end_date + timedelta(days=1), time.min, tzinfo=timezone.utc)

    try:
        activity = load_trade_activity(
            source=source,
            product_id=product_id,
            start=start,
            end=end,
        )
    except psycopg2.Error:
        st.error("Trade activity could not be loaded from the database.")
        return

    if not activity:
        st.info("No trade activity was found for the selected date range.")
        return

    (
        hours,
        trade_counts,
        base_volumes,
        quote_volumes,
        vwaps,
        maker_buy_counts,
        maker_sell_counts,
    ) = zip(*activity)

    metric_columns = st.columns(4)
    metric_columns[0].metric("Trades", f"{sum(trade_counts):,}")
    metric_columns[1].metric("Base volume", f"{sum(base_volumes):,.8f}")
    metric_columns[2].metric("Quote volume", f"{sum(quote_volumes):,.2f}")
    metric_columns[3].metric(
        "Buy / sell",
        f"{sum(maker_buy_counts):,} / {sum(maker_sell_counts):,}",
    )

    figure = go.Figure()
    figure.add_trace(
        go.Bar(
            x=hours,
            y=trade_counts,
            name="Trade count",
        )
    )
    figure.add_trace(
        go.Scatter(
            x=hours,
            y=[float(value) for value in vwaps],
            mode="lines+markers",
            name="VWAP",
            yaxis="y2",
        )
    )
    figure.update_layout(
        title=f"{product_id} · Hourly trade activity",
        xaxis_title="UTC hour",
        yaxis_title="Trades",
        yaxis2={
            "title": "VWAP",
            "overlaying": "y",
            "side": "right",
        },
        height=550,
        legend={"orientation": "h"},
    )
    st.plotly_chart(figure, width="stretch")
    st.caption(f"Showing {len(activity):,} hourly aggregates. Query limit: 1,000.")


st.set_page_config(page_title="Market data", page_icon="📈", layout="wide")
st.title("Market data")
st.caption("Candles and trade activity loaded from the PostgreSQL Gold layer")

candles_tab, trades_tab = st.tabs(["Candles", "Trade activity"])
with candles_tab:
    _render_candles()
with trades_tab:
    _render_trade_activity()
