from datetime import datetime, time, timedelta, timezone

import plotly.graph_objects as go
import psycopg2
import streamlit as st

from database import MAX_CANDLES, list_market_series, load_candles


st.set_page_config(page_title="Market data", page_icon="📈", layout="wide")
st.title("Market data")
st.caption("Candles loaded from the PostgreSQL Gold layer")

try:
    series = list_market_series()
except psycopg2.Error:
    st.error("The market-data database is currently unavailable.")
    st.stop()

if not series:
    st.info("Gold does not contain any candles yet.")
    st.stop()

selected_series = st.selectbox(
    "Market",
    options=series,
    format_func=lambda value: f"{value[1]} · {value[0]}",
)
source, product_id, granularity_seconds, first_candle, last_candle = selected_series

selected_dates = st.date_input(
    "Date range (UTC)",
    value=(first_candle.date(), last_candle.date()),
    min_value=first_candle.date(),
    max_value=last_candle.date(),
)

if len(selected_dates) != 2:
    st.info("Select both the start and end date.")
    st.stop()

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
    st.stop()

if not candles:
    st.info("No candles were found for the selected market and date range.")
    st.stop()

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
