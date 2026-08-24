import os
from contextlib import closing

import psycopg2

from utils import load_sql


MAX_CANDLES = 1_000
MAX_TRADE_ACTIVITY_HOURS = 1_000
LIST_MARKET_SERIES_SQL = load_sql("list_market_series.sql")
SELECT_GOLD_CANDLES_SQL = load_sql("select_gold_candles.sql")
LIST_TRADE_ACTIVITY_SERIES_SQL = load_sql("list_trade_activity_series.sql")
SELECT_TRADE_ACTIVITY_SQL = load_sql("select_trade_activity_hourly.sql")


def get_connection():
    return psycopg2.connect(
        host=os.environ["MARKET_DATA_HOST"],
        port=os.environ.get("MARKET_DATA_PORT", "5432"),
        dbname=os.environ["MARKET_DATA_DB"],
        user=os.environ["MARKET_DATA_USER"],
        password=os.environ["MARKET_DATA_PASSWORD"],
        connect_timeout=5,
    )


def list_market_series():
    with closing(get_connection()) as connection:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(LIST_MARKET_SERIES_SQL)
                return cursor.fetchall()


def load_candles(*, source, product_id, granularity_seconds, start, end):
    parameters = {
        "source": source,
        "product_id": product_id,
        "granularity_seconds": granularity_seconds,
        "start": start,
        "end": end,
        "limit": MAX_CANDLES,
    }

    with closing(get_connection()) as connection:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(SELECT_GOLD_CANDLES_SQL, parameters)
                return cursor.fetchall()


def list_trade_activity_series():
    with closing(get_connection()) as connection:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(LIST_TRADE_ACTIVITY_SERIES_SQL)
                return cursor.fetchall()


def load_trade_activity(*, source, product_id, start, end):
    parameters = {
        "source": source,
        "product_id": product_id,
        "start": start,
        "end": end,
        "limit": MAX_TRADE_ACTIVITY_HOURS,
    }

    with closing(get_connection()) as connection:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(SELECT_TRADE_ACTIVITY_SQL, parameters)
                return cursor.fetchall()
