"""Candle-specific SQL loading plus shared Airflow helpers."""

from pathlib import Path

from shared.utils import (
    COINBASE_API_RETRY_ARGS,
    build_ingestion_key,
    market_data_connection,
)


SQL_DIRECTORY = Path(__file__).with_name("sql")


def load_sql(filename):
    return (SQL_DIRECTORY / filename).read_text(encoding="utf-8")
