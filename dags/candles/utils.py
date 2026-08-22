"""Candle-specific SQL loading"""

from pathlib import Path

SQL_DIRECTORY = Path(__file__).with_name("sql")

def load_sql(filename):
    return (SQL_DIRECTORY / filename).read_text(encoding="utf-8")
