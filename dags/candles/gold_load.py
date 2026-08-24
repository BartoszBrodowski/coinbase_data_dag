import logging
from pathlib import Path

from shared.utils import load_sql, market_data_connection


logger = logging.getLogger(__name__)
SQL_DIRECTORY = Path(__file__).with_name("sql")

UPSERT_GOLD_SQL = load_sql(SQL_DIRECTORY, "upsert_gold_candles.sql")


def upsert_gold(cursor, ingestion_id):
    cursor.execute(UPSERT_GOLD_SQL, {"ingestion_id": ingestion_id})
    return cursor.rowcount


def load_silver_to_gold(ingestion_id):
    with market_data_connection() as connection:
        with connection:
            with connection.cursor() as cursor:
                rows_processed = upsert_gold(cursor, ingestion_id)

    logger.info(
        "market_data_stage_summary stage=gold ingestion_id=%s records_written=%s",
        ingestion_id,
        rows_processed,
    )
