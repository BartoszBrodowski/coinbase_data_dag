import logging

from shared.utils import market_data_connection
from candles.utils import load_sql


logger = logging.getLogger(__name__)

UPSERT_GOLD_SQL = load_sql("upsert_gold_candles.sql")


def upsert_gold(cursor, ingestion_id):
    cursor.execute(UPSERT_GOLD_SQL, {"ingestion_id": ingestion_id})
    return cursor.rowcount


def load_silver_to_gold(ingestion_id):
    with market_data_connection() as connection:
        with connection:
            with connection.cursor() as cursor:
                rows_processed = upsert_gold(cursor, ingestion_id)

    logger.info(
        "gold_candles_upserted ingestion_id=%s record_count=%s",
        ingestion_id,
        rows_processed,
    )
