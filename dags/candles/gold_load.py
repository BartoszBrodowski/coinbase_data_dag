import logging

from candles.utils import load_sql, market_data_connection


logger = logging.getLogger(__name__)

UPSERT_GOLD_SQL = load_sql("upsert_gold_candles.sql")
GOLD_QUALITY_SQL = load_sql("check_gold_quality.sql")


def upsert_gold(cursor, ingestion_id):
    cursor.execute(UPSERT_GOLD_SQL, {"ingestion_id": ingestion_id})
    return cursor.rowcount


def check_gold_quality(cursor):
    cursor.execute(GOLD_QUALITY_SQL)
    (
        silver_count,
        gold_count,
        duplicate_count,
        null_count,
        invalid_ohlc_count,
        time_gap_count,
    ) = cursor.fetchone()

    if gold_count != silver_count:
        raise ValueError(
            f"Gold contains {gold_count} rows, but Silver contains {silver_count}"
        )
    quality_issues = (
        (duplicate_count, "duplicate candle keys"),
        (null_count, "rows with null values"),
        (invalid_ohlc_count, "rows with invalid OHLC values"),
        (time_gap_count, "time gaps between candles"),
    )
    for count_issue, description in quality_issues:
        if count_issue:
            raise ValueError(f"Gold contains {count_issue} {description}")

    return gold_count


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
    return rows_processed


def validate_gold_quality():
    with market_data_connection() as connection:
        with connection:
            with connection.cursor() as cursor:
                rows_checked = check_gold_quality(cursor)

    logger.info("gold_quality_passed record_count=%s", rows_checked)
    return rows_checked
