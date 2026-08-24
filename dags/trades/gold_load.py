"""Build the Gold hourly trade-activity read model."""

import logging
from datetime import datetime, timedelta
from pathlib import Path

from shared.utils import load_sql


logger = logging.getLogger(__name__)
SQL_DIRECTORY = Path(__file__).with_name("sql")
UPSERT_TRADE_ACTIVITY_HOURLY_SQL = load_sql(
    SQL_DIRECTORY,
    "upsert_gold_trade_activity_hourly.sql",
)


def upsert_trade_activity_hourly(
    cursor,
    *,
    source: str,
    product_id: str,
    start: datetime,
    end: datetime,
):
    """Recompute affected UTC hours from Silver and upsert them into Gold."""

    cursor.execute(
        UPSERT_TRADE_ACTIVITY_HOURLY_SQL,
        {
            "source": source,
            "product_id": product_id,
            "start": start,
            "end": end,
        },
    )
    return cursor.rowcount


def load_trade_activity_hourly(
    connection,
    *,
    source: str,
    product_id: str,
    start: datetime,
    end: datetime,
):
    """Refresh the affected Gold range in one database transaction."""

    full_hour_start = start.replace(minute=0, second=0, microsecond=0)
    full_hour_end = end.replace(minute=0, second=0, microsecond=0)
    if end > full_hour_end:
        full_hour_end += timedelta(hours=1)

    with connection:
        with connection.cursor() as cursor:
            rows_written = upsert_trade_activity_hourly(
                cursor,
                source=source,
                product_id=product_id,
                start=full_hour_start,
                end=full_hour_end,
            )

    logger.info(
        "market_data_stage_summary stage=gold_trade_activity "
        "source=%s product_id=%s start=%s end=%s hours_written=%s",
        source,
        product_id,
        full_hour_start.isoformat(),
        full_hour_end.isoformat(),
        rows_written,
    )
    return rows_written
