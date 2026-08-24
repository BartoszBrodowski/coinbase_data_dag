"""Detect actionable gaps in committed Coinbase trade coverage."""

from datetime import datetime
from pathlib import Path

from shared.utils import load_sql


SQL_DIRECTORY = Path(__file__).with_name("sql")
SELECT_NEWEST_TRADE_GAP_SQL = load_sql(
    SQL_DIRECTORY,
    "select_newest_trade_gap.sql",
)


def find_newest_trade_gap(
    connection,
    *,
    source,
    product_id,
    lookback_start: datetime,
    eligible_before: datetime,
    minimum_gap_seconds: int,
    maximum_page_amount: int,
):
    """Return the newest repairable gap, or ``None`` when none exists."""

    parameters = {
        "source": source,
        "product_id": product_id,
        "lookback_start": lookback_start,
        "eligible_before": eligible_before,
        "minimum_gap_seconds": minimum_gap_seconds,
        "maximum_page_amount": maximum_page_amount,
    }
    with connection.cursor() as cursor:
        cursor.execute(SELECT_NEWEST_TRADE_GAP_SQL, parameters)
        row = cursor.fetchone()

    if row is None:
        return None

    (
        older_trade_time,
        newer_trade_time,
        gap_seconds,
        older_trade_id,
        newer_trade_id,
        after_cursor,
        estimated_page_amount,
        page_amount,
    ) = row
    return {
        "source": source,
        "product_id": product_id,
        "older_trade_time": older_trade_time.isoformat(),
        "newer_trade_time": newer_trade_time.isoformat(),
        "gap_seconds": gap_seconds,
        "older_trade_id": older_trade_id,
        "newer_trade_id": newer_trade_id,
        "after_cursor": after_cursor,
        "estimated_page_amount": estimated_page_amount,
        "page_amount": page_amount,
        "page_amount_capped": estimated_page_amount > page_amount,
    }
