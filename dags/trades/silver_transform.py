"""Transform one raw Coinbase trade page and advance its watermark."""

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

from trades.ingestion_state import UPDATE_INGESTION_PROGRESS_SQL


logger = logging.getLogger(__name__)
SOURCE = "coinbase"
SQL_DIRECTORY = Path(__file__).with_name("sql")
SELECT_TRADE_PAGE_FOR_SILVER_SQL = (
    SQL_DIRECTORY / "select_bronze_trade_page_for_silver.sql"
).read_text(encoding="utf-8")
UPSERT_TRADES_SQL = (SQL_DIRECTORY / "upsert_silver_trades.sql").read_text(
    encoding="utf-8"
)
MARK_BRONZE_TRADE_PAGE_COMMITTED_SQL = (
    SQL_DIRECTORY / "mark_bronze_trade_page_committed.sql"
).read_text(encoding="utf-8")


@dataclass(frozen=True)
class Trade:
    trade_id: int
    price: Decimal
    time: datetime
    size: Decimal
    side: str


def parse_trades(response_body):
    """Parse and validate a Coinbase page, deduplicating identical trades."""

    payload = json.loads(response_body)
    if not isinstance(payload, list):
        raise ValueError("Coinbase trades payload must be a list")

    trades_by_id = {}
    for index, item in enumerate(payload):
        if not isinstance(item, dict):
            raise ValueError(f"trade {index} must be an object")

        try:
            trade_id = item["trade_id"]
            price = Decimal(item["price"])
            trade_time = _parse_utc_time(item["time"])
            size = Decimal(item["size"])
            side = item["side"]
        except (KeyError, InvalidOperation, TypeError, ValueError) as error:
            raise ValueError(f"trade {index} has invalid required fields") from error

        if isinstance(trade_id, bool) or not isinstance(trade_id, int) or trade_id <= 0:
            raise ValueError(f"trade {index} has an invalid trade_id")
        if side not in {"buy", "sell"}:
            raise ValueError(f"trade {index} has an invalid maker side")
        if not price.is_finite() or price <= 0:
            raise ValueError(f"trade {index} has an invalid price")
        if not size.is_finite() or size <= 0:
            raise ValueError(f"trade {index} has an invalid size")

        trade = Trade(trade_id, price, trade_time, size, side)
        existing = trades_by_id.get(trade_id)
        if existing is not None and existing != trade:
            raise ValueError(f"conflicting values for trade_id {trade_id}")
        trades_by_id[trade_id] = trade

    return sorted(trades_by_id.values(), key=lambda trade: trade.trade_id)


def transform_bronze_to_silver(connection, ingestion_id):
    """Commit a page and advance its live or catch-up process atomically."""

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(
                SELECT_TRADE_PAGE_FOR_SILVER_SQL,
                {"ingestion_id": ingestion_id},
            )
            bronze = cursor.fetchone()
            if bronze is None:
                raise ValueError(f"Bronze trade ingestion {ingestion_id} does not exist")

            (
                bronze_ingestion_id,
                trade_ingestion_key,
                product_id,
                mode,
                oldest_time,
                response_cb_after,
                pipeline_run_id,
                http_status,
                response_body,
            ) = bronze
            if not 200 <= http_status < 300:
                raise ValueError(f"Bronze trade ingestion {ingestion_id} has HTTP {http_status}")

            page_trades = parse_trades(response_body)
            reached_time_boundary = bool(
                oldest_time is not None
                and (
                    not page_trades
                    or min(trade.time for trade in page_trades) <= oldest_time
                )
            )
            trades = [
                trade
                for trade in page_trades
                if oldest_time is None or trade.time >= oldest_time
            ]
            rows = [
                (
                    SOURCE,
                    product_id,
                    trade.trade_id,
                    trade.price,
                    trade.time,
                    trade.size,
                    trade.side,
                    bronze_ingestion_id,
                )
                for trade in trades
            ]
            if rows:
                cursor.executemany(UPSERT_TRADES_SQL, rows)

            cursor.execute(
                MARK_BRONZE_TRADE_PAGE_COMMITTED_SQL,
                {"ingestion_id": bronze_ingestion_id},
            )
            completed = mode == "catchup" and (
                reached_time_boundary or response_cb_after is None
            )
            cursor.execute(
                UPDATE_INGESTION_PROGRESS_SQL,
                {
                    "trade_ingestion_key": trade_ingestion_key,
                    "resume_after_cursor": response_cb_after,
                    "highest_trade_id": max(
                        (trade.trade_id for trade in trades), default=None
                    ),
                    "completed": completed,
                    "trades_committed": len(trades),
                    "pipeline_run_id": pipeline_run_id,
                    "bronze_ingestion_id": bronze_ingestion_id,
                },
            )

    logger.info(
        "coinbase_trades_silver_committed ingestion_id=%s record_count=%s",
        ingestion_id,
        len(trades),
    )
    return {
        "trades_processed": len(trades),
        "completed": completed,
    }


def _parse_utc_time(value):
    if not isinstance(value, str):
        raise ValueError("time must be an ISO 8601 string")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("time must include a timezone")
    return parsed.astimezone(timezone.utc)
