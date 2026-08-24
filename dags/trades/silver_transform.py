"""Transform one raw Coinbase trade page and advance its watermark."""

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

from shared.utils import load_sql
from trades.ingestion_state import UPDATE_INGESTION_PROGRESS_SQL


logger = logging.getLogger(__name__)
SQL_DIRECTORY = Path(__file__).with_name("sql")
SELECT_TRADE_PAGE_FOR_SILVER_SQL = load_sql(
    SQL_DIRECTORY, "select_bronze_trade_page_for_silver.sql"
)
UPSERT_TRADES_SQL = load_sql(SQL_DIRECTORY, "upsert_silver_trades.sql")
MARK_BRONZE_TRADE_PAGE_COMMITTED_SQL = load_sql(
    SQL_DIRECTORY, "mark_bronze_trade_page_committed.sql"
)


@dataclass(frozen=True)
class Trade:
    trade_id: int
    price: Decimal
    time: datetime
    size: Decimal
    side: str


@dataclass(frozen=True)
class TradeParseResult:
    trades: list[Trade]
    records_fetched: int
    records_accepted: int
    duplicate_records: int


def parse_trades(response_body):
    """Parse and validate a Coinbase page, deduplicating identical trades."""

    return parse_trades_with_stats(response_body).trades


def parse_trades_with_stats(response_body):
    """Parse a Coinbase page and retain small, task-log-friendly counts."""

    payload = json.loads(response_body)
    if not isinstance(payload, list):
        raise ValueError("Coinbase trades payload must be a list")

    trades_by_id = {}
    duplicate_records = 0
    records_accepted = 0
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
        if not isinstance(side, str) or side not in {"buy", "sell"}:
            raise ValueError(f"trade {index} has an invalid maker side")
        if not price.is_finite() or price <= 0:
            raise ValueError(f"trade {index} has an invalid price")
        if not size.is_finite() or size <= 0:
            raise ValueError(f"trade {index} has an invalid size")

        trade = Trade(trade_id, price, trade_time, size, side)
        records_accepted += 1
        existing = trades_by_id.get(trade_id)
        if existing is not None and existing != trade:
            raise ValueError(f"conflicting values for trade_id {trade_id}")
        if existing is not None:
            duplicate_records += 1
            continue
        trades_by_id[trade_id] = trade

    trades = sorted(trades_by_id.values(), key=lambda trade: trade.trade_id)
    return TradeParseResult(
        trades=trades,
        records_fetched=len(payload),
        records_accepted=records_accepted,
        duplicate_records=duplicate_records,
    )


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
                process_key,
                source,
                product_id,
                mode,
                oldest_time,
                response_cb_before,
                response_cb_after,
                pipeline_run_id,
                http_status,
                response_body,
            ) = bronze
            if not 200 <= http_status < 300:
                raise ValueError(f"Bronze trade ingestion {ingestion_id} has HTTP {http_status}")

            parse_result = parse_trades_with_stats(response_body)
            page_trades = parse_result.trades
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
                    source,
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
                    "process_key": process_key,
                    "response_cb_before": response_cb_before,
                    "response_cb_after": response_cb_after,
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
        "market_data_stage_summary stage=silver ingestion_id=%s product_id=%s "
        "records_fetched=%s records_accepted=%s duplicate_records=%s "
        "records_filtered_by_boundary=%s records_written=%s completed=%s",
        ingestion_id,
        product_id,
        parse_result.records_fetched,
        parse_result.records_accepted,
        parse_result.duplicate_records,
        len(page_trades) - len(trades),
        len(trades),
        completed,
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
