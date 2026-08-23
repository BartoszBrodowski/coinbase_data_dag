import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

from shared.utils import load_sql, market_data_connection


logger = logging.getLogger(__name__)
SQL_DIRECTORY = Path(__file__).with_name("sql")

SELECT_BRONZE_FOR_SILVER_SQL = load_sql(
    SQL_DIRECTORY, "select_bronze_for_silver.sql"
)
UPSERT_SILVER_CANDLES_SQL = load_sql(SQL_DIRECTORY, "upsert_silver_candles.sql")


@dataclass(frozen=True)
class Candle:
    source: str
    product_id: str
    granularity_seconds: int
    candle_start: datetime
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    volume: Decimal
    bronze_ingestion_id: int


def parse_candles(
    response_body: str,
    *,
    source: str,
    product_id: str,
    granularity_seconds: int,
    requested_start: datetime,
    requested_end: datetime,
    bronze_ingestion_id: int,
) -> list[Candle]:
    """Convert a raw Coinbase response into validated, unique candles."""

    payload = json.loads(response_body, parse_float=Decimal)
    if not isinstance(payload, list):
        raise ValueError("Coinbase payload must be a list")
    if granularity_seconds <= 0:
        raise ValueError("granularity_seconds must be positive")

    candles_by_start: dict[datetime, Candle] = {}

    for index, item in enumerate(payload):
        candle = _parse_candle(
            item,
            index=index,
            source=source,
            product_id=product_id,
            granularity_seconds=granularity_seconds,
            requested_start=requested_start,
            requested_end=requested_end,
            bronze_ingestion_id=bronze_ingestion_id,
        )
        if candle is None:
            continue

        existing = candles_by_start.get(candle.candle_start)
        if existing is not None and existing != candle:
            raise ValueError(
                f"conflicting candles for {candle.candle_start.isoformat()}"
            )

        candles_by_start[candle.candle_start] = candle

    return sorted(candles_by_start.values(), key=lambda candle: candle.candle_start)


def upsert_candles(cursor, candles):
    if not candles:
        return 0

    rows = [
        (
            candle.source,
            candle.product_id,
            candle.granularity_seconds,
            candle.candle_start,
            candle.open_price,
            candle.high_price,
            candle.low_price,
            candle.close_price,
            candle.volume,
            candle.bronze_ingestion_id,
        )
        for candle in candles
    ]
    cursor.executemany(UPSERT_SILVER_CANDLES_SQL, rows)
    return len(rows)


def transform_bronze_to_silver(ingestion_id, *, source):
    with market_data_connection() as connection:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    SELECT_BRONZE_FOR_SILVER_SQL,
                    {"ingestion_id": ingestion_id},
                )
                bronze = cursor.fetchone()

                if bronze is None:
                    raise ValueError(f"Bronze ingestion {ingestion_id} does not exist")

                (
                    product_id,
                    granularity_seconds,
                    requested_start,
                    requested_end,
                    response_body,
                ) = bronze

                candles = parse_candles(
                    response_body,
                    source=source,
                    product_id=product_id,
                    granularity_seconds=granularity_seconds,
                    requested_start=requested_start,
                    requested_end=requested_end,
                    bronze_ingestion_id=ingestion_id,
                )
                rows_processed_count = upsert_candles(cursor, candles)

    logger.info(
        "coinbase_silver_upserted ingestion_id=%s record_count=%s",
        ingestion_id,
        rows_processed_count,
    )
    return rows_processed_count


def _parse_candle(
    item,
    *,
    index: int,
    source: str,
    product_id: str,
    granularity_seconds: int,
    requested_start: datetime,
    requested_end: datetime,
    bronze_ingestion_id: int,
) -> Candle | None:
    """Parse one Coinbase candle, or ignore it when outside the request range."""

    if not isinstance(item, list) or len(item) != 6:
        raise ValueError(f"candle {index} must contain exactly 6 fields")

    timestamp, raw_low, raw_high, raw_open, raw_close, raw_volume = item
    candle_start = _parse_candle_start(timestamp, index=index)

    # Coinbase may include a candle preceding the requested start.
    if not requested_start <= candle_start < requested_end:
        return None

    if timestamp % granularity_seconds:
        raise ValueError(
            f"candle {index} timestamp is not aligned to "
            f"{granularity_seconds}-second granularity"
        )

    low = _parse_decimal(raw_low, index=index, field="low")
    high = _parse_decimal(raw_high, index=index, field="high")
    opening = _parse_decimal(raw_open, index=index, field="open")
    close = _parse_decimal(raw_close, index=index, field="close")
    volume = _parse_decimal(raw_volume, index=index, field="volume")

    prices = low, high, opening, close
    if not all(value.is_finite() for value in (*prices, volume)):
        raise ValueError(f"candle {index} contains a non-finite number")
    if min(prices) <= 0:
        raise ValueError(f"candle {index} prices must be positive")
    if volume < 0:
        raise ValueError(f"candle {index} volume must be nonnegative")
    if not low <= opening <= high or not low <= close <= high:
        raise ValueError(f"candle {index} has invalid OHLC values")

    return Candle(
        source=source,
        product_id=product_id,
        granularity_seconds=granularity_seconds,
        candle_start=candle_start,
        open_price=opening,
        high_price=high,
        low_price=low,
        close_price=close,
        volume=volume,
        bronze_ingestion_id=bronze_ingestion_id,
    )


def _parse_candle_start(value, *, index: int) -> datetime:
    # Check because True == 1 and False == 0
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"candle {index} timestamp must be an integer")

    try:
        return datetime.fromtimestamp(value, tz=timezone.utc)
    except (OverflowError, OSError, ValueError) as error:
        raise ValueError(f"candle {index} has an invalid timestamp") from error


def _parse_decimal(value, *, index: int, field: str) -> Decimal:
    if isinstance(value, bool):
        raise ValueError(f"candle {index} has invalid {field} value")

    try:
        return Decimal(value)
    except (InvalidOperation, TypeError, ValueError) as error:
        raise ValueError(f"candle {index} has invalid {field} value") from error
