import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

from utils import load_sql, market_data_connection


logger = logging.getLogger(__name__)

SELECT_BRONZE_FOR_SILVER_SQL = load_sql("select_bronze_for_silver.sql")
UPSERT_SILVER_CANDLES_SQL = load_sql("upsert_silver_candles.sql")


@dataclass(frozen=True)
class Candle:
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
    response_body,
    *,
    product_id,
    granularity_seconds,
    requested_start,
    requested_end,
    bronze_ingestion_id,
):
    """Convert a raw Coinbase response into validated, unique candles."""

    payload = json.loads(response_body, parse_float=Decimal)
    if not isinstance(payload, list):
        raise ValueError("Coinbase payload must be a list")

    candles_by_start = {}

    for index, item in enumerate(payload):
        if not isinstance(item, list) or len(item) != 6:
            raise ValueError(f"candle {index} must contain exactly 6 fields")

        timestamp, low, high, opening, close, volume = item
        candle_start = datetime.fromtimestamp(int(timestamp), tz=timezone.utc)

        # Coinbase may include a candle preceding the requested start.
        if not requested_start <= candle_start < requested_end:
            continue

        low = Decimal(low)
        high = Decimal(high)
        opening = Decimal(opening)
        close = Decimal(close)
        volume = Decimal(volume)

        prices = (low, high, opening, close)
        if not all(value.is_finite() for value in (*prices, volume)):
            raise ValueError(f"candle {index} contains a non-finite number")
        if min(prices) <= 0:
            raise ValueError(f"candle {index} prices must be positive")
        if volume < 0:
            raise ValueError(f"candle {index} volume must be nonnegative")
        if not low <= opening <= high or not low <= close <= high:
            raise ValueError(f"candle {index} has invalid OHLC values")

        candle = Candle(
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

        existing = candles_by_start.get(candle_start)
        if existing is not None and existing != candle:
            raise ValueError(f"conflicting candles for {candle_start.isoformat()}")

        candles_by_start[candle_start] = candle

    return sorted(candles_by_start.values(), key=lambda candle: candle.candle_start)


def upsert_candles(cursor, candles):
    if not candles:
        return 0

    rows = [
        (
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


def transform_bronze_to_silver(ingestion_id):
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
                    product_id=product_id,
                    granularity_seconds=granularity_seconds,
                    requested_start=requested_start,
                    requested_end=requested_end,
                    bronze_ingestion_id=ingestion_id,
                )
                rows_processed = upsert_candles(cursor, candles)

    logger.info(
        "coinbase_silver_upserted ingestion_id=%s record_count=%s",
        ingestion_id,
        rows_processed,
    )
    return rows_processed
