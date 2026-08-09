import json
import logging
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal


logger = logging.getLogger(__name__)


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

        low = Decimal(str(low))
        high = Decimal(str(high))
        opening = Decimal(str(opening))
        close = Decimal(str(close))
        volume = Decimal(str(volume))

        prices = (low, high, opening, close)
        if not all(value.is_finite() for value in (*prices, volume)):
            raise ValueError(f"candle {index} contains a non-finite number")
        if min(prices) <= 0:
            raise ValueError(f"candle {index} prices must be positive")
        if volume < 0:
            raise ValueError(f"candle {index} volume must be nonnegative")
        if low > high or not low <= opening <= high or not low <= close <= high:
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


UPSERT_SQL = """
    INSERT INTO silver.candles (
        source,
        product_id,
        granularity_seconds,
        candle_start,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        bronze_ingestion_id
    )
    VALUES ('coinbase', %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (
        source,
        product_id,
        granularity_seconds,
        candle_start
    )
    DO UPDATE SET
        open_price = EXCLUDED.open_price,
        high_price = EXCLUDED.high_price,
        low_price = EXCLUDED.low_price,
        close_price = EXCLUDED.close_price,
        volume = EXCLUDED.volume,
        bronze_ingestion_id = EXCLUDED.bronze_ingestion_id
"""


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
    cursor.executemany(UPSERT_SQL, rows)
    return len(rows)


def transform_bronze_to_silver(ingestion_id):
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id="market_data")

    with closing(hook.get_conn()) as connection:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        product_id,
                        granularity_seconds,
                        requested_start,
                        requested_end,
                        response_body
                    FROM bronze.coinbase_candles_raw
                    WHERE ingestion_id = %(ingestion_id)s
                    """,
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
