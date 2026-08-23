import json
import logging
from pathlib import Path

from shared.coinbase_client import fetch_candles
from shared.utils import load_sql, market_data_connection
from shared.validation import require_active_product


logger = logging.getLogger(__name__)
SQL_DIRECTORY = Path(__file__).with_name("sql")

INSERT_BRONZE_RESPONSE_SQL = load_sql(SQL_DIRECTORY, "insert_bronze_response.sql")
SELECT_BRONZE_BY_KEY_SQL = load_sql(SQL_DIRECTORY, "select_bronze_by_key.sql")


def save_bronze_response(
    connection,
    *,
    bronze_request_key,
    product_id,
    granularity_seconds,
    start,
    end,
    response,
):
    """Save a response once and return its Bronze ingestion ID."""

    require_active_product(product_id)

    parameters = {
        "bronze_request_key": bronze_request_key,
        "product_id": product_id,
        "granularity_seconds": granularity_seconds,
        "requested_start": start,
        "requested_end": end,
        "request_duration_ms": response.duration_ms,
        "http_status": response.status_code,
        "content_type": response.content_type,
        "response_headers": json.dumps(response.headers),
        "response_body": response.body,
    }

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(INSERT_BRONZE_RESPONSE_SQL, parameters)
            inserted = cursor.fetchone()

            if inserted is not None:
                return inserted[0]

            cursor.execute(
                SELECT_BRONZE_BY_KEY_SQL,
                {"bronze_request_key": bronze_request_key},
            )
            existing = cursor.fetchone()

            if existing is None:
                raise RuntimeError("existing Bronze response could not be read")

            existing_id = existing[0]
            logger.info(
                "coinbase_bronze_reused bronze_request_key=%s ingestion_id=%s",
                bronze_request_key,
                existing_id,
            )
            return existing_id


def ingest_one_interval(
    *,
    bronze_request_key,
    product_id,
    granularity_seconds,
    start,
    end,
):
    require_active_product(product_id)

    response = fetch_candles(
        product_id=product_id,
        granularity_seconds=granularity_seconds,
        start=start,
        end=end,
    )

    with market_data_connection() as connection:
        ingestion_id = save_bronze_response(
            connection,
            bronze_request_key=bronze_request_key,
            product_id=product_id,
            granularity_seconds=granularity_seconds,
            start=start,
            end=end,
            response=response,
        )

    # Preserve error responses in Bronze before failing the Airflow task.
    response.raise_for_status()

    payload = json.loads(response.body)
    record_count = len(payload) if isinstance(payload, list) else 0

    logger.info(
        "coinbase_bronze_saved "
        "product_id=%s granularity_seconds=%s "
        "start=%s end=%s record_count=%s "
        "http_status=%s duration_ms=%s ingestion_id=%s",
        product_id,
        granularity_seconds,
        start.isoformat(),
        end.isoformat(),
        record_count,
        response.status_code,
        response.duration_ms,
        ingestion_id,
    )

    return ingestion_id
