import json
import logging
from contextlib import closing

from coinbase_client import fetch_candles


logger = logging.getLogger(__name__)


def save_bronze_response(
    connection,
    *,
    ingestion_key,
    product_id,
    granularity_seconds,
    start,
    end,
    response,
):
    """Save a response once and return its Bronze ingestion ID."""

    sql = """
        INSERT INTO bronze.coinbase_candles_raw (
            ingestion_key,
            product_id,
            granularity_seconds,
            requested_start,
            requested_end,
            request_duration_ms,
            http_status,
            content_type,
            response_headers,
            response_body
        )
        VALUES (
            %(ingestion_key)s,
            %(product_id)s,
            %(granularity_seconds)s,
            %(requested_start)s,
            %(requested_end)s,
            %(request_duration_ms)s,
            %(http_status)s,
            %(content_type)s,
            %(response_headers)s::jsonb,
            %(response_body)s
        )
        ON CONFLICT (ingestion_key) DO NOTHING
        RETURNING ingestion_id
    """
    parameters = {
        "ingestion_key": ingestion_key,
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
            cursor.execute(sql, parameters)
            inserted = cursor.fetchone()

            if inserted is not None:
                return inserted[0]

            cursor.execute(
                """
                SELECT ingestion_id
                FROM bronze.coinbase_candles_raw
                WHERE ingestion_key = %(ingestion_key)s
                """,
                {"ingestion_key": ingestion_key},
            )
            existing = cursor.fetchone()

            if existing is None:
                raise RuntimeError("existing Bronze response could not be read")

            logger.info(
                "coinbase_bronze_reused ingestion_key=%s ingestion_id=%s",
                ingestion_key,
                existing[0],
            )
            return existing[0]


def ingest_one_interval(
    *,
    ingestion_key,
    product_id,
    granularity_seconds,
    start,
    end,
):
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    response = fetch_candles(
        product_id=product_id,
        granularity_seconds=granularity_seconds,
        start=start,
        end=end,
    )

    hook = PostgresHook(postgres_conn_id="market_data")

    with closing(hook.get_conn()) as connection:
        ingestion_id = save_bronze_response(
            connection,
            ingestion_key=ingestion_key,
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
