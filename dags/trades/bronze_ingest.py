"""Fetch and persist one Coinbase trades page without Airflow dependencies."""

import json
import logging
from pathlib import Path

from shared.coinbase_client import fetch_trades
from shared.utils import load_sql, market_data_connection
from shared.validation import require_active_product


logger = logging.getLogger(__name__)
SQL_DIRECTORY = Path(__file__).with_name("sql")
INSERT_TRADE_PAGE_SQL = load_sql(SQL_DIRECTORY, "insert_bronze_trade_page.sql")
SELECT_TRADE_PAGE_BY_KEY_SQL = load_sql(
    SQL_DIRECTORY, "select_bronze_trade_page_by_key.sql"
)


def save_trade_page(
    connection,
    *,
    bronze_request_key,
    process_key,
    product_id,
    response,
    requested_before=None,
    requested_after=None,
    pipeline_run_id=None,
    pipeline_task_id=None,
    attempt_number=None,
):
    """Persist one raw HTTP response once and return its Bronze ID."""

    require_active_product(product_id)

    parameters = {
        "bronze_request_key": bronze_request_key,
        "process_key": process_key,
        "product_id": product_id,
        "requested_before": requested_before,
        "requested_after": requested_after,
        "response_cb_before": response.headers.get("CB-BEFORE"),
        "response_cb_after": response.headers.get("CB-AFTER"),
        "request_duration_ms": response.duration_ms,
        "http_status": response.status_code,
        "content_type": response.content_type,
        "response_headers": json.dumps(response.headers),
        "response_body": response.body,
        "pipeline_run_id": pipeline_run_id,
        "pipeline_task_id": pipeline_task_id,
        "attempt_number": attempt_number,
    }

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(INSERT_TRADE_PAGE_SQL, parameters)
            inserted = cursor.fetchone()
            if inserted is None:
                cursor.execute(
                    SELECT_TRADE_PAGE_BY_KEY_SQL,
                    {"bronze_request_key": bronze_request_key},
                )
                existing = cursor.fetchone()
                if existing is None:
                    raise RuntimeError("existing Bronze trade page could not be read")
                ingestion_id = existing[0]
            else:
                ingestion_id = inserted[0]

            return ingestion_id


def ingest_trade_page(
    *,
    bronze_request_key,
    process_key,
    product_id,
    before=None,
    after=None,
    limit=1_000,
    pipeline_run_id=None,
    pipeline_task_id=None,
    attempt_number=None,
):
    """Fetch a page, persist it in Bronze, then raise HTTP failures."""

    require_active_product(product_id)

    response = fetch_trades(
        product_id=product_id,
        before=before,
        after=after,
        limit=limit,
    )
    with market_data_connection() as connection:
        ingestion_id = save_trade_page(
            connection,
            bronze_request_key=bronze_request_key,
            process_key=process_key,
            product_id=product_id,
            response=response,
            requested_before=before,
            requested_after=after,
            pipeline_run_id=pipeline_run_id,
            pipeline_task_id=pipeline_task_id,
            attempt_number=attempt_number,
        )

        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT response_cb_before, response_cb_after
                FROM bronze.coinbase_trades_raw
                WHERE ingestion_id = %(ingestion_id)s
                """,
                {"ingestion_id": ingestion_id},
            )
            response_cb_before, response_cb_after = cursor.fetchone()

    response.raise_for_status()

    logger.info(
        "coinbase_trades_bronze_saved product_id=%s ingestion_id=%s "
        "http_status=%s duration_ms=%s",
        product_id,
        ingestion_id,
        response.status_code,
        response.duration_ms,
    )
    return {
        "ingestion_id": ingestion_id,
        "response_cb_before": response_cb_before,
        "response_cb_after": response_cb_after,
    }
