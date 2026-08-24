"""Shared durable cursor state for live and catch-up trade ingestion."""

import hashlib
import json
from pathlib import Path

from shared.utils import load_sql
from shared.validation import require_active_product
from trades.settings import MAX_CATCHUP_PAGE_AMOUNT


SQL_DIRECTORY = Path(__file__).with_name("sql")
UPDATE_INGESTION_PROGRESS_SQL = load_sql(
    SQL_DIRECTORY, "update_trade_ingestion_progress.sql"
)
PAUSE_INGESTION_SQL = load_sql(SQL_DIRECTORY, "pause_trade_ingestion.sql")
COMPLETE_INGESTION_SQL = load_sql(SQL_DIRECTORY, "complete_trade_ingestion.sql")
INSERT_TRADE_INGESTION_SQL = load_sql(SQL_DIRECTORY, "insert_trade_ingestion.sql")
SELECT_COMMITTED_AFTER_CURSOR_SQL = load_sql(
    SQL_DIRECTORY,
    "select_committed_trade_page_by_after_cursor.sql",
)
SELECT_HIGHEST_COMMITTED_TRADE_ID_SQL = load_sql(
    SQL_DIRECTORY,
    "select_highest_committed_trade_id.sql",
)
SELECT_TRADE_INGESTION_SUMMARY_SQL = load_sql(
    SQL_DIRECTORY,
    "select_trade_ingestion_summary.sql",
)
SELECT_TRADE_INGESTION_STATUS_SQL = load_sql(
    SQL_DIRECTORY,
    "select_trade_ingestion_status.sql",
)


def live_process_key(source, product_id):
    return f"live:{source}:{product_id}"


def validate_catchup_request(source, product_id, after_cursor, page_amount):
    """Return a normalized page-count request for the shared state model."""

    require_active_product(product_id)

    if not isinstance(after_cursor, str) or after_cursor.strip() == "":
        raise ValueError("after_cursor is required for trade catch-up")
    normalized_cursor = after_cursor.strip()

    if (
        not isinstance(page_amount, int)
        or isinstance(page_amount, bool)
        or not 1 <= page_amount <= MAX_CATCHUP_PAGE_AMOUNT
    ):
        raise ValueError(
            f"page_amount must be between 1 and {MAX_CATCHUP_PAGE_AMOUNT}"
        )

    identity = {
        "source": source,
        "product_id": product_id,
        "mode": "catchup",
        "initial_after_cursor": normalized_cursor,
        "page_amount": page_amount,
    }
    digest = hashlib.sha256(
        json.dumps(identity, sort_keys=True).encode("utf-8")
    ).hexdigest()

    return {
        **identity,
        "oldest_time": None,
        "cursor_direction": "after",
        "process_key": f"catchup:{digest}",
    }


def start_or_resume_ingestion(connection, request, pipeline_run_id):
    """Create a cursor stream or return its last Silver-committed cursor."""

    pagination_direction = request.get(
        "cursor_direction",
        "after" if request["mode"] == "catchup" else "before",
    )

    with connection:
        with connection.cursor() as cursor:
            # The process key identifies one durable live or catch-up stream.
            # Its cursor is advanced only after that page reaches Silver.
            cursor.execute(
                """
                SELECT
                    resume_before_cursor,
                    resume_after_cursor,
                    status,
                    pages_committed
                FROM control.trade_ingestions
                WHERE process_key = %(process_key)s
                """,
                request,
            )
            existing_ingestion = cursor.fetchone()
            if existing_ingestion is not None:
                (
                    resume_before_cursor,
                    resume_after_cursor,
                    stored_status,
                    pages_committed,
                ) = existing_ingestion
                if stored_status != "COMPLETED":
                    # An unfinished process is active again when its next DAG run starts.
                    cursor.execute(
                        """
                        UPDATE control.trade_ingestions
                        SET status = 'ACTIVE',
                            last_pipeline_run_id = %(pipeline_run_id)s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE process_key = %(process_key)s
                        """,
                        {**request, "pipeline_run_id": pipeline_run_id},
                    )
                start_cursor = (
                    resume_before_cursor
                    if pagination_direction == "before"
                    else resume_after_cursor
                )
                return (
                    start_cursor,
                    "ACTIVE" if stored_status != "COMPLETED" else "COMPLETED",
                    pages_committed,
                )

            # This is the first run for this process key. Seed it with the
            # explicit request cursors supplied by the caller.
            resume_before_cursor = request.get("initial_before_cursor")
            resume_after_cursor = request.get("initial_after_cursor")

            cursor.execute(
                INSERT_TRADE_INGESTION_SQL,
                {
                    **request,
                    "initial_before_cursor": resume_before_cursor,
                    "initial_after_cursor": resume_after_cursor,
                    "resume_after_cursor": resume_after_cursor,
                    "resume_before_cursor": resume_before_cursor,
                    "pipeline_run_id": pipeline_run_id,
                },
            )
            start_cursor = (
                resume_before_cursor
                if pagination_direction == "before"
                else resume_after_cursor
            )
            return start_cursor, "ACTIVE", 0


def committed_after_cursor_exists(connection, product_id, requested_after):
    """Return whether this exact historical cursor page reached Silver."""

    if requested_after is None:
        return False

    with connection.cursor() as cursor:
        cursor.execute(
            SELECT_COMMITTED_AFTER_CURSOR_SQL,
            {
                "product_id": product_id,
                "requested_after": requested_after,
            },
        )
        return cursor.fetchone() is not None


def highest_committed_trade_id(connection, source, product_id):
    """Return the newest trade ID already committed to Silver."""

    with connection.cursor() as cursor:
        cursor.execute(
            SELECT_HIGHEST_COMMITTED_TRADE_ID_SQL,
            {"source": source, "product_id": product_id},
        )
        return cursor.fetchone()[0]


def trade_ingestion_summary(connection, process_key):
    """Return cumulative totals for one durable catch-up process."""

    with connection.cursor() as cursor:
        cursor.execute(
            SELECT_TRADE_INGESTION_SUMMARY_SQL,
            {"process_key": process_key},
        )
        row = cursor.fetchone()

    if row is None:
        raise ValueError(f"trade catch-up process not found: {process_key}")

    (
        source,
        product_id,
        status,
        pages_committed,
        trades_committed,
        initial_after_cursor,
        resume_after_cursor,
        created_at,
        completed_at,
    ) = row
    duration_seconds = None
    if completed_at is not None:
        duration_seconds = round((completed_at - created_at).total_seconds(), 3)

    return {
        "source": source,
        "product_id": product_id,
        "status": status,
        "pages_committed": pages_committed,
        "trades_committed": trades_committed,
        "initial_after_cursor": initial_after_cursor,
        "resume_after_cursor": resume_after_cursor,
        "duration_seconds": duration_seconds,
    }


def trade_ingestion_status(connection, process_key):
    """Return a catch-up process status, or ``None`` before it is created."""

    with connection.cursor() as cursor:
        cursor.execute(
            SELECT_TRADE_INGESTION_STATUS_SQL,
            {"process_key": process_key},
        )
        row = cursor.fetchone()
    return row[0] if row is not None else None


def pause_ingestion(connection, process_key):
    """Pause an unfinished catch-up after its bounded page batch."""

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(PAUSE_INGESTION_SQL, {"process_key": process_key})


def complete_ingestion(connection, process_key):
    """Mark a page-count catch-up complete after a terminal stop condition."""

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(COMPLETE_INGESTION_SQL, {"process_key": process_key})
