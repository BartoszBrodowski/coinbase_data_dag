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


def live_process_key(source, product_id):
    return f"live:{source}:{product_id}"


def validate_catchup_request(source, product_id, after_cursor, page_amount):
    """Return a normalized page-count request for the shared state model."""

    require_active_product(product_id)

    if not isinstance(after_cursor, str) or after_cursor.strip() == "":
        normalized_cursor = None
    else:
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
            # explicit request cursors, if the caller supplied any.
            resume_before_cursor = request.get("initial_before_cursor")
            resume_after_cursor = request.get("initial_after_cursor")
            if request["mode"] == "catchup" and resume_after_cursor is None:
                # A page-count catch-up begins from the live stream's last
                # committed `after` cursor, so it does not disturb live state.
                cursor.execute(
                    """
                    SELECT resume_after_cursor
                    FROM control.trade_ingestions
                    WHERE process_key = %(live_process_key)s
                    """,
                    {
                        "live_process_key": live_process_key(
                            request["source"], request["product_id"]
                        )
                    },
                )
                live_ingestion = cursor.fetchone()
                resume_after_cursor = live_ingestion[0] if live_ingestion else None
                if resume_after_cursor is None:
                    raise ValueError(
                        "catch-up requires an explicit after_cursor or a committed "
                        "live cursor"
                    )

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
