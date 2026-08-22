"""Shared durable cursor state for live and catch-up trade ingestion."""

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from trades.settings import ACTIVE_PRODUCT_IDS


SQL_DIRECTORY = Path(__file__).with_name("sql")
UPDATE_INGESTION_PROGRESS_SQL = (
    SQL_DIRECTORY / "update_trade_ingestion_progress.sql"
).read_text(encoding="utf-8")
PAUSE_INGESTION_SQL = (SQL_DIRECTORY / "pause_trade_ingestion.sql").read_text(
    encoding="utf-8"
)


def live_ingestion_key(source, product_id):
    return f"live:{source}:{product_id}"


def validate_catchup_request(source, product_id, oldest_time, after_cursor, max_pages):
    """Return a normalized, bounded request for the shared state model."""

    if product_id not in ACTIVE_PRODUCT_IDS:
        raise ValueError(f"trade catch-up is not enabled for {product_id}")

    normalized_oldest_time = None
    if oldest_time:
        try:
            parsed = datetime.fromisoformat(oldest_time.replace("Z", "+00:00"))
        except (AttributeError, ValueError) as error:
            raise ValueError("oldest_time must be an ISO 8601 date-time") from error
        if parsed.tzinfo is None:
            raise ValueError("oldest_time must include a timezone")
        normalized_oldest_time = parsed.astimezone(timezone.utc)
        if normalized_oldest_time > datetime.now(timezone.utc):
            raise ValueError("oldest_time cannot be in the future")

    normalized_cursor = after_cursor.strip() if isinstance(after_cursor, str) else None
    if normalized_cursor == "":
        normalized_cursor = None
    if normalized_oldest_time is None and normalized_cursor is None:
        raise ValueError("catch-up requires oldest_time or after_cursor")
    if not isinstance(max_pages, int) or not 1 <= max_pages <= 20:
        raise ValueError("max_pages must be between 1 and 20")

    identity = {
        "source": source,
        "product_id": product_id,
        "mode": "catchup",
        "oldest_time": (
            normalized_oldest_time.isoformat() if normalized_oldest_time else None
        ),
        "initial_after_cursor": normalized_cursor,
    }
    digest = hashlib.sha256(
        json.dumps(identity, sort_keys=True).encode("utf-8")
    ).hexdigest()

    return {
        **identity,
        "ingestion_key": f"catchup:{digest}",
        "max_pages": max_pages,
    }


def start_or_resume_ingestion(connection, request, pipeline_run_id):
    """Create a cursor stream or return its last Silver-committed cursor."""

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT resume_after_cursor, status
                FROM control.trade_ingestions
                WHERE ingestion_key = %(ingestion_key)s
                """,
                request,
            )
            existing = cursor.fetchone()
            if existing is not None:
                resume_after_cursor, status = existing
                if status != "COMPLETED":
                    cursor.execute(
                        """
                        UPDATE control.trade_ingestions
                        SET status = 'ACTIVE',
                            last_pipeline_run_id = %(pipeline_run_id)s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE ingestion_key = %(ingestion_key)s
                        """,
                        {**request, "pipeline_run_id": pipeline_run_id},
                    )
                return resume_after_cursor, status

            resume_after_cursor = request["initial_after_cursor"]
            if request["mode"] == "catchup" and resume_after_cursor is None:
                cursor.execute(
                    """
                    SELECT resume_after_cursor
                    FROM control.trade_ingestions
                    WHERE ingestion_key = %(live_ingestion_key)s
                    """,
                    {
                        "live_ingestion_key": live_ingestion_key(
                            request["source"], request["product_id"]
                        )
                    },
                )
                live_state = cursor.fetchone()
                resume_after_cursor = live_state[0] if live_state else None

            cursor.execute(
                """
                INSERT INTO control.trade_ingestions (
                    ingestion_key,
                    source,
                    product_id,
                    mode,
                    oldest_time,
                    initial_after_cursor,
                    resume_after_cursor,
                    status,
                    last_pipeline_run_id
                )
                VALUES (
                    %(ingestion_key)s,
                    %(source)s,
                    %(product_id)s,
                    %(mode)s,
                    %(oldest_time)s,
                    %(initial_after_cursor)s,
                    %(resume_after_cursor)s,
                    'ACTIVE',
                    %(pipeline_run_id)s
                )
                """,
                {
                    **request,
                    "resume_after_cursor": resume_after_cursor,
                    "pipeline_run_id": pipeline_run_id,
                },
            )
            return resume_after_cursor, "ACTIVE"


def pause_ingestion(connection, ingestion_key):
    """Pause an unfinished catch-up after its bounded page batch."""

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(PAUSE_INGESTION_SQL, {"ingestion_key": ingestion_key})
