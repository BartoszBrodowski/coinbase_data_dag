import json
import logging
from datetime import datetime, timedelta, timezone

from airflow.sdk import PokeReturnValue, get_current_context, task
from airflow.utils.types import DagRunType
from shared.utils import (
    COINBASE_API_RETRY_ARGS,
    build_bronze_request_key,
    fail_without_retry_on,
    market_data_connection,
)
from shared.coinbase_client import COINBASE_SOURCE, CoinbaseNonRetryableAPIError
from trades.bronze_ingest import ingest_trade_page
from trades.ingestion_state import (
    committed_after_cursor_exists,
    complete_ingestion,
    highest_committed_trade_id,
    live_process_key,
    pause_ingestion,
    start_or_resume_ingestion,
    trade_ingestion_status,
    trade_ingestion_summary,
    validate_catchup_request,
)
from trades.gold_load import load_trade_activity_hourly
from trades.gap_detection import find_newest_trade_gap
from trades.silver_transform import transform_bronze_to_silver
from trades.settings import (
    CATCHUP_PAGES_PER_BATCH,
    GAP_REPAIR_LOOKBACK_DAYS,
    GAP_REPAIR_COMPLETION_POKE_SECONDS,
    GAP_REPAIR_COMPLETION_TIMEOUT_HOURS,
    GAP_REPAIR_MINIMUM_SECONDS,
    GAP_REPAIR_RECENT_DATA_MINUTES,
    MAX_CATCHUP_PAGE_AMOUNT,
    MAX_LIVE_PAGES_PER_RUN,
    TASK_TIMEOUT_MINUTES,
    TRADES_API_POOL,
)


logger = logging.getLogger(__name__)
PRODUCT_ID = "BTC-USD"


def build_live_ingestion_request(run_type=DagRunType.SCHEDULED, conf=None):
    """Build a new head scan or an operator-triggered continuation."""

    normalized_run_type = getattr(run_type, "value", run_type)
    conf = conf or {}
    is_continuation = normalized_run_type == DagRunType.OPERATOR_TRIGGERED.value
    if normalized_run_type not in {
        DagRunType.SCHEDULED.value,
        DagRunType.MANUAL.value,
        DagRunType.OPERATOR_TRIGGERED.value,
    }:
        raise ValueError("trade live ingestion does not support this run type")

    initial_after_cursor = conf.get("after_cursor") if is_continuation else None
    overlap_trade_id = conf.get("overlap_trade_id") if is_continuation else None
    if is_continuation:
        if not isinstance(initial_after_cursor, str) or not initial_after_cursor:
            raise ValueError("live continuation requires after_cursor")
        if (
            isinstance(overlap_trade_id, bool)
            or not isinstance(overlap_trade_id, int)
            or overlap_trade_id <= 0
        ):
            raise ValueError("live continuation requires overlap_trade_id")

    return {
        "mode": "live",
        "source": COINBASE_SOURCE,
        "product_id": PRODUCT_ID,
        "process_key": live_process_key(COINBASE_SOURCE, PRODUCT_ID),
        "oldest_time": None,
        "cursor_direction": "after",
        "initial_before_cursor": None,
        "initial_after_cursor": initial_after_cursor,
        "overlap_trade_id": overlap_trade_id,
        "is_continuation": is_continuation,
        "max_pages": MAX_LIVE_PAGES_PER_RUN,
    }


def build_catchup_ingestion_request(params, run_type):
    """Build a bounded cursor-based request for a manual catch-up run."""

    normalized_run_type = getattr(run_type, "value", run_type)
    allowed_run_types = {
        DagRunType.MANUAL.value,
        DagRunType.OPERATOR_TRIGGERED.value,
    }
    if normalized_run_type not in allowed_run_types:
        raise ValueError(
            "trade catch-up can only be started manually or continued by its "
            "trigger operator"
        )

    return {
        "mode": "catchup",
        **validate_catchup_request(
            source=COINBASE_SOURCE,
            product_id=PRODUCT_ID,
            after_cursor=params.get("after_cursor"),
            page_amount=params["page_amount"],
        ),
    }


@task(retries=0)
def resolve_live_ingestion_request():
    context = get_current_context()
    with fail_without_retry_on(ValueError):
        return build_live_ingestion_request(
            context["dag_run"].run_type,
            context["dag_run"].conf,
        )


@task(retries=0)
def resolve_catchup_ingestion_request():
    context = get_current_context()
    with fail_without_retry_on(ValueError):
        return build_catchup_ingestion_request(
            context["params"],
            context["dag_run"].run_type,
        )


@task(
    **COINBASE_API_RETRY_ARGS,
    pool=TRADES_API_POOL,
    execution_timeout=timedelta(minutes=TASK_TIMEOUT_MINUTES),
)
def ingest_bronze_pages(request):
    """Fetch and persist a sequence of cursor-paginated pages."""

    with fail_without_retry_on(CoinbaseNonRetryableAPIError, ValueError):
        return _ingest_bronze_pages(request, get_current_context())


def _ingest_bronze_pages(request, context):
    """Run Bronze ingestion; retryable API errors escape unchanged."""

    task_instance = context["ti"]
    pipeline_run_id = context["dag_run"].run_id

    with market_data_connection() as connection:
        cursor, status, pages_committed = start_or_resume_ingestion(
            connection,
            request,
            pipeline_run_id,
        )
        if request["mode"] == "live":
            if request["is_continuation"]:
                cursor = request["initial_after_cursor"]
                overlap_trade_id = request["overlap_trade_id"]
            else:
                cursor = None
                overlap_trade_id = highest_committed_trade_id(
                    connection,
                    request["source"],
                    request["product_id"],
                )
        else:
            overlap_trade_id = request.get("overlap_trade_id")

    if status == "COMPLETED":
        return {
            **request,
            "ingestion_ids": [],
            "already_completed": True,
            "pages_committed_before": pages_committed,
            "overlap_reached": False,
            "overlap_trade_id": overlap_trade_id,
            "next_after_cursor": cursor,
            "bronze_stop_reason": "already_completed",
        }

    ingestion_ids = []
    cursor_direction = request.get(
        "cursor_direction",
        "after" if request["mode"] == "catchup" else "before",
    )

    if request["mode"] == "catchup":
        remaining_pages = max(request["page_amount"] - pages_committed, 0)
        pages_in_batch = min(CATCHUP_PAGES_PER_BATCH, remaining_pages)
    else:
        pages_in_batch = request["max_pages"]

    overlap_reached = False
    stop_reason = None
    for page_index in range(pages_in_batch):
        if cursor_direction == "after" and cursor is not None:
            with market_data_connection() as connection:
                if committed_after_cursor_exists(
                    connection,
                    request["product_id"],
                    cursor,
                ):
                    overlap_reached = True
                    stop_reason = "committed_cursor_reached"
                    logger.info(
                        "market_data_stage_summary stage=bronze product_id=%s "
                        "status=stopped reason=committed_cursor cursor=%s",
                        request["product_id"],
                        cursor,
                    )
                    break

        bronze_request_key = build_bronze_request_key(
            task_instance,
            map_index=page_index,
        )
        page = ingest_trade_page(
            bronze_request_key=bronze_request_key,
            process_key=request["process_key"],
            product_id=request["product_id"],
            before=cursor if cursor_direction == "before" else None,
            after=cursor if cursor_direction == "after" else None,
            pipeline_run_id=pipeline_run_id,
            pipeline_task_id=task_instance.task_id,
            attempt_number=task_instance.try_number,
        )
        ingestion_ids.append(page["ingestion_id"])

        next_cursor = page[
            "response_cb_before"
            if cursor_direction == "before"
            else "response_cb_after"
        ]

        if next_cursor is None:
            overlap_reached = True
            stop_reason = "source_exhausted"
            break

        if request["mode"] == "live":
            if overlap_trade_id is None:
                overlap_reached = True
                stop_reason = "initial_snapshot"
                cursor = next_cursor
                break
            try:
                reached_trade_id = int(next_cursor) <= overlap_trade_id
            except (TypeError, ValueError) as error:
                raise ValueError(
                    "Coinbase live pagination returned a non-numeric cursor"
                ) from error
            if reached_trade_id:
                overlap_reached = True
                stop_reason = "committed_trade_id_reached"
                cursor = next_cursor
                break

        if next_cursor == cursor:
            overlap_reached = True
            stop_reason = "cursor_did_not_advance"
            break
        cursor = next_cursor

    if not overlap_reached:
        stop_reason = "batch_limit_reached"

    return {
        **request,
        "ingestion_ids": ingestion_ids,
        "already_completed": False,
        "pages_committed_before": pages_committed,
        "overlap_reached": overlap_reached,
        "overlap_trade_id": overlap_trade_id,
        "next_after_cursor": cursor,
        "bronze_stop_reason": stop_reason,
    }


@task(retries=0)
def load_silver_pages(batch):
    """Validate Bronze pages and commit Silver rows plus watermarks."""

    with fail_without_retry_on(ValueError):
        return _load_silver_pages(batch)


def _load_silver_pages(batch):
    if batch["already_completed"]:
        return {
            "source": batch.get("source"),
            "product_id": batch.get("product_id"),
            "pages_processed": 0,
            "trades_processed": 0,
            "activity_start": None,
            "activity_end": None,
            "completed": True,
            "stop_reason": "already_completed",
        }

    pages_processed = 0
    trades_processed = 0
    completed = False
    activity_start = None
    activity_end = None

    with market_data_connection() as connection:
        for ingestion_id in batch["ingestion_ids"]:
            result = transform_bronze_to_silver(connection, ingestion_id)
            pages_processed += 1
            trades_processed += result["trades_processed"]
            completed = result["completed"]
            if result.get("oldest_trade_time") is not None:
                activity_start = min(
                    activity_start or result["oldest_trade_time"],
                    result["oldest_trade_time"],
                )
            if result.get("newest_trade_time") is not None:
                activity_end = max(
                    activity_end or result["newest_trade_time"],
                    result["newest_trade_time"],
                )
            if completed:
                break

        if batch["mode"] == "catchup":
            total_pages = batch["pages_committed_before"] + pages_processed
            page_amount_reached = total_pages >= batch["page_amount"]
            if batch["overlap_reached"]:
                stop_reason = batch["bronze_stop_reason"]
            elif completed:
                stop_reason = "source_exhausted"
            elif page_amount_reached:
                stop_reason = "page_amount_reached"
            else:
                stop_reason = "batch_limit_reached"

            if stop_reason == "batch_limit_reached":
                pause_ingestion(connection, batch["process_key"])
            else:
                complete_ingestion(connection, batch["process_key"])
                completed = True
        else:
            total_pages = pages_processed
            completed = batch["overlap_reached"]
            stop_reason = batch["bronze_stop_reason"]

            logger.info(
                "market_data_stage_summary stage=trade_live product_id=%s "
                "pages_processed=%s trades_processed=%s completed=%s "
                "stop_reason=%s overlap_trade_id=%s next_after_cursor=%s",
                batch["product_id"],
                pages_processed,
                trades_processed,
                completed,
                stop_reason,
                batch["overlap_trade_id"],
                batch["next_after_cursor"],
            )

    result = {
        "source": batch.get("source"),
        "product_id": batch.get("product_id"),
        "pages_processed": pages_processed,
        "trades_processed": trades_processed,
        "activity_start": (
            activity_start.isoformat() if activity_start is not None else None
        ),
        "activity_end": (
            (activity_end + timedelta(microseconds=1)).isoformat()
            if activity_end is not None
            else None
        ),
        "completed": completed,
        "pages_committed": total_pages,
        "stop_reason": stop_reason,
    }

    if batch["mode"] == "catchup":
        logger.info(
            "market_data_stage_summary stage=trade_catchup product_id=%s "
            "pages_committed=%s page_amount=%s completed=%s stop_reason=%s",
            batch["product_id"],
            total_pages,
            batch["page_amount"],
            completed,
            stop_reason,
        )
    return result


@task
def build_continuation_conf(request):
    """Return the public parameters for the next bounded catch-up batch."""

    return json.dumps(
        {
            "after_cursor": request["initial_after_cursor"],
            "page_amount": request["page_amount"],
        }
    )


@task.branch
def choose_catchup_continuation(silver_result):
    """Continue an unfinished page-count catch-up in a new bounded DAG run."""

    if not silver_result["completed"]:
        return "continue_catchup"
    return "finish_trade_ingestion"


@task(retries=0)
def finish_trade_ingestion(request, silver_result):
    """Log cumulative catch-up totals after its terminal bounded run."""

    return _finish_trade_ingestion(request, silver_result)


def _finish_trade_ingestion(request, silver_result):
    """Read and log the durable totals for a completed catch-up."""

    with market_data_connection() as connection:
        summary = trade_ingestion_summary(connection, request["process_key"])

    logger.info(
        "market_data_dag_summary pipeline=trades mode=catchup "
        "product_id=%s status=%s total_pages_committed=%s "
        "total_trade_records_committed=%s final_batch_trade_records=%s "
        "stop_reason=%s initial_after_cursor=%s final_after_cursor=%s "
        "duration_seconds=%s",
        summary["product_id"],
        summary["status"],
        summary["pages_committed"],
        summary["trades_committed"],
        silver_result.get("trades_processed", 0),
        silver_result.get("stop_reason"),
        summary["initial_after_cursor"],
        summary["resume_after_cursor"],
        summary["duration_seconds"],
    )
    return summary


@task
def build_live_continuation_conf(batch):
    """Carry the bounded head scan's cursor and fixed overlap boundary."""

    return json.dumps(
        {
            "after_cursor": batch["next_after_cursor"],
            "overlap_trade_id": batch["overlap_trade_id"],
        }
    )


@task.branch
def choose_live_continuation(silver_result):
    """Continue scanning older pages until prior Silver coverage is reached."""

    if not silver_result["completed"]:
        return "continue_live_ingestion"
    return "finish_live_ingestion"


@task
def load_gold_trade_activity(silver_result):
    """Refresh Gold only for hours touched by the committed Silver pages."""

    if not silver_result.get("activity_start"):
        logger.info(
            "market_data_stage_summary stage=gold_trade_activity "
            "status=skipped reason=no_new_trades records_written=0 "
            "source_trades_processed=%s",
            silver_result.get("trades_processed", 0),
        )
        return 0

    start = datetime.fromisoformat(silver_result["activity_start"])
    end = datetime.fromisoformat(silver_result["activity_end"])
    with market_data_connection() as connection:
        rows_written = load_trade_activity_hourly(
            connection,
            source=silver_result["source"],
            product_id=silver_result["product_id"],
            start=start,
            end=end,
        )

    logger.info(
        "market_data_dag_summary pipeline=trades product_id=%s "
        "source_trades_processed=%s gold_records_written=%s",
        silver_result["product_id"],
        silver_result.get("trades_processed", 0),
        rows_written,
    )
    return rows_written


@task(retries=0)
def detect_newest_trade_gap():
    """Find one stable, recent gap and return a small approval summary."""

    detected_at = datetime.now(timezone.utc)
    eligible_before = detected_at - timedelta(
        minutes=GAP_REPAIR_RECENT_DATA_MINUTES
    )
    lookback_start = eligible_before - timedelta(days=GAP_REPAIR_LOOKBACK_DAYS)
    with market_data_connection() as connection:
        gap = find_newest_trade_gap(
            connection,
            source=COINBASE_SOURCE,
            product_id=PRODUCT_ID,
            lookback_start=lookback_start,
            eligible_before=eligible_before,
            minimum_gap_seconds=GAP_REPAIR_MINIMUM_SECONDS,
            maximum_page_amount=MAX_CATCHUP_PAGE_AMOUNT,
        )

    if gap is None:
        logger.info(
            "market_data_stage_summary stage=trade_gap_detection "
            "product_id=%s status=no_actionable_gap lookback_days=%s "
            "minimum_gap_seconds=%s recent_data_excluded_minutes=%s",
            PRODUCT_ID,
            GAP_REPAIR_LOOKBACK_DAYS,
            GAP_REPAIR_MINIMUM_SECONDS,
            GAP_REPAIR_RECENT_DATA_MINUTES,
        )
        return None

    logger.info(
        "market_data_stage_summary stage=trade_gap_detection product_id=%s "
        "status=approval_required older_trade_time=%s newer_trade_time=%s "
        "gap_seconds=%s after_cursor=%s estimated_page_amount=%s "
        "page_amount=%s page_amount_capped=%s",
        gap["product_id"],
        gap["older_trade_time"],
        gap["newer_trade_time"],
        gap["gap_seconds"],
        gap["after_cursor"],
        gap["estimated_page_amount"],
        gap["page_amount"],
        gap["page_amount_capped"],
    )
    return gap


@task.branch
def choose_gap_repair(gap):
    """Request approval only when an actionable gap was detected."""

    if gap is None:
        return "no_gap_found"
    return "approve_gap_repair"


@task
def build_gap_repair_conf(gap):
    """Build the existing catch-up DAG's public cursor parameters."""

    return json.dumps(
        {
            "after_cursor": gap["after_cursor"],
            "page_amount": gap["page_amount"],
        }
    )


def build_gap_repair_ingestion_request(gap):
    """Recreate the deterministic catch-up identity used by the trigger."""

    return {
        "mode": "catchup",
        **validate_catchup_request(
            source=COINBASE_SOURCE,
            product_id=gap["product_id"],
            after_cursor=gap["after_cursor"],
            page_amount=gap["page_amount"],
        ),
    }


@task.sensor(
    poke_interval=GAP_REPAIR_COMPLETION_POKE_SECONDS,
    timeout=timedelta(hours=GAP_REPAIR_COMPLETION_TIMEOUT_HOURS),
    mode="reschedule",
)
def wait_for_gap_repair_completion(gap):
    """Wait without holding a worker for the triggered catch-up chain."""

    return _wait_for_gap_repair_completion(gap)


def _wait_for_gap_repair_completion(gap):
    request = build_gap_repair_ingestion_request(gap)
    with market_data_connection() as connection:
        status = trade_ingestion_status(connection, request["process_key"])

    logger.info(
        "market_data_stage_summary stage=trade_gap_repair_wait "
        "product_id=%s process_status=%s process_key=%s",
        gap["product_id"],
        status or "NOT_CREATED",
        request["process_key"],
    )
    return PokeReturnValue(
        is_done=status == "COMPLETED",
        xcom_value=request if status == "COMPLETED" else None,
    )


@task(retries=0)
def finish_gap_repair(request, gap):
    """Log cumulative totals after the approved repair chain completes."""

    return _finish_gap_repair(request, gap)


def _finish_gap_repair(request, gap):
    with market_data_connection() as connection:
        summary = trade_ingestion_summary(connection, request["process_key"])

    logger.info(
        "market_data_dag_summary pipeline=trades mode=gap_repair "
        "product_id=%s status=%s total_pages_committed=%s "
        "total_trade_records_committed=%s gap_start=%s gap_end=%s "
        "initial_after_cursor=%s final_after_cursor=%s duration_seconds=%s",
        summary["product_id"],
        summary["status"],
        summary["pages_committed"],
        summary["trades_committed"],
        gap["older_trade_time"],
        gap["newer_trade_time"],
        summary["initial_after_cursor"],
        summary["resume_after_cursor"],
        summary["duration_seconds"],
    )
    return summary
