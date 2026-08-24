import json
import logging
from datetime import datetime, timedelta, timezone

from airflow.sdk import Param, dag, get_current_context, task
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
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
    live_process_key,
    pause_ingestion,
    start_or_resume_ingestion,
    validate_catchup_request,
)
from trades.gold_load import load_trade_activity_hourly
from trades.silver_transform import transform_bronze_to_silver
from trades.settings import (
    CATCHUP_PAGES_PER_BATCH,
    DEFAULT_CATCHUP_PAGE_AMOUNT,
    MAX_CATCHUP_PAGE_AMOUNT,
    MAX_LIVE_PAGES_PER_RUN,
    TASK_TIMEOUT_MINUTES,
    TRADES_API_POOL,
)


logger = logging.getLogger(__name__)
DAG_START_DATE = datetime(2026, 8, 10, tzinfo=timezone.utc)
LIVE_DAG_SCHEDULE = CronTriggerTimetable("*/15 * * * *", timezone="UTC")
PRODUCT_ID = "BTC-USD"
CATCHUP_PARAMS = {
    "after_cursor": Param(
        None,
        type=["null", "string"],
        title="Optional initial catch-up cursor",
        description=(
            "Leave empty to start from the live stream's last committed "
            "historical cursor."
        ),
    ),
    "page_amount": Param(
        DEFAULT_CATCHUP_PAGE_AMOUNT,
        type="integer",
        minimum=1,
        maximum=MAX_CATCHUP_PAGE_AMOUNT,
        title="Historical pages to fetch",
        description=(
            "Total page amount to fetch. Airflow continues automatically "
            f"in batches of {CATCHUP_PAGES_PER_BATCH} pages."
        ),
    ),
}


def build_live_ingestion_request():
    """Build the request used by the scheduled live cursor poll."""

    return {
        "mode": "live",
        "source": COINBASE_SOURCE,
        "product_id": PRODUCT_ID,
        "process_key": live_process_key(COINBASE_SOURCE, PRODUCT_ID),
        "oldest_time": None,
        "cursor_direction": "before",
        "initial_before_cursor": None,
        "initial_after_cursor": None,
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
    return build_live_ingestion_request()


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

    if status == "COMPLETED":
        return {
            **request,
            "ingestion_ids": [],
            "already_completed": True,
            "pages_committed_before": pages_committed,
            "overlap_reached": False,
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
    for page_index in range(pages_in_batch):
        if request["mode"] == "catchup" and cursor is not None:
            with market_data_connection() as connection:
                if committed_after_cursor_exists(
                    connection,
                    request["product_id"],
                    cursor,
                ):
                    overlap_reached = True
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

        if next_cursor is None or next_cursor == cursor:
            break
        cursor = next_cursor

    return {
        **request,
        "ingestion_ids": ingestion_ids,
        "already_completed": False,
        "pages_committed_before": pages_committed,
        "overlap_reached": overlap_reached,
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
                stop_reason = "committed_cursor_reached"
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
            stop_reason = None

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


@task
def load_gold_trade_activity(silver_result):
    """Refresh Gold only for hours touched by the committed Silver pages."""

    if not silver_result.get("activity_start"):
        logger.info(
            "market_data_stage_summary stage=gold_trade_activity "
            "status=skipped reason=no_new_trades"
        )
        return 0

    start = datetime.fromisoformat(silver_result["activity_start"])
    end = datetime.fromisoformat(silver_result["activity_end"])
    with market_data_connection() as connection:
        return load_trade_activity_hourly(
            connection,
            source=silver_result["source"],
            product_id=silver_result["product_id"],
            start=start,
            end=end,
        )


@dag(
    dag_id="coinbase_trades_live",
    start_date=DAG_START_DATE,
    schedule=LIVE_DAG_SCHEDULE,
    catchup=False,
    allowed_run_types={DagRunType.SCHEDULED},
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["coinbase", "trades", "live"],
)
def generate_live_dag():
    request = resolve_live_ingestion_request()
    batch = ingest_bronze_pages(request)
    silver_result = load_silver_pages(batch)
    load_gold_trade_activity(silver_result)


@dag(
    dag_id="coinbase_trades_catchup",
    start_date=DAG_START_DATE,
    schedule=None,
    catchup=False,
    allowed_run_types={DagRunType.MANUAL, DagRunType.OPERATOR_TRIGGERED},
    max_active_runs=1,
    is_paused_upon_creation=False,
    params=CATCHUP_PARAMS,
    tags=["coinbase", "trades", "catchup"],
)
def generate_catchup_dag():
    request = resolve_catchup_ingestion_request()
    batch = ingest_bronze_pages(request)
    silver_result = load_silver_pages(batch)
    gold_result = load_gold_trade_activity(silver_result)
    continuation_conf = build_continuation_conf(request)
    continuation_choice = choose_catchup_continuation(silver_result)
    gold_result >> continuation_choice

    continue_catchup = TriggerDagRunOperator(
        task_id="continue_catchup",
        trigger_dag_id="coinbase_trades_catchup",
        conf="{{ ti.xcom_pull(task_ids='build_continuation_conf') }}",
        wait_for_completion=False,
        fail_when_dag_is_paused=True,
    )
    finish_trade_ingestion = EmptyOperator(task_id="finish_trade_ingestion")
    continuation_conf >> continue_catchup
    continuation_choice >> [continue_catchup, finish_trade_ingestion]


live_dag = generate_live_dag()
catchup_dag = generate_catchup_dag()
