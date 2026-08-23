from datetime import datetime, timedelta, timezone

from airflow.sdk import Param, dag, get_current_context, task
from airflow.timetables.interval import CronDataIntervalTimetable
from shared.utils import (
    COINBASE_API_RETRY_ARGS,
    build_bronze_request_key,
    fail_without_retry_on,
    market_data_connection,
)
from shared.coinbase_client import COINBASE_SOURCE, CoinbaseNonRetryableAPIError
from trades.bronze_ingest import ingest_trade_page
from trades.ingestion_state import (
    live_process_key,
    pause_ingestion,
    start_or_resume_ingestion,
    validate_catchup_request,
)
from trades.silver_transform import transform_bronze_to_silver
from trades.settings import (
    MAX_CATCHUP_PAGES_PER_RUN,
    MAX_LIVE_PAGES_PER_RUN,
    TASK_TIMEOUT_MINUTES,
    TRADES_API_POOL,
)


DAG_START_DATE = datetime(2026, 8, 10, tzinfo=timezone.utc)
DAG_SCHEDULE = CronDataIntervalTimetable("*/15 * * * *", timezone="UTC")
PRODUCT_ID = "BTC-USD"
# One DAG deliberately serves scheduled live ingestion and manual catch-up: both
# use the same Bronze-to-Silver path and durable cursor model.  Splitting them
# would duplicate orchestration without changing the data-processing behaviour.
# Airflow's standard Param form is static; it cannot hide catch-up-only fields
# when `mode` is `live`.  The live branch below deliberately ignores them.
TRADES_PARAMS = {
    "mode": Param(
        "live",
        type="string",
        enum=["live", "catchup"],
        values_display={
            "live": "Live - regular ingestion",
            "catchup": "Catch-up - historical repair",
        },
        title="Ingestion mode",
        description_md=(
            "**Live** - regular, scheduled ingestion. It advances the live "
            "cursor; trigger it manually only to resume normal ingestion after "
            "an incident.\n\n"
            "**Catch-up** - a bounded manual repair of older history. It uses "
            "a separate cursor and does not change the live cursor."
        ),
    ),
    "oldest_time": Param(
        None,
        type=["null", "string"],
        format="date-time",
        title="Oldest catch-up trade time (inclusive)",
        description="Used only when mode is catchup; ignored in live mode.",
    ),
    "after_cursor": Param(
        None,
        type=["null", "string"],
        title="Optional initial catch-up cursor",
        description="Used only when mode is catchup; ignored in live mode.",
    ),
    "max_pages": Param(
        MAX_LIVE_PAGES_PER_RUN,
        type="integer",
        minimum=1,
        maximum=MAX_CATCHUP_PAGES_PER_RUN,
        title="Maximum catch-up pages in this run",
        description="Used only when mode is catchup; live uses its configured limit.",
    ),
}


def build_ingestion_request(params, run_type):
    """Resolve scheduled live ingestion or a bounded manual catch-up."""

    mode = params.get("mode", "live")
    if mode == "live":
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
    if mode != "catchup":
        raise ValueError("trade ingestion mode must be live or catchup")

    normalized_run_type = getattr(run_type, "value", run_type)
    if normalized_run_type != "manual":
        raise ValueError("catch-up mode can only be triggered manually")

    return {
        "mode": "catchup",
        **validate_catchup_request(
            source=COINBASE_SOURCE,
            product_id=PRODUCT_ID,
            oldest_time=params.get("oldest_time"),
            after_cursor=params.get("after_cursor"),
            max_pages=params["max_pages"],
        ),
    }


@task(retries=0)
def resolve_ingestion_request():
    context = get_current_context()
    with fail_without_retry_on(ValueError):
        return build_ingestion_request(
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
        cursor, status = start_or_resume_ingestion(
            connection,
            request,
            pipeline_run_id,
        )

    if status == "COMPLETED":
        return {**request, "ingestion_ids": [], "already_completed": True}

    ingestion_ids = []
    cursor_direction = request.get(
        "cursor_direction",
        "after" if request["mode"] == "catchup" else "before",
    )

    for page_index in range(request["max_pages"]):
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

    return {**request, "ingestion_ids": ingestion_ids, "already_completed": False}


@task(retries=0)
def load_silver_pages(batch):
    """Validate Bronze pages and commit Silver rows plus watermarks."""

    with fail_without_retry_on(ValueError):
        return _load_silver_pages(batch)


def _load_silver_pages(batch):
    if batch["already_completed"]:
        return {"pages_processed": 0, "trades_processed": 0, "completed": True}

    pages_processed = 0
    trades_processed = 0
    completed = False

    with market_data_connection() as connection:
        for ingestion_id in batch["ingestion_ids"]:
            result = transform_bronze_to_silver(connection, ingestion_id)
            pages_processed += 1
            trades_processed += result["trades_processed"]
            completed = result["completed"]
            if completed:
                break

        if batch["mode"] == "catchup":
            pause_ingestion(connection, batch["process_key"])

    result = {
        "pages_processed": pages_processed,
        "trades_processed": trades_processed,
    }
    if batch["mode"] == "catchup":
        result["completed"] = completed
    return result


@dag(
    dag_id="coinbase_trades_ingest",
    start_date=DAG_START_DATE,
    schedule=DAG_SCHEDULE,
    catchup=False,
    max_active_runs=1,
    params=TRADES_PARAMS,
    tags=["coinbase", "trades"],
)
def generate_dag():
    request = resolve_ingestion_request()
    batch = ingest_bronze_pages(request)
    load_silver_pages(batch)


dag = generate_dag()
