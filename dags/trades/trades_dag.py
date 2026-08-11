from datetime import datetime, timedelta, timezone

from airflow.sdk import Param, dag, get_current_context, task
from airflow.timetables.interval import CronDataIntervalTimetable
from shared.utils import (
    COINBASE_API_RETRY_ARGS,
    build_ingestion_key,
    fail_without_retry_on,
    market_data_connection,
)
from shared.coinbase_client import CoinbaseNonRetryableAPIError
from trades.bronze_ingest import ingest_trade_page
from trades.ingestion_state import (
    live_ingestion_key,
    pause_ingestion,
    start_or_resume_ingestion,
    validate_catchup_request,
)
from trades.silver_transform import transform_bronze_to_silver
from trades.settings import MAX_PAGES_PER_RUN, TASK_TIMEOUT_MINUTES, TRADES_API_POOL


DAG_START_DATE = datetime(2026, 8, 10, tzinfo=timezone.utc)
DAG_SCHEDULE = CronDataIntervalTimetable("*/15 * * * *", timezone="UTC")
PRODUCT_ID = "BTC-USD"
SOURCE = "coinbase"
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
        MAX_PAGES_PER_RUN,
        type="integer",
        minimum=1,
        maximum=20,
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
            "source": SOURCE,
            "product_id": PRODUCT_ID,
            "ingestion_key": live_ingestion_key(PRODUCT_ID),
            "oldest_time": None,
            "initial_after_cursor": None,
            "max_pages": MAX_PAGES_PER_RUN,
        }
    if mode != "catchup":
        raise ValueError("trade ingestion mode must be live or catchup")

    normalized_run_type = getattr(run_type, "value", run_type)
    if normalized_run_type != "manual":
        raise ValueError("catch-up mode can only be triggered manually")

    return {
        "mode": "catchup",
        **validate_catchup_request(
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
        after, status = start_or_resume_ingestion(
            connection,
            request,
            pipeline_run_id,
        )
        if status == "COMPLETED":
            return {**request, "ingestion_ids": [], "already_completed": True}

        ingestion_ids = []

        for page_index in range(request["max_pages"]):
            ingestion_id = ingest_trade_page(
                connection,
                ingestion_key=build_ingestion_key(task_instance, map_index=page_index),
                trade_ingestion_key=request["ingestion_key"],
                product_id=request["product_id"],
                after=after,
                pipeline_run_id=pipeline_run_id,
                pipeline_task_id=task_instance.task_id,
                attempt_number=task_instance.try_number,
            )
            ingestion_ids.append(ingestion_id)

            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT response_cb_after
                    FROM bronze.coinbase_trades_raw
                    WHERE ingestion_id = %(ingestion_id)s
                    """,
                    {"ingestion_id": ingestion_id},
                )
                next_cursor = cursor.fetchone()[0]

            if next_cursor is None or next_cursor == after:
                break
            after = next_cursor

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
            pause_ingestion(connection, batch["ingestion_key"])

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
