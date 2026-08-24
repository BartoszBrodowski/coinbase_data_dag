"""Manual cursor-based historical Coinbase trade ingestion."""

from datetime import datetime, timezone

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Param, dag
from airflow.utils.types import DagRunType

from trades.pipeline_tasks import (
    build_continuation_conf,
    choose_catchup_continuation,
    finish_trade_ingestion,
    ingest_bronze_pages,
    load_gold_trade_activity,
    load_silver_pages,
    resolve_catchup_ingestion_request,
)
from trades.settings import (
    CATCHUP_PAGES_PER_BATCH,
    DEFAULT_CATCHUP_PAGE_AMOUNT,
    MAX_CATCHUP_PAGE_AMOUNT,
)


DAG_START_DATE = datetime(2026, 8, 10, tzinfo=timezone.utc)
CATCHUP_PARAMS = {
    "after_cursor": Param(
        type="string",
        minLength=1,
        title="Initial catch-up cursor",
        description="Required cursor from which to fetch older trade pages.",
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
    completion_summary = finish_trade_ingestion(request, silver_result)
    continuation_conf >> continue_catchup
    continuation_choice >> [continue_catchup, completion_summary]


catchup_dag = generate_catchup_dag()
