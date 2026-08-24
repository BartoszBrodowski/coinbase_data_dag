"""Scheduled and manually triggered Coinbase trade head ingestion."""

from datetime import datetime, timezone

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.utils.types import DagRunType

from trades.pipeline_tasks import (
    build_live_continuation_conf,
    choose_live_continuation,
    ingest_bronze_pages,
    load_gold_trade_activity,
    load_silver_pages,
    resolve_live_ingestion_request,
)


DAG_START_DATE = datetime(2026, 8, 10, tzinfo=timezone.utc)
LIVE_DAG_SCHEDULE = CronTriggerTimetable("*/15 * * * *", timezone="UTC")


@dag(
    dag_id="coinbase_trades_live",
    start_date=DAG_START_DATE,
    schedule=LIVE_DAG_SCHEDULE,
    catchup=False,
    allowed_run_types={
        DagRunType.SCHEDULED,
        DagRunType.MANUAL,
        DagRunType.OPERATOR_TRIGGERED,
    },
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["coinbase", "trades", "live"],
)
def generate_live_dag():
    request = resolve_live_ingestion_request()
    batch = ingest_bronze_pages(request)
    silver_result = load_silver_pages(batch)
    gold_result = load_gold_trade_activity(silver_result)
    continuation_conf = build_live_continuation_conf(batch)
    continuation_choice = choose_live_continuation(silver_result)
    gold_result >> continuation_choice

    continue_live_ingestion = TriggerDagRunOperator(
        task_id="continue_live_ingestion",
        trigger_dag_id="coinbase_trades_live",
        conf="{{ ti.xcom_pull(task_ids='build_live_continuation_conf') }}",
        wait_for_completion=False,
        fail_when_dag_is_paused=True,
    )
    finish_live_ingestion = EmptyOperator(task_id="finish_live_ingestion")
    continuation_conf >> continue_live_ingestion
    continuation_choice >> [continue_live_ingestion, finish_live_ingestion]


live_dag = generate_live_dag()
