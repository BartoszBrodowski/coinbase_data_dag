"""Approval-gated repair for detected Coinbase trade coverage gaps."""

from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.hitl import ApprovalOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag
from airflow.utils.types import DagRunType

from trades.pipeline_tasks import (
    build_gap_repair_conf,
    choose_gap_repair,
    detect_newest_trade_gap,
    finish_gap_repair,
    wait_for_gap_repair_completion,
)


DAG_START_DATE = datetime(2026, 8, 10, tzinfo=timezone.utc)


@dag(
    dag_id="coinbase_trades_gap_repair",
    start_date=DAG_START_DATE,
    schedule=None,
    catchup=False,
    allowed_run_types={DagRunType.MANUAL},
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["coinbase", "trades", "gap-repair"],
)
def generate_gap_repair_dag():
    gap = detect_newest_trade_gap()
    repair_choice = choose_gap_repair(gap)

    approve_gap_repair = ApprovalOperator(
        task_id="approve_gap_repair",
        subject="Approve Coinbase trade gap repair?",
        body=(
            "**Product:** {{ ti.xcom_pull(task_ids='detect_newest_trade_gap')"
            "['product_id'] }}  \n"
            "**Gap:** {{ ti.xcom_pull(task_ids='detect_newest_trade_gap')"
            "['older_trade_time'] }} to "
            "{{ ti.xcom_pull(task_ids='detect_newest_trade_gap')"
            "['newer_trade_time'] }}  \n"
            "**Duration:** {{ ti.xcom_pull(task_ids='detect_newest_trade_gap')"
            "['gap_seconds'] }} seconds  \n"
            "**After cursor:** {{ ti.xcom_pull(task_ids='detect_newest_trade_gap')"
            "['after_cursor'] }}  \n"
            "**Estimated pages:** "
            "{{ ti.xcom_pull(task_ids='detect_newest_trade_gap')"
            "['estimated_page_amount'] }}  \n"
            "**Pages to request:** "
            "{{ ti.xcom_pull(task_ids='detect_newest_trade_gap')"
            "['page_amount'] }}  \n"
            "Approval starts the existing cursor-based catch-up DAG. "
            "Rejection writes no market data."
        ),
        defaults="Reject",
        response_timeout=timedelta(hours=24),
        fail_on_reject=False,
    )
    no_gap_found = EmptyOperator(task_id="no_gap_found")
    repair_conf = build_gap_repair_conf(gap)
    trigger_gap_repair = TriggerDagRunOperator(
        task_id="trigger_gap_repair",
        trigger_dag_id="coinbase_trades_catchup",
        conf="{{ ti.xcom_pull(task_ids='build_gap_repair_conf') }}",
        wait_for_completion=False,
        fail_when_dag_is_paused=True,
    )
    completed_request = wait_for_gap_repair_completion(gap)
    repair_summary = finish_gap_repair(completed_request, gap)

    gap >> repair_choice
    repair_choice >> [approve_gap_repair, no_gap_found]
    approve_gap_repair >> repair_conf >> trigger_gap_repair
    trigger_gap_repair >> completed_request >> repair_summary


gap_repair_dag = generate_gap_repair_dag()
