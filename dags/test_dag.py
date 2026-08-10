from datetime import datetime, timezone

import pendulum
from airflow.sdk import dag, get_current_context, task
from airflow.timetables.interval import CronDataIntervalTimetable
from bronze_ingest import ingest_one_interval
from gold_load import load_silver_to_gold, validate_gold_quality
from silver_transform import transform_bronze_to_silver
from utils import COINBASE_API_RETRY_ARGS, build_ingestion_key


PRODUCT_ID = "BTC-USD"
GRANULARITY_SECONDS = 3600
DAG_START_DATE = datetime(2025, 1, 1, tzinfo=timezone.utc)
DAG_SCHEDULE = CronDataIntervalTimetable("0 * * * *", timezone="UTC")
DAG_DEFAULT_ARGS = COINBASE_API_RETRY_ARGS


def get_data_interval(context):
    start = context.get("data_interval_start")
    end = context.get("data_interval_end")

    if start is not None and end is not None:
        return start, end

    interval = DAG_SCHEDULE.infer_manual_data_interval(
        run_after=pendulum.instance(context["dag_run"].run_after)
    )
    return interval.start, interval.end


@task
def ingest_bronze():
    context = get_current_context()
    task_instance = context["ti"]
    start, end = get_data_interval(context)

    return ingest_one_interval(
        ingestion_key=build_ingestion_key(task_instance),
        product_id=PRODUCT_ID,
        granularity_seconds=GRANULARITY_SECONDS,
        start=start,
        end=end,
    )


@task
def load_silver(ingestion_id):
    rows_processed = transform_bronze_to_silver(ingestion_id)
    return {
        "ingestion_id": ingestion_id,
        "rows_processed": rows_processed,
    }


@task
def load_gold(silver_result):
    ingestion_id = silver_result["ingestion_id"]
    rows_processed = load_silver_to_gold(ingestion_id)
    return {
        "ingestion_id": ingestion_id,
        "rows_processed": rows_processed,
    }


@task
def test_gold_quality(gold_result):
    return {
        "ingestion_id": gold_result["ingestion_id"],
        "rows_checked": validate_gold_quality(),
    }


@dag(
    start_date=DAG_START_DATE,
    schedule=DAG_SCHEDULE,
    catchup=False,
    default_args=DAG_DEFAULT_ARGS,
)
def generate_dag():
    ingestion_id = ingest_bronze()
    silver_result = load_silver(ingestion_id)
    gold_result = load_gold(silver_result)
    test_gold_quality(gold_result)


dag = generate_dag()
