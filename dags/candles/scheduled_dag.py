from datetime import datetime, timezone

import pendulum
from airflow.sdk import dag, get_current_context, task
from airflow.timetables.interval import CronDataIntervalTimetable
from candles.bronze_ingest import ingest_one_interval
from candles.gold_load import load_silver_to_gold
from candles.silver_transform import transform_bronze_to_silver
from shared.utils import (
    COINBASE_API_RETRY_ARGS,
    build_ingestion_key,
    fail_without_retry_on,
)
from shared.coinbase_client import COINBASE_SOURCE, CoinbaseNonRetryableAPIError


PRODUCT_ID = "BTC-USD"
GRANULARITY_SECONDS = 3600
DAG_START_DATE = datetime(2025, 1, 1, tzinfo=timezone.utc)
DAG_SCHEDULE = CronDataIntervalTimetable("0 * * * *", timezone="UTC")


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

    with fail_without_retry_on(CoinbaseNonRetryableAPIError, ValueError):
        return ingest_one_interval(
            ingestion_key=build_ingestion_key(task_instance),
            product_id=PRODUCT_ID,
            granularity_seconds=GRANULARITY_SECONDS,
            start=start,
            end=end,
        )


@task
def load_silver(ingestion_id):
    transform_bronze_to_silver(
        ingestion_id,
        source=COINBASE_SOURCE,
    )
    return ingestion_id


@task
def load_gold(ingestion_id):
    load_silver_to_gold(ingestion_id)


@dag(
    dag_id="coinbase_candles_hourly",
    start_date=DAG_START_DATE,
    schedule=DAG_SCHEDULE,
    catchup=False,
    default_args=COINBASE_API_RETRY_ARGS,
)
def generate_dag():
    ingestion_id = ingest_bronze()
    ingestion_id = load_silver(ingestion_id)
    load_gold(ingestion_id)


dag = generate_dag()
