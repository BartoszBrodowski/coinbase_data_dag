from datetime import datetime, timezone

from airflow.sdk import dag, get_current_context, task
from bronze_ingest import ingest_one_interval
from silver_transform import transform_bronze_to_silver


PRODUCT_ID = "BTC-USD"
GRANULARITY_SECONDS = 3600
START = datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)
END = datetime(2025, 1, 1, 6, 0, tzinfo=timezone.utc)


@task
def ingest_bronze():
    context = get_current_context()
    task_instance = context["ti"]
    ingestion_key = (
        f"{task_instance.dag_id}:"
        f"{task_instance.run_id}:"
        f"{task_instance.task_id}:"
        f"{task_instance.try_number}"
    )

    return ingest_one_interval(
        ingestion_key=ingestion_key,
        product_id=PRODUCT_ID,
        granularity_seconds=GRANULARITY_SECONDS,
        start=START,
        end=END,
    )


@task
def load_silver(ingestion_id):
    return transform_bronze_to_silver(ingestion_id)


@dag(start_date=datetime(2021, 1, 1, tzinfo=timezone.utc), schedule=None, catchup=False)
def generate_dag():
    ingestion_id = ingest_bronze()
    load_silver(ingestion_id)


generate_dag()
