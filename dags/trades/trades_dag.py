
from datetime import datetime, timezone

from airflow.sdk import dag, get_current_context, task
from airflow.timetables.interval import CronDataIntervalTimetable
from candles.utils import COINBASE_API_RETRY_ARGS, build_ingestion_key
from candles.utils import market_data_connection
from trades.bronze_ingest import ingest_trade_page



DAG_START_DATE = datetime(2026, 8, 10, tzinfo=timezone.utc)
DAG_SCHEDULE = CronDataIntervalTimetable("*/15 * * * *", timezone="UTC")
DAG_DEFAULT_ARGS = COINBASE_API_RETRY_ARGS
PRODUCT_ID = "BTC-USD"
SOURCE = "coinbase"


@task
def ingest_bronze():
    context = get_current_context()
    task_instance = context["ti"]

    with market_data_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT resume_after_cursor
                FROM control.trade_watermarks
                WHERE source = %(source)s AND product_id = %(product_id)s
                """,
                {"source": SOURCE, "product_id": PRODUCT_ID},
            )
            watermark = cursor.fetchone()

        return ingest_trade_page(
            connection,
            ingestion_key=build_ingestion_key(task_instance),
            product_id=PRODUCT_ID,
            after=watermark[0] if watermark else None,
            pipeline_run_id=context["dag_run"].run_id,
            pipeline_task_id=task_instance.task_id,
            attempt_number=task_instance.try_number,
        )

@dag(
    start_date=DAG_START_DATE,
    schedule=DAG_SCHEDULE,
    catchup=False,
    default_args=DAG_DEFAULT_ARGS,
)
def generate_dag():
    ingestion_id = ingest_bronze()


dag = generate_dag()
