"""Manually triggered Coinbase backfills split into API-safe intervals."""

from datetime import datetime, timedelta, timezone

from airflow.sdk import Param, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

from bronze_ingest import ingest_one_interval
from coinbase_client import MAX_CANDLES_PER_REQUEST, SUPPORTED_GRANULARITIES
from gold_load import load_silver_to_gold, validate_gold_quality
from silver_transform import transform_bronze_to_silver
from utils import COINBASE_API_RETRY_ARGS, build_ingestion_key


BACKFILL_START_DATE = datetime(2025, 1, 1, tzinfo=timezone.utc)
MAX_BACKFILL_INTERVALS = 2_000
MAX_ACTIVE_API_TASKS = 2

BACKFILL_PARAMS = {
    "product_id": Param(
        "BTC-USD",
        type="string",
        pattern=r"^[A-Z0-9]+-[A-Z0-9]+$",
        title="Coinbase product",
        description="For example BTC-USD or ETH-USD.",
    ),
    "granularity_seconds": Param(
        3600,
        type="integer",
        enum=sorted(SUPPORTED_GRANULARITIES),
        title="Candle size in seconds",
    ),
    "start": Param(
        "2025-01-01T00:00:00Z",
        type="string",
        format="date-time",
        title="Start (inclusive)",
    ),
    "end": Param(
        "2025-01-02T00:00:00Z",
        type="string",
        format="date-time",
        title="End (exclusive)",
    ),
}


def parse_utc_datetime(value):
    """Parse an ISO 8601 value and normalize it to UTC."""

    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, ValueError) as error:
        raise ValueError("start and end must be ISO 8601 date-times") from error

    if parsed.tzinfo is None:
        raise ValueError("start and end must include a timezone")

    return parsed.astimezone(timezone.utc)


def split_backfill_interval(product_id, granularity_seconds, start, end):
    """Return consecutive intervals containing at most 300 candles each."""

    if granularity_seconds not in SUPPORTED_GRANULARITIES:
        raise ValueError("unsupported Coinbase candle granularity")
    if start >= end:
        raise ValueError("start must be earlier than end")

    interval_size = timedelta(
        seconds=granularity_seconds * MAX_CANDLES_PER_REQUEST
    )
    intervals = []
    interval_start = start

    while interval_start < end:
        interval_end = min(interval_start + interval_size, end)
        intervals.append(
            {
                "product_id": product_id,
                "granularity_seconds": granularity_seconds,
                "start": interval_start.isoformat(),
                "end": interval_end.isoformat(),
            }
        )
        interval_start = interval_end

        if len(intervals) > MAX_BACKFILL_INTERVALS:
            raise ValueError(
                f"backfill would create more than {MAX_BACKFILL_INTERVALS} requests"
            )

    return intervals


@task(retries=0)
def create_backfill_intervals():
    params = get_current_context()["params"]

    try:
        return split_backfill_interval(
            product_id=params["product_id"],
            granularity_seconds=params["granularity_seconds"],
            start=parse_utc_datetime(params["start"]),
            end=parse_utc_datetime(params["end"]),
        )
    except ValueError as error:
        # Invalid input will not become valid on a retry.
        raise AirflowFailException(str(error)) from error


@task(
    **COINBASE_API_RETRY_ARGS,
    max_active_tis_per_dag=MAX_ACTIVE_API_TASKS,
)
def ingest_backfill_interval(interval):
    context = get_current_context()
    task_instance = context["ti"]

    return ingest_one_interval(
        ingestion_key=build_ingestion_key(
            task_instance,
            map_index=task_instance.map_index,
        ),
        product_id=interval["product_id"],
        granularity_seconds=interval["granularity_seconds"],
        start=parse_utc_datetime(interval["start"]),
        end=parse_utc_datetime(interval["end"]),
    )


@task
def load_backfill_silver(ingestion_id):
    transform_bronze_to_silver(ingestion_id)
    return ingestion_id


@task
def load_backfill_gold(ingestion_id):
    load_silver_to_gold(ingestion_id)
    return ingestion_id


@task
def test_backfill_gold_quality():
    return validate_gold_quality()


@dag(
    dag_id="coinbase_backfill",
    start_date=BACKFILL_START_DATE,
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params=BACKFILL_PARAMS,
    tags=["coinbase", "backfill"],
)
def create_coinbase_backfill_dag():
    intervals = create_backfill_intervals()
    ingestion_ids = ingest_backfill_interval.expand(interval=intervals)
    silver_ingestion_ids = load_backfill_silver.expand(ingestion_id=ingestion_ids)
    gold_ingestion_ids = load_backfill_gold.expand(
        ingestion_id=silver_ingestion_ids
    )
    gold_ingestion_ids >> test_backfill_gold_quality()


dag = create_coinbase_backfill_dag()
