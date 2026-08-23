from contextlib import closing, contextmanager
from datetime import timedelta
from pathlib import Path

from airflow.sdk.exceptions import AirflowFailException


COINBASE_API_RETRY_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
}


def load_sql(sql_directory: Path, filename: str) -> str:
    """Read a UTF-8 SQL file from a domain-specific SQL directory."""

    return (sql_directory / filename).read_text(encoding="utf-8")


def build_bronze_request_key(task_instance, *, map_index=None):
    parts = [
        task_instance.dag_id,
        task_instance.run_id,
        task_instance.task_id,
    ]
    if map_index is not None:
        parts.append(map_index)
    parts.append(task_instance.try_number)
    return ":".join(str(part) for part in parts)


@contextmanager
def fail_without_retry_on(*exception_types):
    """Turn deterministic failures into terminal Airflow task failures."""

    try:
        yield
    except exception_types as error:
        raise AirflowFailException(str(error)) from error


@contextmanager
def market_data_connection():
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id="market_data")
    with closing(hook.get_conn()) as connection:
        yield connection
