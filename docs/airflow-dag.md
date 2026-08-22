# Airflow market-data DAG

The `coinbase_candles_hourly` DAG runs hourly for `BTC-USD`. It uses Airflow's
`CronDataIntervalTimetable`, so each scheduled run receives the closed interval
`[data_interval_start, data_interval_end)` rather than a single trigger
timestamp or a range calculated from the current time.

Airflow 3 may create a manual run without data-interval values. In that case,
the DAG asks the same timetable to infer the preceding closed hourly interval
from the run timestamp. Scheduled runs always use the interval supplied
directly by Airflow.

The task order is:

```text
ingest_bronze -> load_silver -> load_gold
```

Tasks exchange only an ingestion ID through XCom. The Coinbase payload remains
in PostgreSQL Bronze and is never passed through Airflow's metadata database.

Every task has two retries. The delay starts at one minute, grows
exponentially, and is capped at five minutes. Bronze uses the Airflow DAG ID,
run ID, task ID, and try number as its ingestion key, so every API attempt is
auditable. Silver and Gold use upserts, so retrying them does not create
duplicate candles.

## Manual verification

Unpause and trigger `coinbase_candles_hourly` from the Airflow UI, then inspect
Grid view. A successful run contains three green tasks for one hourly interval.
To test retry behavior, clear a completed `load_silver` task and include its
downstream task. The repeated Silver and Gold upserts should succeed without
creating duplicate candles.
