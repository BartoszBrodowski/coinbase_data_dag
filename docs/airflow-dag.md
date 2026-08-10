# Airflow market-data DAG

The `generate_dag` pipeline runs hourly for `BTC-USD`. It explicitly uses
Airflow's `CronDataIntervalTimetable`, so each scheduled run receives the
closed interval `[data_interval_start, data_interval_end)` rather than a single
trigger timestamp or a range calculated from the current time.

Airflow 3 may create a manual run without data-interval values. In that case,
the DAG asks the same timetable to infer the preceding closed hourly interval
from the run timestamp. Scheduled runs always use the interval supplied
directly by Airflow.

The task order is:

```text
ingest_bronze -> load_silver -> load_gold -> test_gold_quality
```

Tasks exchange only an ingestion ID and row counts through XCom. The Coinbase
payload remains in PostgreSQL Bronze and is never passed through Airflow's
metadata database.

Every task has two retries. The delay starts at one minute, grows
exponentially, and is capped at five minutes. Bronze uses the Airflow DAG ID,
run ID, task ID, and try number as its ingestion key, so every API attempt is
auditable. Silver and Gold use upserts, so retrying them does not create
duplicate candles.

## Manual verification

Trigger `generate_dag` from the Airflow UI and inspect Grid view. A successful
run contains four green tasks for one hourly interval. To test retry behavior,
clear a completed `load_gold` or `test_gold_quality` task and include its
downstream tasks. The repeated upsert and quality check should succeed without
changing the number of Gold candles.
