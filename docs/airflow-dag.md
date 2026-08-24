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
Each stage writes a compact `market_data_stage_summary` to its normal Airflow
task log. Silver includes fetched, accepted, duplicate, filtered, and written
counts. Candle Silver also logs expected, actual, and missing hourly buckets;
partial coverage is a warning only and does not fail the task. These values are
not stored in a metrics table or returned through XCom.

Every task has two retries. The delay starts at one minute, grows
exponentially, and is capped at five minutes. Bronze uses the Airflow DAG ID,
run ID, task ID, and try number as its `bronze_request_key`, so every API
attempt is auditable. Silver and Gold use upserts, so retrying them does not
create duplicate candles.

Payload shape, numeric values, UTC timestamps, granularity alignment, and OHLC
relationships are validated during the Silver load. PostgreSQL constraints
provide a second contract boundary. Persisted quality and freshness results
are planned but are not currently a separate task in this DAG.

## Manual verification

Unpause and trigger `coinbase_candles_hourly` from the Airflow UI, then inspect
Grid view. A successful run contains three green tasks for one hourly interval.
To test retry behavior, clear a completed `load_silver` task and include its
downstream task. The repeated Silver and Gold upserts should succeed without
creating duplicate candles.
