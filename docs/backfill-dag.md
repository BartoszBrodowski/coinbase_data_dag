# Coinbase backfill DAG

Use the `coinbase_backfill` DAG for manually loading a historical range. The
regular hourly DAG remains responsible only for current, scheduled intervals.

In Airflow, open `coinbase_backfill`, select **Trigger DAG**, and provide:

- `product_id`, for example `BTC-USD`;
- `granularity_seconds`, selected from the values supported by Coinbase;
- `start`, the inclusive ISO 8601 start time;
- `end`, the exclusive ISO 8601 end time.

Both dates must include a timezone, for example `2025-01-01T00:00:00Z`.

The first task validates the input and divides the range into consecutive
requests of no more than 300 candles. Airflow dynamically maps the Bronze,
Silver, and Gold tasks over those intervals. At most two Coinbase requests run
at once, only the API tasks are retried, and one Gold quality check runs after
all mapped intervals finish.

For example, one year of hourly candles becomes 30 API requests instead of one
request that Coinbase rejects. Re-running the DAG is safe because Silver and
Gold use upserts; each individual run and mapped interval also receives its own
Bronze ingestion key.
