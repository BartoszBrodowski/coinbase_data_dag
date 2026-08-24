# Coinbase trade ingestion DAG

The `coinbase_trades_ingest` DAG runs every 15 minutes for `BTC-USD`. It uses a
bounded micro-batch rather than attempting to retain the complete Coinbase
trade tape.

The task order is:

```text
resolve_ingestion_request -> ingest_bronze_pages -> load_silver_pages
```

## Live ingestion

Scheduled runs use `mode=live`, fetch at most three sequential pages, and use
Coinbase's `CB-BEFORE` cursor to move toward newer trades. `max_active_runs=1`
and the `coinbase_trades_api` Airflow pool prevent concurrent requests from
competing for the same process state.

## Manual catch-up

A manual run can use `mode=catchup`. It must include either `oldest_time` or an
explicit `after_cursor`, and it is limited to four pages per run. Scheduled
runs cannot enable catch-up mode.

An unfinished catch-up is paused after its bounded page batch. Triggering the
same request again produces the same stable process key and resumes from its
last Silver-committed cursor.

## Commit and retry behavior

Each API response is stored in `bronze.coinbase_trades_raw` before its HTTP
status is raised. Retryable failures therefore remain auditable. Silver rows,
the Bronze processing checkpoint, and the process cursor are committed in one
database transaction.

Timeouts, HTTP 429 responses, and server errors use bounded Airflow retries.
Invalid parameters, non-retryable HTTP errors, and malformed trade payloads
fail without retry.

## Current scope

The implemented trade path ends in `silver.trades`. Hourly Gold aggregates,
retention, and capacity metrics are explicit roadmap work in TICKET-010; they
are not represented as completed features.
