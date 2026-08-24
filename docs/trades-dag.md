# Coinbase trade ingestion DAGs

Trade ingestion is split into two DAGs that share the same Bronze, Silver, and
Gold processing tasks:

- `coinbase_trades_live` polls the latest `BTC-USD` trades every 15 minutes.
- `coinbase_trades_catchup` is an unscheduled, page-count historical repair
  started manually by a user.

The live task order is:

```text
resolve_live_ingestion_request -> ingest_bronze_pages -> load_silver_pages
                               -> load_gold_trade_activity
```

The catch-up task order is:

```text
resolve_catchup_ingestion_request -> ingest_bronze_pages -> load_silver_pages
                                  -> load_gold_trade_activity
                                  -> choose_catchup_continuation
                                     -> continue_catchup | finish_trade_ingestion
```

## Live ingestion

`coinbase_trades_live` uses a trigger timetable every 15 minutes, so each run is
a cursor poll rather than a logical data interval. It accepts scheduled runs
only, fetches at most three sequential pages, and uses Coinbase's `CB-BEFORE`
cursor to move toward newer trades. `max_active_runs=1` and the
`coinbase_trades_api` Airflow pool prevent concurrent requests from competing
for the same process state.

## Page-count catch-up

A user starts `coinbase_trades_catchup` manually and chooses `page_amount` from
1 to 1,000. The DAG has no schedule, mode selector, or date range. An optional
`after_cursor` can seed a specific historical position; when it is empty, the
catch-up starts from the live stream's last Silver-committed historical cursor.

Each DAG run fetches at most four sequential pages. An unfinished catch-up is
paused after that bounded batch and `continue_catchup` creates the next
operator-triggered DAG run with the same public parameters. The catch-up DAG
allows manual runs for the initial request and operator-triggered runs for these
internal continuations. Because `max_active_runs=1`, the batches cannot compete
for the process cursor. A request for 100 pages therefore uses at most 25 serial
DAG runs without requiring user retriggers.

The catch-up stops when its requested `page_amount` is committed, Coinbase has
no older cursor, or the next exact `requested_after` cursor already exists in
Bronze with `silver_committed_at` set. This overlap check uses the current
schema; no separate checkpoint table is required. Silver's natural trade key
still makes row writes idempotent.

Airflow's built-in schedule Backfill is excluded from both DAGs through their
allowed run types. Its logical dates cannot select Coinbase trade pages because
this endpoint is cursor-based.

## Commit and retry behavior

Each API response is stored in `bronze.coinbase_trades_raw` before its HTTP
status is raised. Retryable failures therefore remain auditable. Silver rows,
the Bronze processing checkpoint, and the process cursor are committed in one
database transaction.

Timeouts, HTTP 429 responses, and server errors use bounded Airflow retries.
Invalid parameters, non-retryable HTTP errors, and malformed trade payloads
fail without retry.

## Gold trade activity

After Silver commits a batch, `load_gold_trade_activity` expands the touched
range to complete UTC-hour boundaries and recomputes those hours from Silver.
It does not increment counters from the API page, so overlapping pages, partial
hour batches, and task retries remain idempotent.

The resulting `gold.trade_activity_hourly` table contains trade count, base
volume, quote volume, VWAP, and maker buy/sell counts. The dashboard reads this
Gold contract through its read-only database role; it does not access Silver
or Bronze directly.

Retention and capacity metrics remain roadmap work in TICKET-010.
