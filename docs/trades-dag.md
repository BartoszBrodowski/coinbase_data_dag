# Coinbase trade ingestion DAGs

Trade ingestion is split into three DAGs that share the same Bronze, Silver, and
Gold processing tasks:

- `coinbase_trades_live` polls the latest `BTC-USD` trades every 15 minutes.
- `coinbase_trades_catchup` is an unscheduled, page-count historical repair
  started manually by a user.
- `coinbase_trades_gap_repair` detects one recent gap and requires explicit
  approval before it starts the catch-up DAG.

Each DAG has one discovery module under `dags/trades`: `live_dag.py`,
`catchup_dag.py`, and `gap_repair_dag.py`. Shared TaskFlow tasks and pure
request builders live in `pipeline_tasks.py`, which intentionally instantiates
no DAG objects. This keeps Airflow discovery ownership unambiguous while the
three workflows reuse the same ingestion implementation.

The live task order is:

```text
resolve_live_ingestion_request -> ingest_bronze_pages -> load_silver_pages
                               -> load_gold_trade_activity
                               -> choose_live_continuation
                                  -> continue_live_ingestion | finish_live_ingestion
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
a cursor poll rather than a logical data interval. It accepts scheduled and
user-triggered manual runs. A new scan fetches Coinbase's newest page without a
cursor, takes its `CB-AFTER` cursor, and walks toward older pages until the page
range reaches the highest trade ID that was already committed to Silver when
the scan began.

Each live DAG run fetches at most three pages. If that bounded batch does not
reach prior coverage, `continue_live_ingestion` creates an operator-triggered
run carrying the next `after` cursor and the fixed overlap trade ID. Manual runs
therefore provide an immediate complete refresh rather than a separate cursor
stream. `max_active_runs=1` and the
`coinbase_trades_api` Airflow pool prevent concurrent requests from competing
for the same process state.

## Page-count catch-up

A user starts `coinbase_trades_catchup` manually and chooses `page_amount` from
1 to 1,000. The DAG has no schedule, mode selector, or date range. A non-empty
`after_cursor` is required and explicitly identifies the historical position
from which Coinbase should return older pages. The catch-up never selects a
live cursor implicitly.

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

On the terminal run, `finish_trade_ingestion` logs one cumulative summary for
the complete chained request. Search its log for `market_data_dag_summary` to
see total committed pages, total validated trade records committed to Silver,
the final batch record count, stop reason, initial and final cursors, and total
elapsed process time. The record total describes validated records processed;
it is not presented as a count of guaranteed new inserts because Silver writes
are idempotent upserts.

Airflow's built-in schedule Backfill is excluded from both DAGs through their
allowed run types. Its logical dates cannot select Coinbase trade pages because
this endpoint is cursor-based.

## Approval-gated gap repair

Trigger `coinbase_trades_gap_repair` manually to inspect the newest actionable
gap from the last seven days. Detection ignores the most recent 30 minutes so
it does not mistake a live ingestion still in progress for missing coverage. A
gap is actionable when adjacent committed BTC-USD trades are more than 30
seconds apart and the required Coinbase `after` cursor has not already been
committed.

When a gap is found, `approve_gap_repair` enters Airflow's `awaiting_input`
state. Its required-action view shows the gap boundaries, duration, cursor,
estimated page count, and bounded page count that will be requested. **Approve**
triggers `coinbase_trades_catchup`; **Reject** skips it and writes no market
data. The approval expires after 24 hours and defaults to rejection. If no gap
is found, the DAG ends successfully at `no_gap_found` without asking for input.

After approval, `trigger_gap_repair` still starts the catch-up asynchronously.
`wait_for_gap_repair_completion` then checks the durable process state once per
minute in reschedule mode, so it does not occupy a worker while the serial
catch-up runs continue. When the complete chain reaches `COMPLETED`,
`finish_gap_repair` logs `market_data_dag_summary` with cumulative pages,
validated trade records, gap boundaries, cursors, and elapsed process time.

Only the newest gap is proposed per run. The page request is capped by the
catch-up DAG's normal maximum; run the detector again after a repair to verify
coverage and propose the next remaining gap.

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
