# Gold read-model contracts

Gold is a physical table populated from validated Silver candles. A table is
used instead of a view so the consumer has a stable, independently indexed
read model. The trade-off is a small amount of duplicated data and an explicit
Silver-to-Gold task.

## Columns

The table exposes only fields needed to identify and draw a candle:

| Column | Purpose |
|---|---|
| `source` | Market-data provider |
| `product_id` | Trading pair, for example `BTC-USD` |
| `granularity_seconds` | Candle duration |
| `candle_start` | UTC start time |
| `open_price` | Opening price |
| `high_price` | Highest price |
| `low_price` | Lowest price |
| `close_price` | Closing price |
| `volume` | Traded volume |

The primary key is `(source, product_id, granularity_seconds, candle_start)`.
An additional index on `(product_id, granularity_seconds, candle_start)` serves
the dashboard's main filter.

## Read query

The dashboard can obtain sorted candles with one parameterized query:

```sql
SELECT
    candle_start,
    open_price,
    high_price,
    low_price,
    close_price,
    volume
FROM gold.candles
WHERE product_id = %(product_id)s
  AND granularity_seconds = %(granularity_seconds)s
  AND candle_start >= %(start)s
  AND candle_start < %(end)s
ORDER BY candle_start;
```

Gold is updated with an idempotent upsert. Processing the same Silver records
again updates the existing candles instead of creating duplicates.

## Hourly trade activity

`gold.trade_activity_hourly` is the read model for the incremental trade
pipeline. Its grain is one UTC hour per source and product. It contains the
number of trades, base volume, quote volume, VWAP, and maker-side buy/sell
counts.

The table is rebuilt for only the hours touched by each committed Silver batch.
The aggregation is calculated from `silver.trades` and written with an upsert,
so overlapping API pages and retries do not double-count activity.
