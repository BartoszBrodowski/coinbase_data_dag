INSERT INTO gold.trade_activity_hourly (
    source,
    product_id,
    hour_start,
    trade_count,
    base_volume,
    quote_volume,
    vwap,
    maker_buy_count,
    maker_sell_count
)
SELECT
    trades.source,
    trades.product_id,
    date_trunc('hour', trades.time AT TIME ZONE 'UTC') AT TIME ZONE 'UTC',
    COUNT(*)::BIGINT,
    SUM(trades.size),
    SUM(trades.price * trades.size),
    SUM(trades.price * trades.size) / NULLIF(SUM(trades.size), 0),
    COUNT(*) FILTER (WHERE trades.side = 'buy')::BIGINT,
    COUNT(*) FILTER (WHERE trades.side = 'sell')::BIGINT
FROM silver.trades AS trades
WHERE trades.source = %(source)s
  AND trades.product_id = %(product_id)s
  AND trades.time >= %(start)s
  AND trades.time < %(end)s
GROUP BY
    trades.source,
    trades.product_id,
    date_trunc('hour', trades.time AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
ON CONFLICT (source, product_id, hour_start) DO UPDATE
SET
    trade_count = EXCLUDED.trade_count,
    base_volume = EXCLUDED.base_volume,
    quote_volume = EXCLUDED.quote_volume,
    vwap = EXCLUDED.vwap,
    maker_buy_count = EXCLUDED.maker_buy_count,
    maker_sell_count = EXCLUDED.maker_sell_count;
