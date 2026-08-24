SELECT
    hour_start,
    trade_count,
    base_volume,
    quote_volume,
    vwap,
    maker_buy_count,
    maker_sell_count
FROM (
    SELECT
        hour_start,
        trade_count,
        base_volume,
        quote_volume,
        vwap,
        maker_buy_count,
        maker_sell_count
    FROM gold.trade_activity_hourly
    WHERE source = %(source)s
      AND product_id = %(product_id)s
      AND hour_start >= %(start)s
      AND hour_start < %(end)s
    ORDER BY hour_start DESC
    LIMIT %(limit)s
) recent_activity
ORDER BY hour_start
