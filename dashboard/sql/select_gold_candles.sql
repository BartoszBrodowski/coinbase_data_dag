SELECT
    candle_start,
    open_price,
    high_price,
    low_price,
    close_price,
    volume
FROM (
    SELECT
        candle_start,
        open_price,
        high_price,
        low_price,
        close_price,
        volume
    FROM gold.candles
    WHERE source = %(source)s
      AND product_id = %(product_id)s
      AND granularity_seconds = %(granularity_seconds)s
      AND candle_start >= %(start)s
      AND candle_start < %(end)s
    ORDER BY candle_start DESC
    LIMIT %(limit)s
) recent_candles
ORDER BY candle_start
