SELECT
    source,
    product_id,
    granularity_seconds,
    MIN(candle_start) AS first_candle,
    MAX(candle_start) AS last_candle
FROM gold.candles
WHERE granularity_seconds = 86400
GROUP BY source, product_id, granularity_seconds
ORDER BY source, product_id, granularity_seconds
LIMIT 100
