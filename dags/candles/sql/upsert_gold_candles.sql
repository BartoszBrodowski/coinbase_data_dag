INSERT INTO gold.candles (
    source,
    product_id,
    granularity_seconds,
    candle_start,
    open_price,
    high_price,
    low_price,
    close_price,
    volume
)
SELECT
    source,
    product_id,
    granularity_seconds,
    candle_start,
    open_price,
    high_price,
    low_price,
    close_price,
    volume
FROM silver.candles
WHERE bronze_ingestion_id = %(ingestion_id)s
ON CONFLICT (
    source,
    product_id,
    granularity_seconds,
    candle_start
)
DO UPDATE SET
    open_price = EXCLUDED.open_price,
    high_price = EXCLUDED.high_price,
    low_price = EXCLUDED.low_price,
    close_price = EXCLUDED.close_price,
    volume = EXCLUDED.volume
