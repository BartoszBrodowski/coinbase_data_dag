INSERT INTO silver.candles AS current_candle (
    source,
    product_id,
    granularity_seconds,
    candle_start,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    bronze_ingestion_id
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
    volume = EXCLUDED.volume,
    bronze_ingestion_id = EXCLUDED.bronze_ingestion_id
-- Update if the newest write has newer fetched_at, if it's same compare ingestion_ids (they're incremental)
-- Done in case a manual run happens at the same time with a scheduled run, we keep the fresher data
WHERE (
    SELECT (b.fetched_at, b.ingestion_id)
    FROM bronze.coinbase_candles_raw AS b
    WHERE b.ingestion_id = EXCLUDED.bronze_ingestion_id
) > (
    SELECT (b.fetched_at, b.ingestion_id)
    FROM bronze.coinbase_candles_raw AS b
    WHERE b.ingestion_id = current_candle.bronze_ingestion_id
);