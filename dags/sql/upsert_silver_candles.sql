INSERT INTO silver.candles (
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
VALUES ('coinbase', %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
