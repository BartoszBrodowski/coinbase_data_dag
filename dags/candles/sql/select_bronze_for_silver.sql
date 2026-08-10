SELECT
    product_id,
    granularity_seconds,
    requested_start,
    requested_end,
    response_body
FROM bronze.coinbase_candles_raw
WHERE ingestion_id = %(ingestion_id)s
