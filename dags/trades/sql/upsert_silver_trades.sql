INSERT INTO silver.trades (
    source,
    product_id,
    trade_id,
    price,
    time,
    size,
    side,
    bronze_ingestion_id
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (source, product_id, trade_id) DO UPDATE
SET
    price = EXCLUDED.price,
    time = EXCLUDED.time,
    size = EXCLUDED.size,
    side = EXCLUDED.side,
    bronze_ingestion_id = EXCLUDED.bronze_ingestion_id;
