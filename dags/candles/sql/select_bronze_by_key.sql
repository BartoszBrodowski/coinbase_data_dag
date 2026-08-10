SELECT ingestion_id
FROM bronze.coinbase_candles_raw
WHERE ingestion_key = %(ingestion_key)s
