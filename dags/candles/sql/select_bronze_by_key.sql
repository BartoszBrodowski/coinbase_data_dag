SELECT ingestion_id
FROM bronze.coinbase_candles_raw
WHERE bronze_request_key = %(bronze_request_key)s
