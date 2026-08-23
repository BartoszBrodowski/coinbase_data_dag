SELECT ingestion_id
FROM bronze.coinbase_trades_raw
WHERE bronze_request_key = %(bronze_request_key)s;
