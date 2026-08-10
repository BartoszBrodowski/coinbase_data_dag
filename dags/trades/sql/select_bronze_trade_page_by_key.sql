SELECT ingestion_id
FROM bronze.coinbase_trades_raw
WHERE ingestion_key = %(ingestion_key)s;
