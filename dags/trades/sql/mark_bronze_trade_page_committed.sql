UPDATE bronze.coinbase_trades_raw
SET silver_committed_at = CURRENT_TIMESTAMP
WHERE ingestion_id = %(ingestion_id)s;
