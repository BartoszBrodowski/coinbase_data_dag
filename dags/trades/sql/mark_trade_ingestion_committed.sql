UPDATE control.trade_ingestions
SET silver_committed_at = CURRENT_TIMESTAMP
WHERE ingestion_key = %(ingestion_key)s;
