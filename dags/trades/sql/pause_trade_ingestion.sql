UPDATE control.trade_ingestions
SET
    status = CASE
        WHEN status = 'COMPLETED' THEN status
        ELSE 'PAUSED'
    END,
    updated_at = CURRENT_TIMESTAMP
WHERE ingestion_key = %(ingestion_key)s
  AND mode = 'catchup';
