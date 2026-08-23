UPDATE control.trade_ingestions
SET
    status = CASE
        WHEN status = 'COMPLETED' THEN status
        ELSE 'PAUSED'
    END,
    updated_at = CURRENT_TIMESTAMP
WHERE process_key = %(process_key)s
  AND mode = 'catchup';
