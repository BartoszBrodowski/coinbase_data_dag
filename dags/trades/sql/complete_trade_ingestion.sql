UPDATE control.trade_ingestions
SET
    status = 'COMPLETED',
    updated_at = CURRENT_TIMESTAMP,
    completed_at = COALESCE(completed_at, CURRENT_TIMESTAMP)
WHERE process_key = %(process_key)s
  AND mode = 'catchup';
