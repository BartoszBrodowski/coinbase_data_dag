SELECT
    source,
    product_id,
    status,
    pages_committed,
    trades_committed,
    initial_after_cursor,
    resume_after_cursor,
    created_at,
    completed_at
FROM control.trade_ingestions
WHERE process_key = %(process_key)s
  AND mode = 'catchup';
