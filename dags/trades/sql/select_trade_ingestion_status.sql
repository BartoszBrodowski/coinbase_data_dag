SELECT status
FROM control.trade_ingestions
WHERE process_key = %(process_key)s
  AND mode = 'catchup';
