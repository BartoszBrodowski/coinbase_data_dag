SELECT 1
FROM bronze.coinbase_trades_raw
WHERE product_id = %(product_id)s
  AND requested_after = %(requested_after)s
  AND http_status BETWEEN 200 AND 299
  AND silver_committed_at IS NOT NULL
LIMIT 1;
