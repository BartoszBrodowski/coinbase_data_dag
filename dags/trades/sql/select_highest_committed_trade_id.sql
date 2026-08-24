SELECT MAX(trade_id)
FROM silver.trades
WHERE source = %(source)s
  AND product_id = %(product_id)s;
