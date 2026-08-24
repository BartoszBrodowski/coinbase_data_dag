SELECT
    source,
    product_id,
    MIN(hour_start) AS first_hour,
    MAX(hour_start) AS last_hour
FROM gold.trade_activity_hourly
GROUP BY source, product_id
ORDER BY source, product_id
LIMIT 100
