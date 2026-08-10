INSERT INTO control.trade_ingestions (
    ingestion_key,
    product_id,
    response_cb_before,
    response_cb_after,
    fetched_at,
    http_status,
    pipeline_run_id
)
SELECT
    ingestion_key,
    product_id,
    response_cb_before,
    response_cb_after,
    fetched_at,
    http_status,
    pipeline_run_id
FROM bronze.coinbase_trades_raw
WHERE ingestion_key = %(ingestion_key)s
ON CONFLICT (ingestion_key) DO NOTHING;
