SELECT
    ingestion_key,
    product_id,
    response_cb_after,
    pipeline_run_id,
    http_status,
    response_body
FROM bronze.coinbase_trades_raw
WHERE ingestion_id = %(ingestion_id)s;
