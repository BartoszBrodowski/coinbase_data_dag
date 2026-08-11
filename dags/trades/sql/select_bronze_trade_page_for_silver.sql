SELECT
    pages.ingestion_id,
    pages.trade_ingestion_key,
    pages.product_id,
    processes.mode,
    processes.oldest_time,
    pages.response_cb_after,
    pages.pipeline_run_id,
    pages.http_status,
    pages.response_body
FROM bronze.coinbase_trades_raw AS pages
JOIN control.trade_ingestions AS processes
    ON processes.ingestion_key = pages.trade_ingestion_key
WHERE pages.ingestion_id = %(ingestion_id)s;
