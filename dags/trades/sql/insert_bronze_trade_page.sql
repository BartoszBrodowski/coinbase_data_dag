INSERT INTO bronze.coinbase_trades_raw (
    ingestion_key,
    product_id,
    response_cb_before,
    response_cb_after,
    request_duration_ms,
    http_status,
    content_type,
    response_headers,
    response_body,
    pipeline_run_id,
    pipeline_task_id,
    attempt_number
)
VALUES (
    %(ingestion_key)s,
    %(product_id)s,
    %(response_cb_before)s,
    %(response_cb_after)s,
    %(request_duration_ms)s,
    %(http_status)s,
    %(content_type)s,
    %(response_headers)s::jsonb,
    %(response_body)s,
    %(pipeline_run_id)s,
    %(pipeline_task_id)s,
    %(attempt_number)s
)
ON CONFLICT (ingestion_key) DO NOTHING
RETURNING ingestion_id;
