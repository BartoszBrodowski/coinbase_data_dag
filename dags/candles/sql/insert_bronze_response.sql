INSERT INTO bronze.coinbase_candles_raw (
    bronze_request_key,
    product_id,
    granularity_seconds,
    requested_start,
    requested_end,
    request_duration_ms,
    http_status,
    content_type,
    response_headers,
    response_body
)
VALUES (
    %(bronze_request_key)s,
    %(product_id)s,
    %(granularity_seconds)s,
    %(requested_start)s,
    %(requested_end)s,
    %(request_duration_ms)s,
    %(http_status)s,
    %(content_type)s,
    %(response_headers)s::jsonb,
    %(response_body)s
)
-- Prevents duplicate Bronze request-key entries from being
-- inserted into the table (in case of hiccups or retries)
ON CONFLICT (bronze_request_key) DO NOTHING
RETURNING ingestion_id
