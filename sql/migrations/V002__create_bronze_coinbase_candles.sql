CREATE TABLE bronze.coinbase_candles_raw (
    ingestion_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- A deterministic identifier for one outbound request attempt. It allows a
    -- retried database write to be idempotent without collapsing API retries.
    ingestion_key TEXT NOT NULL UNIQUE,

    product_id TEXT NOT NULL,
    granularity_seconds INTEGER NOT NULL,
    requested_start TIMESTAMPTZ,
    requested_end TIMESTAMPTZ,

    fetched_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    request_duration_ms INTEGER,
    http_status SMALLINT NOT NULL,
    content_type TEXT,
    response_headers JSONB,

    -- Store the response exactly as received. Parsing and candle-shape
    -- validation belong to the Bronze-to-Silver transformation.
    response_body TEXT NOT NULL,

    pipeline_run_id TEXT,
    pipeline_task_id TEXT,
    attempt_number INTEGER,

    CONSTRAINT coinbase_candles_raw_requested_range_check
        CHECK (
            requested_start IS NULL
            OR requested_end IS NULL
            OR requested_start < requested_end
        ),

    CONSTRAINT coinbase_candles_raw_request_duration_check
        CHECK (request_duration_ms IS NULL OR request_duration_ms >= 0),

    CONSTRAINT coinbase_candles_raw_http_status_check
        CHECK (http_status BETWEEN 100 AND 599),

    CONSTRAINT coinbase_candles_raw_attempt_number_check
        CHECK (attempt_number IS NULL OR attempt_number > 0)
);

COMMENT ON COLUMN bronze.coinbase_candles_raw.response_body IS
    'Exact response body received from Coinbase; may contain JSON, an error document, or an empty string.';

COMMENT ON COLUMN bronze.coinbase_candles_raw.response_headers IS
    'Relevant response headers only; request authorization headers and secrets must not be stored.';
