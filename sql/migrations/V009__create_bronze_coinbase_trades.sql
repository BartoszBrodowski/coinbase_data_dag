CREATE TABLE bronze.coinbase_trades_raw (
    ingestion_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    ingestion_key TEXT NOT NULL UNIQUE,
    product_id TEXT NOT NULL,
    
    requested_before TEXT,
    requested_after TEXT,
    response_cb_before TEXT,
    response_cb_after TEXT,

    fetched_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    request_duration_ms INTEGER,
    http_status SMALLINT NOT NULL,
    content_type TEXT,
    response_headers JSONB,

    response_body TEXT NOT NULL,

    pipeline_run_id TEXT,
    pipeline_task_id TEXT,
    attempt_number INTEGER,

    CONSTRAINT coinbase_trades_raw_request_duration_check
        CHECK (request_duration_ms IS NULL OR request_duration_ms >= 0),

    CONSTRAINT coinbase_trades_raw_http_status_check
        CHECK (http_status BETWEEN 100 AND 599),

    CONSTRAINT coinbase_trades_raw_attempt_number_check
        CHECK (attempt_number IS NULL OR attempt_number > 0)
);

CREATE INDEX coinbase_trades_raw_fetched_at_idx
    ON bronze.coinbase_trades_raw (fetched_at);

CREATE INDEX coinbase_trades_raw_product_fetched_at_idx
    ON bronze.coinbase_trades_raw (product_id, fetched_at DESC);

COMMENT ON COLUMN bronze.coinbase_trades_raw.response_body IS
    'Exact response body received from Coinbase; may contain JSON, an error document, or an empty string.';

COMMENT ON COLUMN bronze.coinbase_trades_raw.response_headers IS
    'Relevant response headers only; request authorization headers and secrets must not be stored.';
