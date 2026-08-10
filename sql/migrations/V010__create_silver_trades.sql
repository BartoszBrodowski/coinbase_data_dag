CREATE SCHEMA control;

CREATE TABLE control.trade_ingestions (
    ingestion_key TEXT PRIMARY KEY,
    product_id TEXT NOT NULL,
    requested_before TEXT,
    requested_after TEXT,
    response_cb_before TEXT,
    response_cb_after TEXT,
    fetched_at TIMESTAMPTZ NOT NULL,
    http_status SMALLINT NOT NULL,
    pipeline_run_id TEXT,
    silver_committed_at TIMESTAMPTZ
);

CREATE TABLE silver.trades (
    source TEXT NOT NULL,
    product_id TEXT NOT NULL,
    trade_id BIGINT NOT NULL,

    price NUMERIC(38, 18) NOT NULL,
    time TIMESTAMPTZ NOT NULL,
    size NUMERIC(38, 18) NOT NULL,
    side TEXT NOT NULL
        CHECK (side IN ('buy', 'sell')),

    ingestion_key TEXT NOT NULL,

    CONSTRAINT trades_ingestion_fk
        FOREIGN KEY (ingestion_key)
        REFERENCES control.trade_ingestions (ingestion_key),

    PRIMARY KEY (
        source,
        product_id,
        trade_id
    ),

    CONSTRAINT trades_prices_positive
        CHECK (
            price > 0
        ),

    CONSTRAINT trades_size_positive
        CHECK (size > 0)
);
