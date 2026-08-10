-- One durable progress record per source and product.  It is updated only
-- after the related Silver transaction has committed.
CREATE TABLE control.trade_watermarks (
    source TEXT NOT NULL,
    product_id TEXT NOT NULL,

    -- Coinbase's `after` cursor continues pagination toward older trades.
    resume_after_cursor TEXT,
    highest_trade_id BIGINT,

    last_success_at TIMESTAMPTZ,
    last_pipeline_run_id TEXT,
    last_committed_ingestion_key TEXT,

    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (source, product_id),

    CONSTRAINT trade_watermarks_highest_trade_id_check
        CHECK (highest_trade_id IS NULL OR highest_trade_id > 0),

    CONSTRAINT trade_watermarks_ingestion_fk
        FOREIGN KEY (last_committed_ingestion_key)
        REFERENCES control.trade_ingestions (ingestion_key)
);

-- Airflow needs to create and advance the operational state, but cannot
-- alter its structure or delete historical audit records.
REVOKE ALL ON SCHEMA control FROM PUBLIC;
GRANT USAGE ON SCHEMA control TO market_data_airflow;
GRANT SELECT, INSERT, UPDATE
    ON control.trade_ingestions, control.trade_watermarks
    TO market_data_airflow;
