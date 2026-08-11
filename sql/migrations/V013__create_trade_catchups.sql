CREATE TABLE control.trade_catchups (
    catchup_key TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    product_id TEXT NOT NULL,
    oldest_time TIMESTAMPTZ,
    initial_after_cursor TEXT,
    resume_after_cursor TEXT,
    status TEXT NOT NULL DEFAULT 'PENDING',
    pages_committed INTEGER NOT NULL DEFAULT 0,
    trades_committed BIGINT NOT NULL DEFAULT 0,
    last_pipeline_run_id TEXT,
    last_committed_ingestion_key TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,

    CONSTRAINT trade_catchups_boundary_check
        CHECK (oldest_time IS NOT NULL OR initial_after_cursor IS NOT NULL),
    CONSTRAINT trade_catchups_status_check
        CHECK (status IN ('PENDING', 'RUNNING', 'PAUSED', 'COMPLETED')),
    CONSTRAINT trade_catchups_pages_check
        CHECK (pages_committed >= 0),
    CONSTRAINT trade_catchups_trades_check
        CHECK (trades_committed >= 0),
    CONSTRAINT trade_catchups_ingestion_fk
        FOREIGN KEY (last_committed_ingestion_key)
        REFERENCES control.trade_ingestions (ingestion_key)
);

CREATE INDEX trade_catchups_active_idx
    ON control.trade_catchups (product_id, status, updated_at DESC)
    WHERE status <> 'COMPLETED';

GRANT SELECT, INSERT, UPDATE
    ON control.trade_catchups
    TO market_data_airflow;
