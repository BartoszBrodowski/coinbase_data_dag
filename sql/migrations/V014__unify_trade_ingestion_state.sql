CREATE TABLE control.trade_ingestion_processes (
    ingestion_key TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    product_id TEXT NOT NULL,
    mode TEXT NOT NULL,
    oldest_time TIMESTAMPTZ,
    initial_after_cursor TEXT,
    resume_after_cursor TEXT,
    highest_trade_id BIGINT,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    pages_committed INTEGER NOT NULL DEFAULT 0,
    trades_committed BIGINT NOT NULL DEFAULT 0,
    last_pipeline_run_id TEXT,
    last_bronze_ingestion_id BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,

    CONSTRAINT trade_ingestion_processes_mode_check
        CHECK (mode IN ('live', 'catchup')),
    CONSTRAINT trade_ingestion_processes_boundary_check
        CHECK (
            mode = 'live'
            OR oldest_time IS NOT NULL
            OR initial_after_cursor IS NOT NULL
        ),
    CONSTRAINT trade_ingestion_processes_status_check
        CHECK (status IN ('ACTIVE', 'PAUSED', 'COMPLETED')),
    CONSTRAINT trade_ingestion_processes_highest_trade_id_check
        CHECK (highest_trade_id IS NULL OR highest_trade_id > 0),
    CONSTRAINT trade_ingestion_processes_pages_check
        CHECK (pages_committed >= 0),
    CONSTRAINT trade_ingestion_processes_trades_check
        CHECK (trades_committed >= 0)
);

-- This is a clean-slate migration: legacy Bronze and Silver trade data and
-- their cursor state are deliberately discarded rather than copied forward.
TRUNCATE TABLE silver.trades, bronze.coinbase_trades_raw;

ALTER TABLE bronze.coinbase_trades_raw
    ADD COLUMN trade_ingestion_key TEXT,
    ADD COLUMN silver_committed_at TIMESTAMPTZ;

ALTER TABLE bronze.coinbase_trades_raw
    ALTER COLUMN trade_ingestion_key SET NOT NULL,
    ADD CONSTRAINT coinbase_trades_raw_process_fk
        FOREIGN KEY (trade_ingestion_key)
        REFERENCES control.trade_ingestion_processes (ingestion_key);

CREATE INDEX coinbase_trades_raw_process_idx
    ON bronze.coinbase_trades_raw (trade_ingestion_key, ingestion_id);

ALTER TABLE silver.trades
    ADD COLUMN bronze_ingestion_id BIGINT;

ALTER TABLE silver.trades
    ALTER COLUMN bronze_ingestion_id SET NOT NULL,
    DROP CONSTRAINT trades_ingestion_fk,
    DROP COLUMN ingestion_key;

CREATE INDEX trades_bronze_ingestion_id_idx
    ON silver.trades (bronze_ingestion_id);

COMMENT ON COLUMN silver.trades.bronze_ingestion_id IS
    'Durable page-lineage identifier; intentionally has no foreign key so Bronze retention cannot block Silver.';

DROP TABLE control.trade_catchups;
DROP TABLE control.trade_watermarks;
DROP TABLE control.trade_ingestions;

ALTER TABLE control.trade_ingestion_processes
    RENAME TO trade_ingestions;

CREATE UNIQUE INDEX trade_ingestions_live_product_idx
    ON control.trade_ingestions (source, product_id)
    WHERE mode = 'live';

CREATE INDEX trade_ingestions_active_idx
    ON control.trade_ingestions (product_id, mode, status, updated_at DESC)
    WHERE status <> 'COMPLETED';

GRANT SELECT, INSERT, UPDATE
    ON control.trade_ingestions
    TO market_data_airflow;
