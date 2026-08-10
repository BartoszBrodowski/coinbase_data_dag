INSERT INTO control.trade_watermarks (
    source,
    product_id,
    resume_after_cursor,
    highest_trade_id,
    last_success_at,
    last_pipeline_run_id,
    last_committed_ingestion_key,
    updated_at
)
VALUES (
    %(source)s,
    %(product_id)s,
    %(resume_after_cursor)s,
    %(highest_trade_id)s,
    CURRENT_TIMESTAMP,
    %(pipeline_run_id)s,
    %(ingestion_key)s,
    CURRENT_TIMESTAMP
)
ON CONFLICT (source, product_id) DO UPDATE
SET
    resume_after_cursor = COALESCE(
        EXCLUDED.resume_after_cursor,
        control.trade_watermarks.resume_after_cursor
    ),
    highest_trade_id = CASE
        WHEN EXCLUDED.highest_trade_id IS NULL
            THEN control.trade_watermarks.highest_trade_id
        WHEN control.trade_watermarks.highest_trade_id IS NULL
            THEN EXCLUDED.highest_trade_id
        ELSE GREATEST(
            control.trade_watermarks.highest_trade_id,
            EXCLUDED.highest_trade_id
        )
    END,
    last_success_at = EXCLUDED.last_success_at,
    last_pipeline_run_id = EXCLUDED.last_pipeline_run_id,
    last_committed_ingestion_key = EXCLUDED.last_committed_ingestion_key,
    updated_at = EXCLUDED.updated_at;
