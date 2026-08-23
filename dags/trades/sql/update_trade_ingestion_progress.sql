UPDATE control.trade_ingestions
SET
    resume_before_cursor = COALESCE(
        %(response_cb_before)s,
        resume_before_cursor
    ),
    resume_after_cursor = COALESCE(
        %(response_cb_after)s,
        resume_after_cursor
    ),
    highest_trade_id = CASE
        WHEN %(highest_trade_id)s IS NULL THEN highest_trade_id
        WHEN highest_trade_id IS NULL THEN %(highest_trade_id)s
        ELSE GREATEST(highest_trade_id, %(highest_trade_id)s)
    END,
    status = CASE
        WHEN %(completed)s THEN 'COMPLETED'
        ELSE 'ACTIVE'
    END,
    pages_committed = pages_committed + 1,
    trades_committed = trades_committed + %(trades_committed)s,
    last_pipeline_run_id = %(pipeline_run_id)s,
    last_bronze_ingestion_id = %(bronze_ingestion_id)s,
    updated_at = CURRENT_TIMESTAMP,
    completed_at = CASE
        WHEN %(completed)s THEN CURRENT_TIMESTAMP
        ELSE NULL
    END
WHERE process_key = %(process_key)s;
