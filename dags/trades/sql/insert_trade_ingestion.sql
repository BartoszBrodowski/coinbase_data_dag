INSERT INTO control.trade_ingestions (
    process_key,
    source,
    product_id,
    mode,
    oldest_time,
    initial_before_cursor,
    initial_after_cursor,
    resume_before_cursor,
    resume_after_cursor,
    status,
    last_pipeline_run_id
)
VALUES (
    %(process_key)s,
    %(source)s,
    %(product_id)s,
    %(mode)s,
    %(oldest_time)s,
    %(initial_before_cursor)s,
    %(initial_after_cursor)s,
    %(resume_before_cursor)s,
    %(resume_after_cursor)s,
    'ACTIVE',
    %(pipeline_run_id)s
);
