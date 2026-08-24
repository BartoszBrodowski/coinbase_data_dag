WITH ordered_trades AS (
    SELECT
        trade_id,
        time,
        bronze_ingestion_id,
        LAG(trade_id) OVER (ORDER BY time, trade_id) AS older_trade_id,
        LAG(time) OVER (ORDER BY time, trade_id) AS older_trade_time
    FROM silver.trades
    WHERE source = %(source)s
      AND product_id = %(product_id)s
      AND time >= %(lookback_start)s
      AND time < %(eligible_before)s
),
actionable_gaps AS (
    SELECT
        ordered_trades.older_trade_time,
        ordered_trades.time AS newer_trade_time,
        EXTRACT(
            EPOCH FROM ordered_trades.time - ordered_trades.older_trade_time
        )::INTEGER AS gap_seconds,
        ordered_trades.older_trade_id,
        ordered_trades.trade_id AS newer_trade_id,
        bronze.response_cb_after AS after_cursor,
        (
            CEIL(
                (
                    ordered_trades.trade_id
                    - ordered_trades.older_trade_id
                )::NUMERIC / 1000
            )::INTEGER + 1
        ) AS estimated_page_amount
    FROM ordered_trades
    JOIN bronze.coinbase_trades_raw AS bronze
      ON bronze.ingestion_id = ordered_trades.bronze_ingestion_id
    WHERE ordered_trades.older_trade_time IS NOT NULL
      AND ordered_trades.time - ordered_trades.older_trade_time
          > make_interval(secs => %(minimum_gap_seconds)s)
      AND bronze.response_cb_after IS NOT NULL
      AND NOT EXISTS (
          SELECT 1
          FROM bronze.coinbase_trades_raw AS committed_page
          WHERE committed_page.product_id = %(product_id)s
            AND committed_page.requested_after = bronze.response_cb_after
            AND committed_page.http_status BETWEEN 200 AND 299
            AND committed_page.silver_committed_at IS NOT NULL
      )
)
SELECT
    older_trade_time,
    newer_trade_time,
    gap_seconds,
    older_trade_id,
    newer_trade_id,
    after_cursor,
    estimated_page_amount,
    LEAST(estimated_page_amount, %(maximum_page_amount)s) AS page_amount
FROM actionable_gaps
ORDER BY newer_trade_time DESC, newer_trade_id DESC
LIMIT 1;
