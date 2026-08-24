CREATE INDEX trades_coverage_lookup_idx
    ON silver.trades (source, product_id, time, trade_id);

COMMENT ON INDEX silver.trades_coverage_lookup_idx IS
    'Supports bounded chronological trade-gap detection.';
