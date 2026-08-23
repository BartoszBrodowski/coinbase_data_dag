ALTER TABLE bronze.coinbase_trades_raw
    ADD COLUMN requested_before TEXT,
    ADD COLUMN requested_after TEXT;

COMMENT ON COLUMN bronze.coinbase_trades_raw.requested_before IS
    'Cursor sent to Coinbase in the before query parameter, when present.';

COMMENT ON COLUMN bronze.coinbase_trades_raw.requested_after IS
    'Cursor sent to Coinbase in the after query parameter, when present.';
