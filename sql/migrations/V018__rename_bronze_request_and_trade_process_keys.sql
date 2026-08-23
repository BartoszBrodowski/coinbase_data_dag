-- A process owns durable cursor state across live or catch-up runs. A Bronze
-- request key identifies one individual HTTP request attempt within that process.
ALTER TABLE control.trade_ingestions
    RENAME COLUMN ingestion_key TO process_key;

ALTER TABLE bronze.coinbase_trades_raw
    RENAME COLUMN ingestion_key TO bronze_request_key;

ALTER TABLE bronze.coinbase_trades_raw
    RENAME COLUMN trade_ingestion_key TO process_key;

ALTER TABLE bronze.coinbase_candles_raw
    RENAME COLUMN ingestion_key TO bronze_request_key;

COMMENT ON COLUMN control.trade_ingestions.process_key IS
    'Stable identifier for one durable live or catch-up ingestion process.';

COMMENT ON COLUMN bronze.coinbase_trades_raw.bronze_request_key IS
    'Identifier for one Bronze HTTP request attempt; unique per task attempt and page.';

COMMENT ON COLUMN bronze.coinbase_trades_raw.process_key IS
    'Identifier of the durable ingestion process that owns this Bronze request.';

COMMENT ON COLUMN bronze.coinbase_candles_raw.bronze_request_key IS
    'Identifier for one Bronze HTTP request attempt; unique per task attempt.';
