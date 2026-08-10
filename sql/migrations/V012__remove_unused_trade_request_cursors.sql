ALTER TABLE bronze.coinbase_trades_raw
    DROP COLUMN requested_before,
    DROP COLUMN requested_after;

ALTER TABLE control.trade_ingestions
    DROP COLUMN requested_before,
    DROP COLUMN requested_after;
