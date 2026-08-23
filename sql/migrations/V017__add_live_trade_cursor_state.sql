ALTER TABLE control.trade_ingestions
    ADD COLUMN initial_before_cursor TEXT,
    ADD COLUMN resume_before_cursor TEXT;

COMMENT ON COLUMN control.trade_ingestions.initial_before_cursor IS
    'Initial cursor for live pagination toward newer trades.';

COMMENT ON COLUMN control.trade_ingestions.resume_before_cursor IS
    'Last committed Coinbase before cursor for live pagination.';
