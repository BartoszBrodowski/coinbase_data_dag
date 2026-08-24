CREATE TABLE gold.trade_activity_hourly (
    source TEXT NOT NULL,
    product_id TEXT NOT NULL,
    hour_start TIMESTAMPTZ NOT NULL,

    trade_count BIGINT NOT NULL,
    base_volume NUMERIC(38, 18) NOT NULL,
    quote_volume NUMERIC(38, 18) NOT NULL,
    vwap NUMERIC(38, 18) NOT NULL,
    maker_buy_count BIGINT NOT NULL,
    maker_sell_count BIGINT NOT NULL,

    PRIMARY KEY (source, product_id, hour_start),

    CONSTRAINT trade_activity_trade_count_check
        CHECK (trade_count >= 0),
    CONSTRAINT trade_activity_base_volume_check
        CHECK (base_volume >= 0),
    CONSTRAINT trade_activity_quote_volume_check
        CHECK (quote_volume >= 0),
    CONSTRAINT trade_activity_vwap_check
        CHECK (vwap >= 0),
    CONSTRAINT trade_activity_buy_count_check
        CHECK (maker_buy_count >= 0),
    CONSTRAINT trade_activity_sell_count_check
        CHECK (maker_sell_count >= 0),
    CONSTRAINT trade_activity_side_count_check
        CHECK (maker_buy_count + maker_sell_count = trade_count)
);

CREATE INDEX gold_trade_activity_lookup_idx
    ON gold.trade_activity_hourly (product_id, hour_start);

COMMENT ON TABLE gold.trade_activity_hourly IS
    'UTC hourly trade aggregates derived from validated Silver trades.';

COMMENT ON COLUMN gold.trade_activity_hourly.vwap IS
    'Volume-weighted average trade price: quote_volume / base_volume.';

GRANT SELECT, INSERT, UPDATE
    ON gold.trade_activity_hourly
    TO market_data_airflow;

GRANT SELECT
    ON gold.trade_activity_hourly
    TO market_data_dashboard;
