DROP VIEW gold.candles;

CREATE TABLE gold.candles (
    source TEXT NOT NULL,
    product_id TEXT NOT NULL,
    granularity_seconds INTEGER NOT NULL,
    candle_start TIMESTAMPTZ NOT NULL,
    open_price NUMERIC(38, 18) NOT NULL,
    high_price NUMERIC(38, 18) NOT NULL,
    low_price NUMERIC(38, 18) NOT NULL,
    close_price NUMERIC(38, 18) NOT NULL,
    volume NUMERIC(38, 18) NOT NULL,

    PRIMARY KEY (
        source,
        product_id,
        granularity_seconds,
        candle_start
    )
);

-- Preserve data that was visible through the former view.
INSERT INTO gold.candles (
    source,
    product_id,
    granularity_seconds,
    candle_start,
    open_price,
    high_price,
    low_price,
    close_price,
    volume
)
SELECT
    source,
    product_id,
    granularity_seconds,
    candle_start,
    open_price,
    high_price,
    low_price,
    close_price,
    volume
FROM silver.candles;

CREATE INDEX gold_candles_lookup_idx
    ON gold.candles (
        product_id,
        granularity_seconds,
        candle_start
    );

GRANT SELECT, INSERT, UPDATE
    ON gold.candles
    TO market_data_airflow;

ALTER DEFAULT PRIVILEGES IN SCHEMA gold
    GRANT SELECT, INSERT, UPDATE ON TABLES TO market_data_airflow;
