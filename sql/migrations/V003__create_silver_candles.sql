CREATE TABLE silver.candles (
    source TEXT NOT NULL,
    product_id TEXT NOT NULL,
    granularity_seconds INTEGER NOT NULL,
    candle_start TIMESTAMPTZ NOT NULL,

    open_price NUMERIC(38, 18) NOT NULL,
    high_price NUMERIC(38, 18) NOT NULL,
    low_price NUMERIC(38, 18) NOT NULL,
    close_price NUMERIC(38, 18) NOT NULL,
    volume NUMERIC(38, 18) NOT NULL,

    bronze_ingestion_id BIGINT NOT NULL,

    PRIMARY KEY (
        source,
        product_id,
        granularity_seconds,
        candle_start
    ),

    CONSTRAINT candles_bronze_ingestion_fk
        FOREIGN KEY (bronze_ingestion_id)
        REFERENCES bronze.coinbase_candles_raw (ingestion_id),

    CONSTRAINT candles_granularity_supported
        CHECK (
            granularity_seconds IN (
                60,
                300,
                900,
                3600,
                21600,
                86400
            )
        ),

    CONSTRAINT candles_prices_positive
        CHECK (
            open_price > 0
            AND high_price > 0
            AND low_price > 0
            AND close_price > 0
        ),

    CONSTRAINT candles_volume_nonnegative
        CHECK (volume >= 0),

    CONSTRAINT candles_price_range_valid
        CHECK (
            low_price <= high_price
            AND open_price BETWEEN low_price AND high_price
            AND close_price BETWEEN low_price AND high_price
        )
);
