-- The first Gold contract mirrors Silver because the current consumer only
-- needs validated OHLCV candles. A view avoids duplicating data at this stage.
CREATE VIEW gold.candles AS
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
