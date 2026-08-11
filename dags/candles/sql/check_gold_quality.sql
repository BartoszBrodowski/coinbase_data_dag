WITH duplicate_keys AS (
    SELECT
        source,
        product_id,
        granularity_seconds,
        candle_start
    FROM gold.candles
    GROUP BY
        source,
        product_id,
        granularity_seconds,
        candle_start
    HAVING COUNT(*) > 1
), ingestion_candles AS (
    SELECT
        gold.candle_start,
        gold.granularity_seconds,
        gold.product_id,
        gold.source
    FROM gold.candles AS gold
    JOIN silver.candles AS silver
      ON silver.source = gold.source
     AND silver.product_id = gold.product_id
     AND silver.granularity_seconds = gold.granularity_seconds
     AND silver.candle_start = gold.candle_start
    WHERE silver.bronze_ingestion_id = %(ingestion_id)s
), sequenced_candles AS (
    SELECT
        candle_start,
        granularity_seconds,
        LAG(candle_start) OVER (
            PARTITION BY source, product_id, granularity_seconds
            ORDER BY candle_start
        ) AS previous_candle_start
    FROM ingestion_candles
)
SELECT
    (SELECT COUNT(*) FROM silver.candles) AS silver_count,
    (SELECT COUNT(*) FROM gold.candles) AS gold_count,
    (SELECT COUNT(*) FROM duplicate_keys) AS duplicate_count,
    (
        SELECT COUNT(*)
        FROM gold.candles
        WHERE source IS NULL
           OR product_id IS NULL
           OR granularity_seconds IS NULL
           OR candle_start IS NULL
           OR open_price IS NULL
           OR high_price IS NULL
           OR low_price IS NULL
           OR close_price IS NULL
           OR volume IS NULL
    ) AS null_count,
    (
        SELECT COUNT(*)
        FROM gold.candles
        WHERE low_price > high_price
           OR open_price NOT BETWEEN low_price AND high_price
           OR close_price NOT BETWEEN low_price AND high_price
    ) AS invalid_ohlc_count,
    (
        SELECT COUNT(*)
        FROM sequenced_candles
        WHERE previous_candle_start IS NOT NULL
          AND candle_start - previous_candle_start
              <> granularity_seconds * INTERVAL '1 second'
    ) AS time_gap_count;
