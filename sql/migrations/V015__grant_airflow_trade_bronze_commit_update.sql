-- Bronze trade pages remain append-only except for the Silver processing
-- checkpoint written after a page has been committed successfully.
GRANT UPDATE (silver_committed_at)
    ON bronze.coinbase_trades_raw
    TO market_data_airflow;
