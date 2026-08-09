-- Prepare the runtime role for a future physical Gold table populated by an
-- Airflow task. The current Gold object remains a read-oriented view.
GRANT INSERT
    ON ALL TABLES IN SCHEMA gold
    TO market_data_airflow;

GRANT USAGE
    ON ALL SEQUENCES IN SCHEMA gold
    TO market_data_airflow;

ALTER DEFAULT PRIVILEGES IN SCHEMA gold
    GRANT INSERT ON TABLES TO market_data_airflow;

ALTER DEFAULT PRIVILEGES IN SCHEMA gold
    GRANT USAGE ON SEQUENCES TO market_data_airflow;
