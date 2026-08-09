-- Flyway owns database objects and schema changes. Airflow is deliberately a
-- non-owner runtime role with only the DML required by the current pipeline.

-- PostgreSQL 13 lets PUBLIC create objects in the public schema by default.
-- Remove that escape hatch before granting narrowly scoped schema access.
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA bronze, silver, gold FROM PUBLIC;

GRANT USAGE ON SCHEMA bronze, silver, gold TO market_data_airflow;

-- Bronze is append-only. SELECT supports Bronze-to-Silver processing and
-- INSERT supports ingestion. ON CONFLICT DO NOTHING needs no UPDATE grant.
GRANT SELECT, INSERT
    ON ALL TABLES IN SCHEMA bronze
    TO market_data_airflow;
GRANT USAGE
    ON ALL SEQUENCES IN SCHEMA bronze
    TO market_data_airflow;

-- Silver uses idempotent upserts, hence SELECT + INSERT + UPDATE. DELETE and
-- TRUNCATE are intentionally absent.
GRANT SELECT, INSERT, UPDATE
    ON ALL TABLES IN SCHEMA silver
    TO market_data_airflow;
GRANT USAGE
    ON ALL SEQUENCES IN SCHEMA silver
    TO market_data_airflow;

-- Gold is currently a view owned by Flyway, not a table populated by Airflow.
GRANT SELECT
    ON ALL TABLES IN SCHEMA gold
    TO market_data_airflow;

-- Apply the same policy automatically to objects Flyway creates later.
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze
    GRANT SELECT, INSERT ON TABLES TO market_data_airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze
    GRANT USAGE ON SEQUENCES TO market_data_airflow;

ALTER DEFAULT PRIVILEGES IN SCHEMA silver
    GRANT SELECT, INSERT, UPDATE ON TABLES TO market_data_airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver
    GRANT USAGE ON SEQUENCES TO market_data_airflow;

ALTER DEFAULT PRIVILEGES IN SCHEMA gold
    GRANT SELECT ON TABLES TO market_data_airflow;
