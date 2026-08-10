#!/usr/bin/env bash
set -eu

: "${POSTGRES_USER:?POSTGRES_USER must be set}"
: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD must be set}"
: "${MARKET_DATA_DB:?MARKET_DATA_DB must be set}"
: "${FLYWAY_USER:?FLYWAY_USER must be set}"
: "${AIRFLOW_MARKET_DATA_PASSWORD:?AIRFLOW_MARKET_DATA_PASSWORD must be set}"
: "${DASHBOARD_DB_PASSWORD:?DASHBOARD_DB_PASSWORD must be set}"

export PGPASSWORD="$POSTGRES_PASSWORD"

# CREATE ROLE has no IF NOT EXISTS form. \gexec creates missing roles, while
# ALTER keeps passwords and restrictions synchronized for existing volumes.
psql \
    --host postgres \
    --username "$POSTGRES_USER" \
    --dbname "$MARKET_DATA_DB" \
    --set=ON_ERROR_STOP=1 \
    --set=market_data_db="$MARKET_DATA_DB" \
    --set=flyway_user="$FLYWAY_USER" \
    --set=airflow_password="$AIRFLOW_MARKET_DATA_PASSWORD" \
    --set=dashboard_password="$DASHBOARD_DB_PASSWORD" <<'EOSQL'
SELECT 'CREATE ROLE market_data_airflow'
WHERE NOT EXISTS (
    SELECT FROM pg_catalog.pg_roles WHERE rolname = 'market_data_airflow'
) \gexec

SELECT 'CREATE ROLE market_data_dashboard'
WHERE NOT EXISTS (
    SELECT FROM pg_catalog.pg_roles WHERE rolname = 'market_data_dashboard'
) \gexec

ALTER ROLE market_data_airflow
    WITH LOGIN
    PASSWORD :'airflow_password'
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    NOINHERIT
    NOREPLICATION;

ALTER ROLE market_data_dashboard
    WITH LOGIN
    PASSWORD :'dashboard_password'
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    NOINHERIT
    NOREPLICATION;

ALTER ROLE market_data_dashboard
    SET default_transaction_read_only = on;

REVOKE CONNECT ON DATABASE :"market_data_db" FROM PUBLIC;
GRANT CONNECT ON DATABASE :"market_data_db"
    TO :"flyway_user", market_data_airflow, market_data_dashboard;

REVOKE CREATE ON SCHEMA public FROM PUBLIC;
ALTER SCHEMA public OWNER TO :"flyway_user";
EOSQL
