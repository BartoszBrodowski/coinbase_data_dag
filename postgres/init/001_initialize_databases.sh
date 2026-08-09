#!/usr/bin/env bash
set -eu

: "${AIRFLOW_DB_USER:?AIRFLOW_DB_USER must be set}"
: "${AIRFLOW_DB_PASSWORD:?AIRFLOW_DB_PASSWORD must be set}"
: "${AIRFLOW_DB_NAME:?AIRFLOW_DB_NAME must be set}"
: "${FLYWAY_USER:?FLYWAY_USER must be set}"
: "${FLYWAY_PASSWORD:?FLYWAY_PASSWORD must be set}"
: "${MARKET_DATA_DB:?MARKET_DATA_DB must be set}"

psql \
    --username "$POSTGRES_USER" \
    --dbname postgres \
    --set=ON_ERROR_STOP=1 \
    --set=airflow_user="$AIRFLOW_DB_USER" \
    --set=airflow_password="$AIRFLOW_DB_PASSWORD" \
    --set=airflow_db="$AIRFLOW_DB_NAME" \
    --set=flyway_user="$FLYWAY_USER" \
    --set=flyway_password="$FLYWAY_PASSWORD" \
    --set=market_data_db="$MARKET_DATA_DB" <<'EOSQL'
CREATE ROLE :"airflow_user"
    LOGIN
    PASSWORD :'airflow_password';

CREATE DATABASE :"airflow_db" OWNER :"airflow_user";

CREATE ROLE :"flyway_user"
    LOGIN
    PASSWORD :'flyway_password';

CREATE DATABASE :"market_data_db" OWNER :"flyway_user";

REVOKE CONNECT ON DATABASE :"airflow_db" FROM PUBLIC;
GRANT CONNECT ON DATABASE :"airflow_db" TO :"airflow_user";

REVOKE CONNECT ON DATABASE :"market_data_db" FROM PUBLIC;
GRANT CONNECT ON DATABASE :"market_data_db" TO :"flyway_user";
EOSQL

psql \
    --username "$POSTGRES_USER" \
    --dbname "$AIRFLOW_DB_NAME" \
    --set=ON_ERROR_STOP=1 \
    --set=airflow_user="$AIRFLOW_DB_USER" <<'EOSQL'
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
ALTER SCHEMA public OWNER TO :"airflow_user";
EOSQL

psql \
    --username "$POSTGRES_USER" \
    --dbname "$MARKET_DATA_DB" \
    --set=ON_ERROR_STOP=1 \
    --set=flyway_user="$FLYWAY_USER" <<'EOSQL'
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
ALTER SCHEMA public OWNER TO :"flyway_user";
EOSQL
