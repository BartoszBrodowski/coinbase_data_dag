# Database access roles

The project deliberately uses separate PostgreSQL identities:

| Identity | Purpose | Effective market-data access |
|---|---|---|
| `postgres` | Local bootstrap administrator | Creates databases and roles; not used by Airflow |
| `market_data_migrator` | Flyway migrations | Owns schemas and objects; performs DDL |
| `market_data_airflow` | DAG runtime | Bronze read/insert; Silver and Gold read/insert/update |
| `airflow` | Airflow metadata | Owns and connects only to the `airflow` database |

`gold.candles` and `gold.trade_activity_hourly` are physical read tables
populated from Silver by Airflow tasks. The runtime role can upsert their rows,
but it is not an owner and cannot alter the Gold schema or its objects.

## Recreate or update the setup

1. Copy `.env.example` to `.env` from the repository root when setting up a
   new checkout.
2. Replace every `change-me...` value. Use a password made only of URL-safe
   characters for `AIRFLOW_MARKET_DATA_PASSWORD`, because Compose places it in
   the Airflow connection URI.
3. Start PostgreSQL and Airflow as usual.
4. Provision the runtime role and apply migrations:

   ```powershell
   docker-compose --profile migration run --rm flyway migrate
   ```

The `market-data-role-init` service runs before Flyway. It creates the runtime
role when missing and synchronizes its password, so the command works with both
a fresh database volume and volumes created with this separated-role setup.

The change from `POSTGRES_USER=airflow` to `POSTGRES_USER=postgres` requires a
fresh PostgreSQL volume once. A volume initialized by the older configuration
does not contain the new `postgres` bootstrap identity and cannot adopt this
change merely by recreating its container.

DAG code should use the Airflow connection ID `market_data`. It is supplied as
`AIRFLOW_CONN_MARKET_DATA`; do not put database passwords in DAG source files.

## Permission boundary

The runtime role cannot create or alter schemas/tables, run Flyway migrations,
or delete or truncate pipeline data. Future objects created by Flyway inherit
the same layer-specific grants automatically.
