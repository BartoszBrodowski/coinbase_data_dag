# Database access roles

The project deliberately uses separate PostgreSQL identities:

| Identity | Purpose | Effective market-data access |
|---|---|---|
| `postgres` | Local bootstrap administrator | Creates databases and roles; not used by Airflow |
| `market_data_migrator` | Flyway migrations | Owns schemas and objects; performs DDL |
| `market_data_airflow` | DAG runtime | Bronze read/insert; Silver read/insert/update; Gold read/insert |
| `airflow` | Airflow metadata | Owns and connects only to the `airflow` database |

`gold.candles` is currently a view over `silver.candles`, so it updates
automatically. The runtime role already has `INSERT` permission for a future
physical Gold table populated by a DAG, but it is not an owner and cannot alter
the Gold schema or its objects.

## Recreate or update the setup

1. Copy `TODO/.env.example` to `TODO/.env` when setting up a new checkout.
2. Replace every `change-me...` value. Use a password made only of URL-safe
   characters for `AIRFLOW_MARKET_DATA_PASSWORD`, because Compose places it in
   the Airflow connection URI.
3. Start PostgreSQL and Airflow as usual.
4. Provision the runtime role and apply migrations:

   ```powershell
   docker-compose --env-file TODO/.env -f TODO/docker-compose.yaml --profile migration run --rm flyway migrate
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
delete or truncate pipeline data, or write through the Gold view. Future objects
created by Flyway inherit the same layer-specific grants automatically.
