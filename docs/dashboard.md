# Market-data dashboard

The dashboard is a Streamlit service that reads completed read models from
Gold. It provides a daily candlestick chart from `gold.candles` and an hourly
trade-activity chart from `gold.trade_activity_hourly`. It does not call
Coinbase and does not connect to Airflow.

Both views provide a market selector and a UTC date range. Queries are
parameterized and limited to the most recent 1,000 matching rows. Empty Gold
data and empty selected ranges display a readable message instead of an
exception.

## Database access

The dashboard connects as `market_data_dashboard`. Migrations grant this role
only `USAGE` on the Gold schema and `SELECT` on `gold.candles` and
`gold.trade_activity_hourly`. It has no Bronze or Silver access and its
PostgreSQL transactions default to read-only.

Set `DASHBOARD_DB_PASSWORD` in `.env`, then provision the role and grants:

```powershell
docker-compose --profile migration run --rm flyway migrate
```

Build and start the dashboard:

```powershell
docker-compose up -d --build dashboard
```

Open `http://localhost:8501`.

The service depends only on PostgreSQL. Stopping the Airflow API server,
scheduler, and DAG processor does not stop the dashboard or remove its ability
to read Gold data.
