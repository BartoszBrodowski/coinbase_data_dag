# Market-data dashboard

The dashboard is a Streamlit service that reads completed candles directly
from `gold.candles` and displays them as a Plotly candlestick chart. It does not
call Coinbase and does not connect to Airflow.

The UI provides a market selector and a UTC date range. It displays only daily
(86,400-second) candles. Queries are
parameterized, ordered by candle time, and limited to the most recent 1,000
matching candles. Empty Gold data and empty selected ranges display a readable
message instead of an exception.

## Database access

The dashboard connects as `market_data_dashboard`. Migration V008 grants this
role only `USAGE` on the Gold schema and `SELECT` on `gold.candles`. It has no
Bronze or Silver access and its PostgreSQL transactions default to read-only.

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
