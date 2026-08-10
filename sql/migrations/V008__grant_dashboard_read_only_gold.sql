REVOKE ALL ON SCHEMA public, bronze, silver, gold
    FROM market_data_dashboard;
REVOKE ALL ON ALL TABLES IN SCHEMA bronze, silver, gold
    FROM market_data_dashboard;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA bronze, silver, gold
    FROM market_data_dashboard;

GRANT USAGE ON SCHEMA gold
    TO market_data_dashboard;
GRANT SELECT ON gold.candles
    TO market_data_dashboard;

ALTER DEFAULT PRIVILEGES IN SCHEMA gold
    GRANT SELECT ON TABLES TO market_data_dashboard;
