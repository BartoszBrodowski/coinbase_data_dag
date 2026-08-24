import json
import os
import sys
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import psycopg2
from psycopg2 import sql


REPOSITORY_ROOT = Path(__file__).parents[2]
DAGS_DIRECTORY = REPOSITORY_ROOT / "dags"
sys.path.insert(0, str(DAGS_DIRECTORY))

from candles.bronze_ingest import save_bronze_response
from candles.gold_load import upsert_gold
from candles.silver_transform import parse_candles, upsert_candles
from shared.coinbase_client import COINBASE_SOURCE, CoinbaseResponse
from trades.gold_load import load_trade_activity_hourly
from trades.gap_detection import find_newest_trade_gap


def _database_settings(database_name):
    return {
        "host": os.environ.get("TEST_POSTGRES_HOST", "postgres"),
        "port": int(os.environ.get("TEST_POSTGRES_PORT", "5432")),
        "user": os.environ.get("TEST_POSTGRES_USER", "postgres"),
        "password": os.environ["TEST_POSTGRES_PASSWORD"],
        "dbname": database_name,
        "connect_timeout": 10,
    }


@unittest.skipUnless(
    os.environ.get("RUN_INTEGRATION_TESTS") == "1",
    "set RUN_INTEGRATION_TESTS=1 to run PostgreSQL integration tests",
)
class PostgresPipelineIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.database_name = os.environ.get(
            "TEST_DATABASE_NAME",
            "market_data_integration_test",
        )
        if not cls.database_name.endswith("_test"):
            raise RuntimeError("integration database name must end with '_test'")

        maintenance_database = os.environ.get(
            "TEST_POSTGRES_MAINTENANCE_DB",
            "postgres",
        )
        cls._ensure_test_database(maintenance_database)
        cls.connection = psycopg2.connect(**_database_settings(cls.database_name))
        cls._reset_schemas()
        cls._ensure_runtime_roles(maintenance_database)
        cls._apply_migrations()

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "connection"):
            cls.connection.close()

    @classmethod
    def _ensure_test_database(cls, maintenance_database):
        connection = psycopg2.connect(**_database_settings(maintenance_database))
        connection.autocommit = True
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT 1 FROM pg_database WHERE datname = %s",
                    (cls.database_name,),
                )
                if cursor.fetchone() is None:
                    cursor.execute(
                        sql.SQL("CREATE DATABASE {}").format(
                            sql.Identifier(cls.database_name)
                        )
                    )
        finally:
            connection.close()

    @classmethod
    def _reset_schemas(cls):
        with cls.connection:
            with cls.connection.cursor() as cursor:
                cursor.execute(
                    "DROP SCHEMA IF EXISTS control, gold, silver, bronze CASCADE"
                )

    @classmethod
    def _ensure_runtime_roles(cls, maintenance_database):
        connection = psycopg2.connect(**_database_settings(maintenance_database))
        connection.autocommit = True
        try:
            with connection.cursor() as cursor:
                for role_name in (
                    "market_data_airflow",
                    "market_data_dashboard",
                ):
                    cursor.execute(
                        "SELECT 1 FROM pg_roles WHERE rolname = %s",
                        (role_name,),
                    )
                    if cursor.fetchone() is None:
                        cursor.execute(
                            sql.SQL("CREATE ROLE {} NOLOGIN").format(
                                sql.Identifier(role_name)
                            )
                        )
        finally:
            connection.close()

    @classmethod
    def _apply_migrations(cls):
        migration_directory = REPOSITORY_ROOT / "sql" / "migrations"
        migrations = sorted(migration_directory.glob("V*.sql"))
        if not migrations:
            raise RuntimeError("no SQL migrations were found")

        for migration in migrations:
            with cls.connection:
                with cls.connection.cursor() as cursor:
                    cursor.execute(migration.read_text(encoding="utf-8"))

    def test_migration_chain_reaches_the_current_state(self):
        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'bronze'
                      AND table_name = 'coinbase_trades_raw'
                    """
                )
                bronze_columns = {row[0] for row in cursor.fetchall()}

                cursor.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'control'
                      AND table_name = 'trade_ingestions'
                    """
                )
                process_columns = {row[0] for row in cursor.fetchall()}

                cursor.execute(
                    """
                    SELECT to_regclass('gold.trade_activity_hourly')
                    """
                )
                trade_activity_table = cursor.fetchone()[0]

                cursor.execute(
                    """
                    SELECT to_regclass('silver.trades_coverage_lookup_idx')
                    """
                )
                trade_coverage_index = cursor.fetchone()[0]

        self.assertTrue(
            {
                "bronze_request_key",
                "process_key",
                "requested_before",
                "requested_after",
            }.issubset(bronze_columns)
        )
        self.assertNotIn("ingestion_key", bronze_columns)
        self.assertNotIn("trade_ingestion_key", bronze_columns)
        self.assertTrue(
            {
                "process_key",
                "initial_before_cursor",
                "resume_before_cursor",
                "initial_after_cursor",
                "resume_after_cursor",
            }.issubset(process_columns)
        )
        self.assertNotIn("ingestion_key", process_columns)
        self.assertEqual(trade_activity_table, "gold.trade_activity_hourly")
        self.assertEqual(
            trade_coverage_index,
            "silver.trades_coverage_lookup_idx",
        )

    def test_trade_gap_detection_returns_an_actionable_cursor(self):
        older_time = datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)
        newer_time = older_time + timedelta(minutes=10)
        process_key = "integration:gap-detection"

        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO control.trade_ingestions (
                        process_key, source, product_id, mode,
                        initial_after_cursor, resume_after_cursor, status
                    )
                    VALUES (%s, 'coinbase', 'BTC-USD', 'catchup', %s, %s, 'COMPLETED')
                    """,
                    (process_key, "9000", "7000"),
                )
                cursor.execute(
                    """
                    INSERT INTO bronze.coinbase_trades_raw (
                        bronze_request_key, process_key, product_id,
                        requested_after, response_cb_after, http_status,
                        response_body, silver_committed_at
                    )
                    VALUES (%s, %s, 'BTC-USD', %s, %s, 200, '[]', CURRENT_TIMESTAMP)
                    RETURNING ingestion_id
                    """,
                    (
                        "integration:gap-detection:page",
                        process_key,
                        "9000",
                        "8000",
                    ),
                )
                bronze_ingestion_id = cursor.fetchone()[0]
                cursor.executemany(
                    """
                    INSERT INTO silver.trades (
                        source, product_id, trade_id, price, time, size, side,
                        bronze_ingestion_id
                    )
                    VALUES ('coinbase', 'BTC-USD', %s, 100, %s, 1, 'buy', %s)
                    """,
                    (
                        (5000, older_time, bronze_ingestion_id),
                        (8000, newer_time, bronze_ingestion_id),
                    ),
                )

        gap = find_newest_trade_gap(
            self.connection,
            source="coinbase",
            product_id="BTC-USD",
            lookback_start=older_time - timedelta(hours=1),
            eligible_before=newer_time + timedelta(hours=1),
            minimum_gap_seconds=30,
            maximum_page_amount=1000,
        )

        self.assertEqual(gap["older_trade_time"], older_time.isoformat())
        self.assertEqual(gap["newer_trade_time"], newer_time.isoformat())
        self.assertEqual(gap["after_cursor"], "8000")
        self.assertEqual(gap["estimated_page_amount"], 4)
        self.assertEqual(gap["page_amount"], 4)

    def test_trade_activity_hourly_is_idempotent(self):
        first_time = datetime(2026, 2, 1, 10, 15, tzinfo=timezone.utc)
        second_time = datetime(2026, 2, 1, 10, 45, tzinfo=timezone.utc)
        outside_time = datetime(2026, 2, 1, 11, 15, tzinfo=timezone.utc)

        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO silver.trades (
                        source, product_id, trade_id, price, time, size, side,
                        bronze_ingestion_id
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    [
                        (
                            "coinbase",
                            "BTC-USD",
                            2001,
                            Decimal("100"),
                            first_time,
                            Decimal("2"),
                            "buy",
                            1,
                        ),
                        (
                            "coinbase",
                            "BTC-USD",
                            2002,
                            Decimal("110"),
                            second_time,
                            Decimal("1"),
                            "sell",
                            2,
                        ),
                        (
                            "coinbase",
                            "BTC-USD",
                            2003,
                            Decimal("120"),
                            outside_time,
                            Decimal("1"),
                            "buy",
                            3,
                        ),
                    ],
                )

        rows_written = load_trade_activity_hourly(
            self.connection,
            source="coinbase",
            product_id="BTC-USD",
            start=first_time,
            end=first_time + timedelta(microseconds=1),
        )
        repeated_rows_written = load_trade_activity_hourly(
            self.connection,
            source="coinbase",
            product_id="BTC-USD",
            start=second_time,
            end=second_time + timedelta(microseconds=1),
        )

        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        trade_count,
                        base_volume,
                        quote_volume,
                        vwap,
                        maker_buy_count,
                        maker_sell_count
                    FROM gold.trade_activity_hourly
                    WHERE source = 'coinbase'
                      AND product_id = 'BTC-USD'
                      AND hour_start = '2026-02-01T10:00:00Z'
                    """
                )
                aggregate = cursor.fetchone()

        self.assertEqual(rows_written, 1)
        self.assertEqual(repeated_rows_written, 1)
        self.assertEqual(aggregate[:3], (2, Decimal("3"), Decimal("310")))
        self.assertAlmostEqual(float(aggregate[3]), 103.33333333333333)
        self.assertEqual(aggregate[4:], (1, 1))

    def test_candle_pipeline_is_idempotent(self):
        interval_start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        interval_end = interval_start + timedelta(days=1)
        candle_timestamp = int(interval_start.timestamp())
        response = CoinbaseResponse(
            status_code=200,
            body=json.dumps(
                [[candle_timestamp, "90", "110", "100", "105", "1.5"]]
            ),
            content_type="application/json",
            headers={},
            duration_ms=12,
        )

        first_ingestion_id = save_bronze_response(
            self.connection,
            bronze_request_key="integration:candles:1",
            product_id="BTC-USD",
            granularity_seconds=86400,
            start=interval_start,
            end=interval_end,
            response=response,
        )
        repeated_ingestion_id = save_bronze_response(
            self.connection,
            bronze_request_key="integration:candles:1",
            product_id="BTC-USD",
            granularity_seconds=86400,
            start=interval_start,
            end=interval_end,
            response=response,
        )
        self.assertEqual(first_ingestion_id, repeated_ingestion_id)

        candles = parse_candles(
            response.body,
            source=COINBASE_SOURCE,
            product_id="BTC-USD",
            granularity_seconds=86400,
            requested_start=interval_start,
            requested_end=interval_end,
            bronze_ingestion_id=first_ingestion_id,
        )

        with self.connection:
            with self.connection.cursor() as cursor:
                upsert_candles(cursor, candles)
                upsert_candles(cursor, candles)
                upsert_gold(cursor, first_ingestion_id)
                upsert_gold(cursor, first_ingestion_id)

        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM bronze.coinbase_candles_raw")
                bronze_count = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(*) FROM silver.candles")
                silver_count = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(*) FROM gold.candles")
                gold_count = cursor.fetchone()[0]

        self.assertEqual((bronze_count, silver_count, gold_count), (1, 1, 1))

    def test_database_roles_keep_their_permission_boundaries(self):
        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        has_table_privilege(
                            'market_data_dashboard', 'gold.candles', 'SELECT'
                        ),
                        has_table_privilege(
                            'market_data_dashboard', 'gold.candles', 'INSERT'
                        ),
                        has_table_privilege(
                            'market_data_dashboard',
                            'bronze.coinbase_candles_raw',
                            'SELECT'
                        ),
                        has_table_privilege(
                            'market_data_airflow',
                            'bronze.coinbase_candles_raw',
                            'INSERT'
                        ),
                        has_table_privilege(
                            'market_data_airflow', 'silver.candles', 'UPDATE'
                        ),
                        has_table_privilege(
                            'market_data_airflow', 'gold.candles', 'UPDATE'
                        ),
                        has_table_privilege(
                            'market_data_dashboard',
                            'gold.trade_activity_hourly',
                            'SELECT'
                        ),
                        has_table_privilege(
                            'market_data_dashboard',
                            'gold.trade_activity_hourly',
                            'INSERT'
                        )
                    """
                )
                privileges = cursor.fetchone()

        self.assertEqual(
            privileges,
            (True, False, False, True, True, True, True, False),
        )


if __name__ == "__main__":
    unittest.main()
