import json
import sys
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path


DAGS_DIRECTORY = Path(__file__).parents[2] / "dags"
sys.path.insert(0, str(DAGS_DIRECTORY))

from trades.silver_transform import (
    parse_trades,
    parse_trades_with_stats,
    transform_bronze_to_silver,
)


class _Cursor:
    def __init__(self, bronze):
        self.bronze = bronze
        self.executions = []
        self.batch = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return None

    def execute(self, sql, parameters):
        self.executions.append((sql, parameters))

    def executemany(self, sql, rows):
        self.batch = (sql, rows)

    def fetchone(self):
        return self.bronze


class _Connection:
    def __init__(self, bronze):
        self.cursor_instance = _Cursor(bronze)
        self.committed = False

    def __enter__(self):
        return self

    def __exit__(self, exception_type, *_):
        self.committed = exception_type is None

    def cursor(self):
        return self.cursor_instance


class TradeSilverTransformTest(unittest.TestCase):
    def test_parses_fixture_into_typed_utc_trades(self):
        fixture = json.dumps(
            [
                {
                    "trade_id": 1068720622,
                    "side": "sell",
                    "size": "0.00077203",
                    "price": "65139.75000000",
                    "time": "2026-08-10T09:22:43.388127Z",
                },
                {
                    "trade_id": 1068720621,
                    "side": "buy",
                    "size": "0.00015351",
                    "price": "65139.74000000",
                    "time": "2026-08-10T09:22:42.862482Z",
                },
            ]
        )

        trades = parse_trades(fixture)

        self.assertEqual([trade.trade_id for trade in trades], [1068720621, 1068720622])
        self.assertEqual(trades[0].price, Decimal("65139.74000000"))
        self.assertEqual(trades[0].time.tzinfo, timezone.utc)

    def test_rejects_an_invalid_maker_side(self):
        payload = json.dumps(
            [
                {
                    "trade_id": 1,
                    "price": "1",
                    "size": "1",
                    "time": "2026-08-10T09:22:43Z",
                    "side": "hold",
                }
            ]
        )

        with self.assertRaisesRegex(ValueError, "invalid maker side"):
            parse_trades(payload)

    def test_rejects_a_non_string_maker_side(self):
        payload = json.dumps(
            [
                {
                    "trade_id": 1,
                    "price": "1",
                    "size": "1",
                    "time": "2026-08-10T09:22:43Z",
                    "side": [],
                }
            ]
        )

        with self.assertRaisesRegex(ValueError, "invalid maker side"):
            parse_trades(payload)

    def test_parse_stats_count_fetched_accepted_and_duplicates(self):
        payload = json.dumps(
            [
                {
                    "trade_id": 1,
                    "price": "1",
                    "size": "1",
                    "time": "2026-08-10T09:22:43Z",
                    "side": "buy",
                },
                {
                    "trade_id": 1,
                    "price": "1",
                    "size": "1",
                    "time": "2026-08-10T09:22:43Z",
                    "side": "buy",
                },
            ]
        )

        result = parse_trades_with_stats(payload)

        self.assertEqual(result.records_fetched, 2)
        self.assertEqual(result.records_accepted, 2)
        self.assertEqual(result.duplicate_records, 1)

    def test_commits_silver_before_advancing_the_watermark(self):
        bronze = (
            91,
            "live:coinbase:BTC-USD",
            "test-exchange",
            "BTC-USD",
            "live",
            None,
            "1068720622",
            "1068720621",
            "manual__2026-08-10",
            200,
            '[{"trade_id": 1068720622, "side": "sell", "size": "0.1", '
            '"price": "65139.75", "time": "2026-08-10T09:22:43Z"}]',
        )
        connection = _Connection(bronze)

        with self.assertLogs("trades.silver_transform", level="INFO") as logs:
            rows_processed = transform_bronze_to_silver(connection, 91)

        self.assertEqual(
            rows_processed,
            {
                "trades_processed": 1,
                "completed": False,
                "oldest_trade_time": datetime(2026, 8, 10, 9, 22, 43, tzinfo=timezone.utc),
                "newest_trade_time": datetime(2026, 8, 10, 9, 22, 43, tzinfo=timezone.utc),
            },
        )
        self.assertTrue(
            any(
                "stage=silver" in message
                and "records_fetched=1" in message
                and "records_accepted=1" in message
                and "records_written=1" in message
                for message in logs.output
            )
        )
        self.assertTrue(connection.committed)
        self.assertEqual(connection.cursor_instance.batch[1][0][2], 1068720622)
        self.assertEqual(connection.cursor_instance.batch[1][0][0], "test-exchange")
        self.assertEqual(connection.cursor_instance.batch[1][0][7], 91)
        executed_sql = [sql for sql, _ in connection.cursor_instance.executions]
        self.assertIn("UPDATE bronze.coinbase_trades_raw", executed_sql[1])
        self.assertIn("UPDATE control.trade_ingestions", executed_sql[2])
        update_parameters = connection.cursor_instance.executions[2][1]
        self.assertEqual(update_parameters["response_cb_before"], "1068720622")
        self.assertEqual(update_parameters["response_cb_after"], "1068720621")

    def test_catchup_filters_at_time_boundary_without_moving_live_watermark(self):
        bronze = (
            91,
            "catchup:catchup-1",
            "test-exchange",
            "BTC-USD",
            "catchup",
            datetime(2026, 8, 10, tzinfo=timezone.utc),
            "1068720622",
            "1068720621",
            "manual__2026-08-11",
            200,
            json.dumps(
                [
                    {
                        "trade_id": 2,
                        "side": "sell",
                        "size": "0.1",
                        "price": "2",
                        "time": "2026-08-10T00:00:01Z",
                    },
                    {
                        "trade_id": 1,
                        "side": "buy",
                        "size": "0.1",
                        "price": "1",
                        "time": "2026-08-09T23:59:59Z",
                    },
                ]
            ),
        )
        connection = _Connection(bronze)

        result = transform_bronze_to_silver(connection, 91)

        boundary_time = datetime(2026, 8, 10, 0, 0, 1, tzinfo=timezone.utc)
        self.assertEqual(
            result,
            {
                "trades_processed": 1,
                "completed": True,
                "oldest_trade_time": boundary_time,
                "newest_trade_time": boundary_time,
            },
        )
        self.assertEqual(len(connection.cursor_instance.batch[1]), 1)
        executed_sql = [sql for sql, _ in connection.cursor_instance.executions]
        self.assertIn("UPDATE control.trade_ingestions", executed_sql[2])
