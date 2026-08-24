import json
import sys
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

DAGS_DIRECTORY = Path(__file__).parents[2] / "dags"
sys.path.insert(0, str(DAGS_DIRECTORY))

from candles.silver_transform import (
    calculate_candle_coverage,
    parse_candles,
    parse_candles_with_stats,
    transform_bronze_to_silver,
    upsert_candles,
)


VALID_CANDLE_RESPONSE = json.dumps(
    [
        [
            1735689600,
            92743.63,
            94960.91,
            93347.59,
            94383.59,
            6871.73848196,
        ]
    ]
)


class SilverTransformTest(unittest.TestCase):
    def setUp(self):
        self.response_body = VALID_CANDLE_RESPONSE
        self.start = datetime(2025, 1, 1, tzinfo=timezone.utc)
        self.end = self.start + timedelta(hours=1)

    def parse(self, response_body=None):
        return parse_candles(
            response_body or self.response_body,
            source="test-exchange",
            product_id="BTC-USD",
            granularity_seconds=3600,
            requested_start=self.start,
            requested_end=self.end,
            bronze_ingestion_id=42,
        )

    def test_parses_valid_response_into_typed_utc_candle(self):
        candles = self.parse()

        self.assertEqual(len(candles), 1)
        candle = candles[0]
        self.assertEqual(candle.source, "test-exchange")
        self.assertEqual(candle.candle_start, self.start)
        self.assertEqual(candle.candle_start.tzinfo, timezone.utc)
        self.assertEqual(candle.open_price, Decimal("93347.59"))
        self.assertEqual(candle.high_price, Decimal("94960.91"))
        self.assertEqual(candle.low_price, Decimal("92743.63"))
        self.assertEqual(candle.close_price, Decimal("94383.59"))
        self.assertEqual(candle.volume, Decimal("6871.73848196"))

    def test_excludes_candle_at_requested_end(self):
        fixture = json.loads(self.response_body)
        candle_at_end = fixture[0].copy()
        candle_at_end[0] = int(self.end.timestamp())

        candles = self.parse(json.dumps([fixture[0], candle_at_end]))

        self.assertEqual(
            [candle.candle_start for candle in candles],
            [self.start],
        )

    def test_removes_identical_duplicate_candles(self):
        fixture = json.loads(self.response_body)
        candles = self.parse(json.dumps([fixture[0], fixture[0]]))

        self.assertEqual(len(candles), 1)

    def test_parse_stats_distinguish_duplicates_and_out_of_range_records(self):
        fixture = json.loads(self.response_body)
        preceding = fixture[0].copy()
        preceding[0] = int((self.start - timedelta(hours=1)).timestamp())

        result = parse_candles_with_stats(
            json.dumps([preceding, fixture[0], fixture[0]]),
            source="test-exchange",
            product_id="BTC-USD",
            granularity_seconds=3600,
            requested_start=self.start,
            requested_end=self.end,
            bronze_ingestion_id=42,
        )

        self.assertEqual(result.records_fetched, 3)
        self.assertEqual(result.records_accepted, 2)
        self.assertEqual(result.duplicate_records, 1)
        self.assertEqual(result.records_outside_interval, 1)

    def test_candle_coverage_reports_missing_buckets_without_failing(self):
        coverage = calculate_candle_coverage(
            self.parse(),
            requested_start=self.start,
            requested_end=self.end + timedelta(hours=1),
            granularity_seconds=3600,
        )

        self.assertEqual(coverage.expected_buckets, 2)
        self.assertEqual(coverage.actual_buckets, 1)
        self.assertEqual(coverage.missing_buckets, 1)

    def test_transform_logs_partial_coverage_without_failing(self):
        cursor = MagicMock()
        cursor.fetchone.return_value = (
            "BTC-USD",
            3600,
            self.start,
            self.end + timedelta(hours=1),
            self.response_body,
        )
        connection = MagicMock()
        connection.__enter__.return_value = connection
        connection.cursor.return_value.__enter__.return_value = cursor

        with patch("candles.silver_transform.market_data_connection", return_value=connection):
            with self.assertLogs("candles.silver_transform", level="INFO") as logs:
                rows_processed = transform_bronze_to_silver(42, source="test-exchange")

        self.assertEqual(rows_processed, 1)
        self.assertTrue(
            any(
                "stage=silver" in message
                and "missing_buckets=1" in message
                and "coverage_status=partial" in message
                for message in logs.output
            )
        )
        self.assertTrue(
            any("coinbase_candle_coverage_partial" in message for message in logs.output)
        )

    def test_rejects_conflicting_duplicate_candles(self):
        fixture = json.loads(self.response_body)
        conflicting = fixture[0].copy()
        conflicting[4] = 94000

        with self.assertRaisesRegex(ValueError, "conflicting candles"):
            self.parse(json.dumps([fixture[0], conflicting]))

    def test_rejects_invalid_ohlc_values(self):
        fixture = json.loads(self.response_body)
        fixture[0][3] = fixture[0][2] + 1

        with self.assertRaisesRegex(ValueError, "invalid OHLC"):
            self.parse(json.dumps(fixture))

    def test_rejects_candle_with_missing_field(self):
        fixture = json.loads(self.response_body)
        fixture[0].pop()

        with self.assertRaisesRegex(ValueError, "exactly 6 fields"):
            self.parse(json.dumps(fixture))

    def test_rejects_candle_with_unaligned_timestamp(self):
        fixture = json.loads(self.response_body)
        fixture[0][0] += 1

        with self.assertRaisesRegex(ValueError, "not aligned"):
            self.parse(json.dumps(fixture))

    def test_rejects_candle_with_noninteger_timestamp(self):
        fixture = json.loads(self.response_body)
        fixture[0][0] = "invalid"

        with self.assertRaisesRegex(ValueError, "timestamp must be an integer"):
            self.parse(json.dumps(fixture))

    def test_rejects_candle_with_invalid_numeric_value(self):
        numeric_fields = {
            1: "low",
            2: "high",
            3: "open",
            4: "close",
            5: "volume",
        }

        for position, field in numeric_fields.items():
            with self.subTest(field=field):
                fixture = json.loads(self.response_body)
                fixture[0][position] = "not-a-number"

                with self.assertRaisesRegex(ValueError, f"invalid {field} value"):
                    self.parse(json.dumps(fixture))

    def test_rejects_boolean_numeric_value(self):
        fixture = json.loads(self.response_body)
        fixture[0][1] = True

        with self.assertRaisesRegex(ValueError, "invalid low value"):
            self.parse(json.dumps(fixture))

    def test_batch_upsert_uses_all_candles(self):
        candles = self.parse()
        cursor = Mock()

        rows_processed = upsert_candles(cursor, candles)

        self.assertEqual(rows_processed, 1)
        cursor.executemany.assert_called_once()
        self.assertIn("ON CONFLICT", cursor.executemany.call_args.args[0])
        self.assertEqual(len(cursor.executemany.call_args.args[1]), 1)
        self.assertEqual(cursor.executemany.call_args.args[1][0][0], "test-exchange")
