import json
import sys
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import Mock

DAGS_DIRECTORY = Path(__file__).parents[2] / "dags"
sys.path.insert(0, str(DAGS_DIRECTORY))

from silver_transform import parse_candles, upsert_candles


FIXTURE_PATH = Path(__file__).parents[2] / "test_fixture.json"


class SilverTransformTest(unittest.TestCase):
    def setUp(self):
        self.response_body = FIXTURE_PATH.read_text(encoding="utf-8")
        self.start = datetime(2025, 1, 1, tzinfo=timezone.utc)
        self.end = self.start + timedelta(hours=1)

    def parse(self, response_body=None):
        return parse_candles(
            response_body or self.response_body,
            product_id="BTC-USD",
            granularity_seconds=3600,
            requested_start=self.start,
            requested_end=self.end,
            bronze_ingestion_id=42,
        )

    def test_parses_fixture_into_typed_utc_candle(self):
        candles = self.parse()

        self.assertEqual(len(candles), 1)
        candle = candles[0]
        self.assertEqual(candle.candle_start, self.start)
        self.assertEqual(candle.candle_start.tzinfo, timezone.utc)
        self.assertEqual(candle.open_price, Decimal("93347.59"))
        self.assertEqual(candle.high_price, Decimal("94960.91"))
        self.assertEqual(candle.low_price, Decimal("92743.63"))
        self.assertEqual(candle.close_price, Decimal("94383.59"))
        self.assertEqual(candle.volume, Decimal("6871.73848196"))

    def test_removes_identical_duplicate_candles(self):
        fixture = json.loads(self.response_body)
        candles = self.parse(json.dumps([fixture[0], fixture[0]]))

        self.assertEqual(len(candles), 1)

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

    def test_batch_upsert_uses_all_candles(self):
        candles = self.parse()
        cursor = Mock()

        rows_processed = upsert_candles(cursor, candles)

        self.assertEqual(rows_processed, 1)
        cursor.executemany.assert_called_once()
        self.assertIn("ON CONFLICT", cursor.executemany.call_args.args[0])
        self.assertEqual(len(cursor.executemany.call_args.args[1]), 1)
