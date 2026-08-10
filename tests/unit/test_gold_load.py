import sys
import unittest
from pathlib import Path
from unittest.mock import Mock

DAGS_DIRECTORY = Path(__file__).parents[2] / "dags"
sys.path.insert(0, str(DAGS_DIRECTORY))

from candles.gold_load import check_gold_quality, upsert_gold


class GoldLoadTest(unittest.TestCase):
    def test_upserts_silver_rows_for_one_ingestion(self):
        cursor = Mock()
        cursor.rowcount = 6

        rows_processed = upsert_gold(cursor, ingestion_id=42)

        self.assertEqual(rows_processed, 6)
        sql, parameters = cursor.execute.call_args.args
        self.assertIn("INSERT INTO gold.candles", sql)
        self.assertIn("ON CONFLICT", sql)
        self.assertEqual(parameters, {"ingestion_id": 42})

    def test_quality_check_returns_matching_record_count(self):
        cursor = Mock()
        cursor.fetchone.return_value = (6, 6, 0, 0, 0, 0)

        self.assertEqual(check_gold_quality(cursor), 6)

    def test_quality_check_rejects_count_mismatch(self):
        cursor = Mock()
        cursor.fetchone.return_value = (6, 5, 0, 0, 0, 0)

        with self.assertRaisesRegex(ValueError, "Gold contains 5 rows"):
            check_gold_quality(cursor)

    def test_quality_check_rejects_duplicate_keys(self):
        cursor = Mock()
        cursor.fetchone.return_value = (6, 6, 1, 0, 0, 0)

        with self.assertRaisesRegex(ValueError, "duplicate candle keys"):
            check_gold_quality(cursor)

    def test_quality_check_rejects_null_values(self):
        cursor = Mock()
        cursor.fetchone.return_value = (6, 6, 0, 2, 0, 0)

        with self.assertRaisesRegex(ValueError, "2 rows with null values"):
            check_gold_quality(cursor)

    def test_quality_check_rejects_invalid_ohlc_values(self):
        cursor = Mock()
        cursor.fetchone.return_value = (6, 6, 0, 0, 3, 0)

        with self.assertRaisesRegex(ValueError, "3 rows with invalid OHLC values"):
            check_gold_quality(cursor)

    def test_quality_check_rejects_time_gaps(self):
        cursor = Mock()
        cursor.fetchone.return_value = (6, 6, 0, 0, 0, 1)

        with self.assertRaisesRegex(ValueError, "1 time gaps between candles"):
            check_gold_quality(cursor)
