import sys
import unittest
from pathlib import Path
from unittest.mock import Mock

DAGS_DIRECTORY = Path(__file__).parents[2] / "dags"
sys.path.insert(0, str(DAGS_DIRECTORY))

from candles.gold_load import upsert_gold


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
