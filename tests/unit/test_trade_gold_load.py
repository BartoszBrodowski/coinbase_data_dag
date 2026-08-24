import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch


DAGS_DIRECTORY = Path(__file__).parents[2] / "dags"
sys.path.insert(0, str(DAGS_DIRECTORY))

from trades.gold_load import load_trade_activity_hourly, upsert_trade_activity_hourly


class TradeGoldLoadTest(unittest.TestCase):
    def test_expands_a_partial_batch_to_complete_utc_hours(self):
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        start = datetime(2026, 8, 24, 10, 15, tzinfo=timezone.utc)
        end = datetime(2026, 8, 24, 11, 20, tzinfo=timezone.utc)

        with patch(
            "trades.gold_load.upsert_trade_activity_hourly",
            return_value=2,
        ) as upsert:
            rows_written = load_trade_activity_hourly(
                connection,
                source="coinbase",
                product_id="BTC-USD",
                start=start,
                end=end,
            )

        self.assertEqual(rows_written, 2)
        upsert.assert_called_once_with(
            cursor,
            source="coinbase",
            product_id="BTC-USD",
            start=datetime(2026, 8, 24, 10, tzinfo=timezone.utc),
            end=datetime(2026, 8, 24, 12, tzinfo=timezone.utc),
        )

    def test_upserts_hourly_trade_activity_for_a_bounded_range(self):
        cursor = Mock()
        cursor.rowcount = 2
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        end = datetime(2026, 1, 2, tzinfo=timezone.utc)

        rows_written = upsert_trade_activity_hourly(
            cursor,
            source="coinbase",
            product_id="BTC-USD",
            start=start,
            end=end,
        )

        self.assertEqual(rows_written, 2)
        sql, parameters = cursor.execute.call_args.args
        self.assertIn("INSERT INTO gold.trade_activity_hourly", sql)
        self.assertIn("SUM(trades.price * trades.size)", sql)
        self.assertIn("ON CONFLICT", sql)
        self.assertEqual(
            parameters,
            {
                "source": "coinbase",
                "product_id": "BTC-USD",
                "start": start,
                "end": end,
            },
        )


if __name__ == "__main__":
    unittest.main()
