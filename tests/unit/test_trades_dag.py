import sys
import unittest
from datetime import timedelta
from pathlib import Path
from unittest.mock import call, patch


DAGS_DIRECTORY = Path(__file__).parents[2] / "dags"
sys.path.insert(0, str(DAGS_DIRECTORY))

from trades.settings import TRADES_API_POOL
from trades.trades_dag import _load_silver_pages, build_ingestion_request, dag


class TradesDagTest(unittest.TestCase):
    def test_separates_bronze_and_silver_tasks(self):
        validation_task = dag.get_task("resolve_ingestion_request")
        bronze_task = dag.get_task("ingest_bronze_pages")
        silver_task = dag.get_task("load_silver_pages")

        self.assertEqual(validation_task.downstream_task_ids, {bronze_task.task_id})
        self.assertEqual(bronze_task.downstream_task_ids, {silver_task.task_id})
        self.assertEqual(silver_task.downstream_task_ids, set())

    def test_only_bronze_has_api_retry_policy_and_pool(self):
        bronze_task = dag.get_task("ingest_bronze_pages")
        silver_task = dag.get_task("load_silver_pages")

        self.assertEqual(bronze_task.retries, 2)
        self.assertEqual(bronze_task.retry_delay, timedelta(minutes=1))
        self.assertTrue(bronze_task.retry_exponential_backoff)
        self.assertEqual(bronze_task.max_retry_delay, timedelta(minutes=5))
        self.assertEqual(bronze_task.pool, TRADES_API_POOL)
        self.assertEqual(silver_task.retries, 0)

    def test_silver_processes_returned_bronze_pages_in_order(self):
        connection = object()
        batch = {
            "mode": "live",
            "ingestion_ids": [91, 92],
            "already_completed": False,
        }

        with patch("trades.trades_dag.market_data_connection") as connection_factory:
            connection_factory.return_value.__enter__.return_value = connection
            with patch(
                "trades.trades_dag.transform_bronze_to_silver",
                side_effect=(
                    {"trades_processed": 2, "completed": False},
                    {"trades_processed": 3, "completed": False},
                ),
            ) as transform:
                result = _load_silver_pages(batch)

        self.assertEqual(
            transform.call_args_list,
            [call(connection, 91), call(connection, 92)],
        )
        self.assertEqual(result, {"pages_processed": 2, "trades_processed": 5})

    def test_scheduled_run_resolves_to_live_mode(self):
        request = build_ingestion_request({"mode": "live"}, "scheduled")

        self.assertEqual(request["mode"], "live")
        self.assertEqual(request["max_pages"], 3)

    def test_manual_run_can_resolve_to_bounded_catchup_mode(self):
        request = build_ingestion_request(
            {
                "mode": "catchup",
                "oldest_time": "2026-08-01T00:00:00Z",
                "after_cursor": None,
                "max_pages": 10,
            },
            "manual",
        )

        self.assertEqual(request["mode"], "catchup")
        self.assertEqual(request["max_pages"], 10)

    def test_scheduled_run_cannot_enable_catchup_mode(self):
        with self.assertRaisesRegex(ValueError, "only be triggered manually"):
            build_ingestion_request(
                {
                    "mode": "catchup",
                    "oldest_time": "2026-08-01T00:00:00Z",
                    "after_cursor": None,
                    "max_pages": 3,
                },
                "scheduled",
            )


if __name__ == "__main__":
    unittest.main()
