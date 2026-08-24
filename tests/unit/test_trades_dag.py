import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import call, patch

from airflow.utils.types import DagRunType


DAGS_DIRECTORY = Path(__file__).parents[2] / "dags"
sys.path.insert(0, str(DAGS_DIRECTORY))

from trades.settings import (
    CATCHUP_PAGES_PER_BATCH,
    DEFAULT_CATCHUP_PAGE_AMOUNT,
    MAX_LIVE_PAGES_PER_RUN,
    TRADES_API_POOL,
)
from trades.trades_dag import (
    _ingest_bronze_pages,
    _load_silver_pages,
    build_catchup_ingestion_request,
    build_live_ingestion_request,
    catchup_dag,
    live_dag,
)


class TradesDagTest(unittest.TestCase):
    def test_live_dag_allows_only_scheduled_runs(self):
        self.assertEqual(
            {run_type.value for run_type in live_dag.allowed_run_types},
            {DagRunType.SCHEDULED.value},
        )

    def test_catchup_dag_allows_manual_and_operator_continuation_runs(self):
        self.assertEqual(
            {run_type.value for run_type in catchup_dag.allowed_run_types},
            {
                DagRunType.MANUAL.value,
                DagRunType.OPERATOR_TRIGGERED.value,
            },
        )
        self.assertNotIn(
            DagRunType.BACKFILL_JOB.value,
            {run_type.value for run_type in catchup_dag.allowed_run_types},
        )

    def test_live_dag_has_only_the_scheduled_ingestion_path(self):
        validation_task = live_dag.get_task("resolve_live_ingestion_request")
        bronze_task = live_dag.get_task("ingest_bronze_pages")
        silver_task = live_dag.get_task("load_silver_pages")
        gold_task = live_dag.get_task("load_gold_trade_activity")

        self.assertEqual(validation_task.downstream_task_ids, {bronze_task.task_id})
        self.assertEqual(bronze_task.downstream_task_ids, {silver_task.task_id})
        self.assertEqual(silver_task.downstream_task_ids, {gold_task.task_id})
        self.assertEqual(gold_task.downstream_task_ids, set())

    def test_catchup_dag_continues_unfinished_page_requests(self):
        validation_task = catchup_dag.get_task("resolve_catchup_ingestion_request")
        bronze_task = catchup_dag.get_task("ingest_bronze_pages")
        silver_task = catchup_dag.get_task("load_silver_pages")
        gold_task = catchup_dag.get_task("load_gold_trade_activity")
        continuation_task = catchup_dag.get_task("choose_catchup_continuation")
        trigger_task = catchup_dag.get_task("continue_catchup")
        finish_task = catchup_dag.get_task("finish_trade_ingestion")

        self.assertEqual(
            validation_task.downstream_task_ids,
            {
                bronze_task.task_id,
                "build_continuation_conf",
            },
        )
        self.assertEqual(bronze_task.downstream_task_ids, {silver_task.task_id})
        self.assertEqual(
            silver_task.downstream_task_ids,
            {gold_task.task_id, continuation_task.task_id},
        )
        self.assertEqual(
            gold_task.downstream_task_ids,
            {continuation_task.task_id},
        )
        self.assertEqual(
            continuation_task.downstream_task_ids,
            {trigger_task.task_id, finish_task.task_id},
        )

    def test_only_bronze_has_api_retry_policy_and_pool(self):
        bronze_task = live_dag.get_task("ingest_bronze_pages")
        silver_task = live_dag.get_task("load_silver_pages")

        self.assertEqual(bronze_task.retries, 2)
        self.assertEqual(bronze_task.retry_delay, timedelta(minutes=1))
        self.assertTrue(bronze_task.retry_exponential_backoff)
        self.assertEqual(bronze_task.max_retry_delay, timedelta(minutes=5))
        self.assertEqual(bronze_task.pool, TRADES_API_POOL)
        self.assertEqual(silver_task.retries, 0)

    def test_silver_processes_returned_bronze_pages_in_order(self):
        connection = object()
        first_trade_time = datetime(2026, 8, 24, 10, 15, tzinfo=timezone.utc)
        second_trade_time = datetime(2026, 8, 24, 10, 45, tzinfo=timezone.utc)
        batch = {
            "mode": "live",
            "source": "coinbase",
            "product_id": "BTC-USD",
            "ingestion_ids": [91, 92],
            "already_completed": False,
            "pages_committed_before": 0,
            "overlap_reached": False,
        }

        with patch("trades.trades_dag.market_data_connection") as connection_factory:
            connection_factory.return_value.__enter__.return_value = connection
            with patch(
                "trades.trades_dag.transform_bronze_to_silver",
                side_effect=(
                    {
                        "trades_processed": 2,
                        "completed": False,
                        "oldest_trade_time": first_trade_time,
                        "newest_trade_time": first_trade_time,
                    },
                    {
                        "trades_processed": 3,
                        "completed": False,
                        "oldest_trade_time": second_trade_time,
                        "newest_trade_time": second_trade_time,
                    },
                ),
            ) as transform:
                result = _load_silver_pages(batch)

        self.assertEqual(
            transform.call_args_list,
            [call(connection, 91), call(connection, 92)],
        )
        self.assertEqual(
            result,
            {
                "source": "coinbase",
                "product_id": "BTC-USD",
                "pages_processed": 2,
                "trades_processed": 5,
                "activity_start": first_trade_time.isoformat(),
                "activity_end": (
                    second_trade_time + timedelta(microseconds=1)
                ).isoformat(),
                "completed": False,
                "pages_committed": 2,
                "stop_reason": None,
            },
        )

    def test_catchup_completes_when_the_requested_page_amount_is_reached(self):
        connection = object()
        batch = {
            "mode": "catchup",
            "source": "coinbase",
            "product_id": "BTC-USD",
            "process_key": "catchup:one",
            "page_amount": 100,
            "ingestion_ids": [91, 92, 93, 94],
            "already_completed": False,
            "pages_committed_before": 96,
            "overlap_reached": False,
        }
        transform_result = {
            "trades_processed": 1,
            "completed": False,
            "oldest_trade_time": None,
            "newest_trade_time": None,
        }

        with patch("trades.trades_dag.market_data_connection") as connection_factory:
            connection_factory.return_value.__enter__.return_value = connection
            with patch(
                "trades.trades_dag.transform_bronze_to_silver",
                return_value=transform_result,
            ):
                with patch("trades.trades_dag.complete_ingestion") as complete:
                    with patch("trades.trades_dag.pause_ingestion") as pause:
                        result = _load_silver_pages(batch)

        complete.assert_called_once_with(connection, "catchup:one")
        pause.assert_not_called()
        self.assertTrue(result["completed"])
        self.assertEqual(result["pages_committed"], 100)
        self.assertEqual(result["stop_reason"], "page_amount_reached")

    def test_catchup_pauses_after_one_operational_batch(self):
        connection = object()
        batch = {
            "mode": "catchup",
            "source": "coinbase",
            "product_id": "BTC-USD",
            "process_key": "catchup:one",
            "page_amount": 100,
            "ingestion_ids": [91, 92, 93, 94],
            "already_completed": False,
            "pages_committed_before": 20,
            "overlap_reached": False,
        }
        transform_result = {
            "trades_processed": 1,
            "completed": False,
            "oldest_trade_time": None,
            "newest_trade_time": None,
        }

        with patch("trades.trades_dag.market_data_connection") as connection_factory:
            connection_factory.return_value.__enter__.return_value = connection
            with patch(
                "trades.trades_dag.transform_bronze_to_silver",
                return_value=transform_result,
            ):
                with patch("trades.trades_dag.complete_ingestion") as complete:
                    with patch("trades.trades_dag.pause_ingestion") as pause:
                        result = _load_silver_pages(batch)

        pause.assert_called_once_with(connection, "catchup:one")
        complete.assert_not_called()
        self.assertFalse(result["completed"])
        self.assertEqual(result["pages_committed"], 24)
        self.assertEqual(result["stop_reason"], "batch_limit_reached")

    def test_scheduled_run_resolves_to_live_mode(self):
        request = build_live_ingestion_request()

        self.assertEqual(request["mode"], "live")
        self.assertEqual(request["cursor_direction"], "before")
        self.assertEqual(request["max_pages"], MAX_LIVE_PAGES_PER_RUN)

    def test_manual_run_can_resolve_to_bounded_catchup_mode(self):
        request = build_catchup_ingestion_request(
            {
                "after_cursor": None,
                "page_amount": DEFAULT_CATCHUP_PAGE_AMOUNT,
            },
            "manual",
        )

        self.assertEqual(request["mode"], "catchup")
        self.assertEqual(request["cursor_direction"], "after")
        self.assertEqual(request["page_amount"], DEFAULT_CATCHUP_PAGE_AMOUNT)

    def test_operator_run_can_continue_bounded_catchup_mode(self):
        request = build_catchup_ingestion_request(
            {
                "after_cursor": None,
                "page_amount": DEFAULT_CATCHUP_PAGE_AMOUNT,
            },
            DagRunType.OPERATOR_TRIGGERED,
        )

        self.assertEqual(request["mode"], "catchup")
        self.assertEqual(request["page_amount"], DEFAULT_CATCHUP_PAGE_AMOUNT)

    def test_live_bronze_pages_follow_before_cursors(self):
        connection = object()
        request = {
            "mode": "live",
            "source": "coinbase",
            "product_id": "BTC-USD",
            "process_key": "live:coinbase:BTC-USD",
            "max_pages": 2,
            "cursor_direction": "before",
        }
        task_instance = SimpleNamespace(
            dag_id="coinbase_trades_live",
            run_id="run-1",
            task_id="ingest_bronze_pages",
            try_number=1,
        )
        context = {
            "ti": task_instance,
            "dag_run": SimpleNamespace(run_id="run-1"),
        }

        with patch("trades.trades_dag.market_data_connection") as connection_factory:
            connection_factory.return_value.__enter__.return_value = connection
            with patch(
                "trades.trades_dag.start_or_resume_ingestion",
                return_value=("105", "ACTIVE", 0),
            ):
                with patch(
                    "trades.trades_dag.ingest_trade_page",
                    side_effect=(
                        {
                            "ingestion_id": 91,
                            "response_cb_before": "106",
                            "response_cb_after": "104",
                        },
                        {
                            "ingestion_id": 92,
                            "response_cb_before": "107",
                            "response_cb_after": "105",
                        },
                    ),
                ) as ingest:
                    result = _ingest_bronze_pages(request, context)

        self.assertEqual(result["ingestion_ids"], [91, 92])
        self.assertEqual(ingest.call_args_list[0].kwargs["before"], "105")
        self.assertIsNone(ingest.call_args_list[0].kwargs["after"])
        self.assertEqual(ingest.call_args_list[1].kwargs["before"], "106")
        self.assertIsNone(ingest.call_args_list[1].kwargs["after"])

    def test_catchup_bronze_pages_follow_after_cursors(self):
        connection = object()
        request = {
            "mode": "catchup",
            "source": "coinbase",
            "product_id": "BTC-USD",
            "process_key": "catchup:one",
            "oldest_time": None,
            "initial_after_cursor": "105",
            "cursor_direction": "after",
            "page_amount": 2,
        }
        task_instance = SimpleNamespace(
            dag_id="coinbase_trades_catchup",
            run_id="run-2",
            task_id="ingest_bronze_pages",
            try_number=1,
        )
        context = {
            "ti": task_instance,
            "dag_run": SimpleNamespace(run_id="run-2"),
        }

        with patch("trades.trades_dag.market_data_connection") as connection_factory:
            connection_factory.return_value.__enter__.return_value = connection
            with patch(
                "trades.trades_dag.start_or_resume_ingestion",
                return_value=("105", "ACTIVE", 0),
            ):
                with patch(
                    "trades.trades_dag.committed_after_cursor_exists",
                    return_value=False,
                ):
                    with patch(
                        "trades.trades_dag.ingest_trade_page",
                        side_effect=(
                            {
                                "ingestion_id": 91,
                                "response_cb_before": "106",
                                "response_cb_after": "104",
                            },
                            {
                                "ingestion_id": 92,
                                "response_cb_before": "107",
                                "response_cb_after": "103",
                            },
                        ),
                    ) as ingest:
                        result = _ingest_bronze_pages(request, context)

        self.assertEqual(result["ingestion_ids"], [91, 92])
        self.assertIsNone(ingest.call_args_list[0].kwargs["before"])
        self.assertEqual(ingest.call_args_list[0].kwargs["after"], "105")
        self.assertIsNone(ingest.call_args_list[1].kwargs["before"])
        self.assertEqual(ingest.call_args_list[1].kwargs["after"], "104")

    def test_catchup_stops_before_fetching_an_exact_committed_cursor(self):
        connection = object()
        request = {
            "mode": "catchup",
            "source": "coinbase",
            "product_id": "BTC-USD",
            "process_key": "catchup:one",
            "oldest_time": None,
            "initial_after_cursor": "105",
            "cursor_direction": "after",
            "page_amount": 100,
        }
        context = {
            "ti": SimpleNamespace(
                dag_id="coinbase_trades_catchup",
                run_id="run-overlap",
                task_id="ingest_bronze_pages",
                try_number=1,
            ),
            "dag_run": SimpleNamespace(run_id="run-overlap"),
        }

        with patch("trades.trades_dag.market_data_connection") as connection_factory:
            connection_factory.return_value.__enter__.return_value = connection
            with patch(
                "trades.trades_dag.start_or_resume_ingestion",
                return_value=("105", "ACTIVE", 10),
            ):
                with patch(
                    "trades.trades_dag.committed_after_cursor_exists",
                    return_value=True,
                ):
                    with patch("trades.trades_dag.ingest_trade_page") as ingest:
                        result = _ingest_bronze_pages(request, context)

        ingest.assert_not_called()
        self.assertEqual(result["ingestion_ids"], [])
        self.assertTrue(result["overlap_reached"])
        self.assertEqual(result["pages_committed_before"], 10)

    def test_catchup_uses_four_page_operational_batches(self):
        connection = object()
        request = {
            "mode": "catchup",
            "source": "coinbase",
            "product_id": "BTC-USD",
            "process_key": "catchup:one",
            "oldest_time": None,
            "initial_after_cursor": "105",
            "cursor_direction": "after",
            "page_amount": 100,
        }
        context = {
            "ti": SimpleNamespace(
                dag_id="coinbase_trades_catchup",
                run_id="run-batch",
                task_id="ingest_bronze_pages",
                try_number=1,
            ),
            "dag_run": SimpleNamespace(run_id="run-batch"),
        }
        pages = [
            {
                "ingestion_id": 90 + index,
                "response_cb_before": str(200 + index),
                "response_cb_after": str(104 - index),
            }
            for index in range(CATCHUP_PAGES_PER_BATCH)
        ]

        with patch("trades.trades_dag.market_data_connection") as connection_factory:
            connection_factory.return_value.__enter__.return_value = connection
            with patch(
                "trades.trades_dag.start_or_resume_ingestion",
                return_value=("105", "ACTIVE", 20),
            ):
                with patch(
                    "trades.trades_dag.committed_after_cursor_exists",
                    return_value=False,
                ):
                    with patch(
                        "trades.trades_dag.ingest_trade_page",
                        side_effect=pages,
                    ) as ingest:
                        result = _ingest_bronze_pages(request, context)

        self.assertEqual(ingest.call_count, CATCHUP_PAGES_PER_BATCH)
        self.assertEqual(len(result["ingestion_ids"]), CATCHUP_PAGES_PER_BATCH)

    def test_scheduled_run_cannot_enable_catchup_mode(self):
        with self.assertRaisesRegex(ValueError, "only be started manually"):
            build_catchup_ingestion_request(
                {
                    "after_cursor": None,
                    "page_amount": 100,
                },
                "scheduled",
            )

    def test_airflow_schedule_backfill_cannot_run_trade_catchup(self):
        with self.assertRaisesRegex(ValueError, "only be started manually"):
            build_catchup_ingestion_request(
                {"after_cursor": None, "page_amount": 100},
                "backfill",
            )


if __name__ == "__main__":
    unittest.main()
