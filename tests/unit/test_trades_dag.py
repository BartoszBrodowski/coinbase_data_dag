import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

from airflow.utils.types import DagRunType
from jinja2 import Environment


DAGS_DIRECTORY = Path(__file__).parents[2] / "dags"
sys.path.insert(0, str(DAGS_DIRECTORY))

from trades.settings import (
    CATCHUP_PAGES_PER_BATCH,
    DEFAULT_CATCHUP_PAGE_AMOUNT,
    MAX_LIVE_PAGES_PER_RUN,
    TRADES_API_POOL,
)
from trades.gap_detection import find_newest_trade_gap
from trades.catchup_dag import catchup_dag
from trades.gap_repair_dag import gap_repair_dag
from trades.live_dag import live_dag
from trades.pipeline_tasks import (
    _finish_gap_repair,
    _finish_trade_ingestion,
    _ingest_bronze_pages,
    _load_silver_pages,
    _wait_for_gap_repair_completion,
    build_catchup_ingestion_request,
    build_live_ingestion_request,
)


class TradesDagTest(unittest.TestCase):
    def test_gap_repair_dag_requires_manual_run_and_human_approval(self):
        self.assertEqual(
            {run_type.value for run_type in gap_repair_dag.allowed_run_types},
            {DagRunType.MANUAL.value},
        )
        self.assertNotIn(
            DagRunType.BACKFILL_JOB.value,
            {run_type.value for run_type in gap_repair_dag.allowed_run_types},
        )

        detect_task = gap_repair_dag.get_task("detect_newest_trade_gap")
        choice_task = gap_repair_dag.get_task("choose_gap_repair")
        approval_task = gap_repair_dag.get_task("approve_gap_repair")
        no_gap_task = gap_repair_dag.get_task("no_gap_found")
        conf_task = gap_repair_dag.get_task("build_gap_repair_conf")
        trigger_task = gap_repair_dag.get_task("trigger_gap_repair")
        wait_task = gap_repair_dag.get_task("wait_for_gap_repair_completion")
        finish_task = gap_repair_dag.get_task("finish_gap_repair")

        self.assertEqual(
            detect_task.downstream_task_ids,
            {
                choice_task.task_id,
                conf_task.task_id,
                wait_task.task_id,
                finish_task.task_id,
            },
        )
        self.assertEqual(
            choice_task.downstream_task_ids,
            {approval_task.task_id, no_gap_task.task_id},
        )
        self.assertEqual(approval_task.downstream_task_ids, {conf_task.task_id})
        self.assertEqual(conf_task.downstream_task_ids, {trigger_task.task_id})
        self.assertEqual(trigger_task.trigger_dag_id, "coinbase_trades_catchup")
        self.assertEqual(trigger_task.downstream_task_ids, {wait_task.task_id})
        self.assertEqual(wait_task.downstream_task_ids, {finish_task.task_id})
        self.assertEqual(wait_task.mode, "reschedule")
        self.assertIn("after_cursor", approval_task.body)
        Environment().parse(approval_task.body)
        self.assertEqual(approval_task.response_timeout, timedelta(hours=24))

    def test_gap_repair_waits_for_the_durable_process_to_complete(self):
        gap = {
            "product_id": "BTC-USD",
            "after_cursor": "1080500000",
            "page_amount": 25,
        }
        connection = object()

        with patch(
            "trades.pipeline_tasks.market_data_connection"
        ) as connection_factory:
            connection_factory.return_value.__enter__.return_value = connection
            with patch(
                "trades.pipeline_tasks.trade_ingestion_status",
                side_effect=(None, "COMPLETED"),
            ):
                waiting = _wait_for_gap_repair_completion(gap)
                completed = _wait_for_gap_repair_completion(gap)

        self.assertFalse(waiting.is_done)
        self.assertIsNone(waiting.xcom_value)
        self.assertTrue(completed.is_done)
        self.assertEqual(completed.xcom_value["mode"], "catchup")
        self.assertEqual(
            completed.xcom_value["initial_after_cursor"],
            "1080500000",
        )

    def test_gap_repair_logs_cumulative_triggered_catchup_totals(self):
        connection = object()
        request = {"process_key": "catchup:gap-summary"}
        gap = {
            "older_trade_time": "2026-08-24T12:15:01+00:00",
            "newer_trade_time": "2026-08-24T12:29:36+00:00",
        }
        summary = {
            "product_id": "BTC-USD",
            "status": "COMPLETED",
            "pages_committed": 17,
            "trades_committed": 16_002,
            "initial_after_cursor": "1080541039",
            "resume_after_cursor": "1080524000",
            "duration_seconds": 90.25,
        }

        with patch(
            "trades.pipeline_tasks.market_data_connection"
        ) as connection_factory:
            connection_factory.return_value.__enter__.return_value = connection
            with patch(
                "trades.pipeline_tasks.trade_ingestion_summary",
                return_value=summary,
            ):
                with self.assertLogs("trades.pipeline_tasks", level="INFO") as logs:
                    result = _finish_gap_repair(request, gap)

        self.assertEqual(result, summary)
        self.assertIn("mode=gap_repair", logs.output[0])
        self.assertIn("total_pages_committed=17", logs.output[0])
        self.assertIn("total_trade_records_committed=16002", logs.output[0])

    def test_gap_detection_returns_small_serializable_approval_summary(self):
        older_time = datetime(2026, 8, 24, 12, 15, 1, tzinfo=timezone.utc)
        newer_time = datetime(2026, 8, 24, 12, 29, 36, tzinfo=timezone.utc)
        cursor = MagicMock()
        cursor.fetchone.return_value = (
            older_time,
            newer_time,
            875,
            1080524000,
            1080540000,
            "1080541039",
            17,
            17,
        )
        connection = MagicMock()
        connection.cursor.return_value.__enter__.return_value = cursor

        result = find_newest_trade_gap(
            connection,
            source="coinbase",
            product_id="BTC-USD",
            lookback_start=datetime(2026, 8, 17, tzinfo=timezone.utc),
            eligible_before=datetime(2026, 8, 24, 13, tzinfo=timezone.utc),
            minimum_gap_seconds=30,
            maximum_page_amount=1000,
        )

        self.assertEqual(result["after_cursor"], "1080541039")
        self.assertEqual(result["page_amount"], 17)
        self.assertFalse(result["page_amount_capped"])
        self.assertEqual(result["older_trade_time"], older_time.isoformat())
        self.assertEqual(result["newer_trade_time"], newer_time.isoformat())
        cursor.execute.assert_called_once()

    def test_gap_detection_returns_none_when_coverage_is_continuous(self):
        cursor = MagicMock()
        cursor.fetchone.return_value = None
        connection = MagicMock()
        connection.cursor.return_value.__enter__.return_value = cursor

        result = find_newest_trade_gap(
            connection,
            source="coinbase",
            product_id="BTC-USD",
            lookback_start=datetime(2026, 8, 17, tzinfo=timezone.utc),
            eligible_before=datetime(2026, 8, 24, 13, tzinfo=timezone.utc),
            minimum_gap_seconds=30,
            maximum_page_amount=1000,
        )

        self.assertIsNone(result)

    def test_live_dag_allows_scheduled_manual_and_operator_continuations(self):
        self.assertEqual(
            {run_type.value for run_type in live_dag.allowed_run_types},
            {
                DagRunType.SCHEDULED.value,
                DagRunType.MANUAL.value,
                DagRunType.OPERATOR_TRIGGERED.value,
            },
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

    def test_live_dag_continues_until_committed_coverage(self):
        validation_task = live_dag.get_task("resolve_live_ingestion_request")
        bronze_task = live_dag.get_task("ingest_bronze_pages")
        silver_task = live_dag.get_task("load_silver_pages")
        gold_task = live_dag.get_task("load_gold_trade_activity")
        continuation_task = live_dag.get_task("choose_live_continuation")
        trigger_task = live_dag.get_task("continue_live_ingestion")
        finish_task = live_dag.get_task("finish_live_ingestion")

        self.assertEqual(
            validation_task.downstream_task_ids,
            {bronze_task.task_id},
        )
        self.assertEqual(
            bronze_task.downstream_task_ids,
            {silver_task.task_id, "build_live_continuation_conf"},
        )
        self.assertEqual(
            silver_task.downstream_task_ids,
            {gold_task.task_id, continuation_task.task_id},
        )
        self.assertEqual(gold_task.downstream_task_ids, {continuation_task.task_id})
        self.assertEqual(
            continuation_task.downstream_task_ids,
            {trigger_task.task_id, finish_task.task_id},
        )

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
                finish_task.task_id,
            },
        )
        self.assertEqual(bronze_task.downstream_task_ids, {silver_task.task_id})
        self.assertEqual(
            silver_task.downstream_task_ids,
            {
                gold_task.task_id,
                continuation_task.task_id,
                finish_task.task_id,
            },
        )
        self.assertEqual(
            gold_task.downstream_task_ids,
            {continuation_task.task_id},
        )
        self.assertEqual(
            continuation_task.downstream_task_ids,
            {trigger_task.task_id, finish_task.task_id},
        )

    def test_catchup_completion_logs_cumulative_process_totals(self):
        connection = object()
        request = {"process_key": "catchup:summary"}
        silver_result = {
            "trades_processed": 327,
            "stop_reason": "page_amount_reached",
        }
        summary = {
            "product_id": "BTC-USD",
            "status": "COMPLETED",
            "pages_committed": 25,
            "trades_committed": 24_327,
            "initial_after_cursor": "1080500000",
            "resume_after_cursor": "1080475000",
            "duration_seconds": 125.5,
        }

        with patch(
            "trades.pipeline_tasks.market_data_connection"
        ) as connection_factory:
            connection_factory.return_value.__enter__.return_value = connection
            with patch(
                "trades.pipeline_tasks.trade_ingestion_summary",
                return_value=summary,
            ):
                with self.assertLogs("trades.pipeline_tasks", level="INFO") as logs:
                    result = _finish_trade_ingestion(request, silver_result)

        self.assertEqual(result, summary)
        self.assertIn("total_pages_committed=25", logs.output[0])
        self.assertIn("total_trade_records_committed=24327", logs.output[0])
        self.assertIn("final_batch_trade_records=327", logs.output[0])

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
            "overlap_trade_id": 100,
            "next_after_cursor": "200",
            "bronze_stop_reason": "batch_limit_reached",
        }

        with patch("trades.pipeline_tasks.market_data_connection") as connection_factory:
            connection_factory.return_value.__enter__.return_value = connection
            with patch(
                "trades.pipeline_tasks.transform_bronze_to_silver",
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
                "stop_reason": "batch_limit_reached",
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

        with patch("trades.pipeline_tasks.market_data_connection") as connection_factory:
            connection_factory.return_value.__enter__.return_value = connection
            with patch(
                "trades.pipeline_tasks.transform_bronze_to_silver",
                return_value=transform_result,
            ):
                with patch("trades.pipeline_tasks.complete_ingestion") as complete:
                    with patch("trades.pipeline_tasks.pause_ingestion") as pause:
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

        with patch("trades.pipeline_tasks.market_data_connection") as connection_factory:
            connection_factory.return_value.__enter__.return_value = connection
            with patch(
                "trades.pipeline_tasks.transform_bronze_to_silver",
                return_value=transform_result,
            ):
                with patch("trades.pipeline_tasks.complete_ingestion") as complete:
                    with patch("trades.pipeline_tasks.pause_ingestion") as pause:
                        result = _load_silver_pages(batch)

        pause.assert_called_once_with(connection, "catchup:one")
        complete.assert_not_called()
        self.assertFalse(result["completed"])
        self.assertEqual(result["pages_committed"], 24)
        self.assertEqual(result["stop_reason"], "batch_limit_reached")

    def test_scheduled_run_resolves_to_live_mode(self):
        request = build_live_ingestion_request()

        self.assertEqual(request["mode"], "live")
        self.assertEqual(request["cursor_direction"], "after")
        self.assertFalse(request["is_continuation"])
        self.assertEqual(request["max_pages"], MAX_LIVE_PAGES_PER_RUN)

    def test_operator_run_resolves_to_live_backward_scan_continuation(self):
        request = build_live_ingestion_request(
            DagRunType.OPERATOR_TRIGGERED,
            {"after_cursor": "150", "overlap_trade_id": 100},
        )

        self.assertTrue(request["is_continuation"])
        self.assertEqual(request["initial_after_cursor"], "150")
        self.assertEqual(request["overlap_trade_id"], 100)

    def test_manual_run_can_resolve_to_bounded_catchup_mode(self):
        request = build_catchup_ingestion_request(
            {
                "after_cursor": "105",
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
                "after_cursor": "105",
                "page_amount": DEFAULT_CATCHUP_PAGE_AMOUNT,
            },
            DagRunType.OPERATOR_TRIGGERED,
        )

        self.assertEqual(request["mode"], "catchup")
        self.assertEqual(request["page_amount"], DEFAULT_CATCHUP_PAGE_AMOUNT)

    def test_live_bronze_pages_fetch_head_then_walk_older_until_overlap(self):
        connection = object()
        request = {
            "mode": "live",
            "source": "coinbase",
            "product_id": "BTC-USD",
            "process_key": "live:coinbase:BTC-USD",
            "max_pages": 3,
            "cursor_direction": "after",
            "initial_after_cursor": None,
            "overlap_trade_id": None,
            "is_continuation": False,
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

        with patch("trades.pipeline_tasks.market_data_connection") as connection_factory:
            connection_factory.return_value.__enter__.return_value = connection
            with patch(
                "trades.pipeline_tasks.start_or_resume_ingestion",
                return_value=("105", "ACTIVE", 0),
            ):
                with patch(
                    "trades.pipeline_tasks.highest_committed_trade_id",
                    return_value=100,
                ):
                    with patch(
                    "trades.pipeline_tasks.committed_after_cursor_exists",
                        return_value=False,
                    ):
                        with patch(
                            "trades.pipeline_tasks.ingest_trade_page",
                            side_effect=(
                                {
                                    "ingestion_id": 91,
                                    "response_cb_before": "300",
                                    "response_cb_after": "201",
                                },
                                {
                                    "ingestion_id": 92,
                                    "response_cb_before": "200",
                                    "response_cb_after": "101",
                                },
                                {
                                    "ingestion_id": 93,
                                    "response_cb_before": "100",
                                    "response_cb_after": "1",
                                },
                            ),
                        ) as ingest:
                            result = _ingest_bronze_pages(request, context)

        self.assertEqual(result["ingestion_ids"], [91, 92, 93])
        self.assertIsNone(ingest.call_args_list[0].kwargs["before"])
        self.assertIsNone(ingest.call_args_list[0].kwargs["after"])
        self.assertEqual(ingest.call_args_list[1].kwargs["after"], "201")
        self.assertEqual(ingest.call_args_list[2].kwargs["after"], "101")
        self.assertTrue(result["overlap_reached"])
        self.assertEqual(result["bronze_stop_reason"], "committed_trade_id_reached")

    def test_live_continuation_uses_its_carried_after_cursor(self):
        connection = object()
        request = {
            "mode": "live",
            "source": "coinbase",
            "product_id": "BTC-USD",
            "process_key": "live:coinbase:BTC-USD",
            "max_pages": 2,
            "cursor_direction": "after",
            "initial_after_cursor": "201",
            "overlap_trade_id": 100,
            "is_continuation": True,
        }
        context = {
            "ti": SimpleNamespace(
                dag_id="coinbase_trades_live",
                run_id="run-continuation",
                task_id="ingest_bronze_pages",
                try_number=1,
            ),
            "dag_run": SimpleNamespace(run_id="run-continuation"),
        }

        with patch("trades.pipeline_tasks.market_data_connection") as connection_factory:
            connection_factory.return_value.__enter__.return_value = connection
            with patch(
                "trades.pipeline_tasks.start_or_resume_ingestion",
                return_value=("stale-shared-cursor", "ACTIVE", 3),
            ):
                with patch(
                    "trades.pipeline_tasks.committed_after_cursor_exists",
                    return_value=False,
                ):
                    with patch(
                            "trades.pipeline_tasks.ingest_trade_page",
                        side_effect=(
                            {
                                "ingestion_id": 94,
                                "response_cb_before": "200",
                                "response_cb_after": "101",
                            },
                            {
                                "ingestion_id": 95,
                                "response_cb_before": "100",
                                "response_cb_after": "1",
                            },
                        ),
                    ) as ingest:
                        result = _ingest_bronze_pages(request, context)

        self.assertEqual(ingest.call_args_list[0].kwargs["after"], "201")
        self.assertEqual(ingest.call_args_list[1].kwargs["after"], "101")
        self.assertEqual(result["bronze_stop_reason"], "committed_trade_id_reached")

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

        with patch("trades.pipeline_tasks.market_data_connection") as connection_factory:
            connection_factory.return_value.__enter__.return_value = connection
            with patch(
                "trades.pipeline_tasks.start_or_resume_ingestion",
                return_value=("105", "ACTIVE", 0),
            ):
                with patch(
                    "trades.pipeline_tasks.committed_after_cursor_exists",
                    return_value=False,
                ):
                    with patch(
                            "trades.pipeline_tasks.ingest_trade_page",
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

        with patch("trades.pipeline_tasks.market_data_connection") as connection_factory:
            connection_factory.return_value.__enter__.return_value = connection
            with patch(
                "trades.pipeline_tasks.start_or_resume_ingestion",
                return_value=("105", "ACTIVE", 10),
            ):
                with patch(
                    "trades.pipeline_tasks.committed_after_cursor_exists",
                    return_value=True,
                ):
                    with patch("trades.pipeline_tasks.ingest_trade_page") as ingest:
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

        with patch("trades.pipeline_tasks.market_data_connection") as connection_factory:
            connection_factory.return_value.__enter__.return_value = connection
            with patch(
                "trades.pipeline_tasks.start_or_resume_ingestion",
                return_value=("105", "ACTIVE", 20),
            ):
                with patch(
                    "trades.pipeline_tasks.committed_after_cursor_exists",
                    return_value=False,
                ):
                    with patch(
                            "trades.pipeline_tasks.ingest_trade_page",
                        side_effect=pages,
                    ) as ingest:
                        result = _ingest_bronze_pages(request, context)

        self.assertEqual(ingest.call_count, CATCHUP_PAGES_PER_BATCH)
        self.assertEqual(len(result["ingestion_ids"]), CATCHUP_PAGES_PER_BATCH)

    def test_scheduled_run_cannot_enable_catchup_mode(self):
        with self.assertRaisesRegex(ValueError, "only be started manually"):
            build_catchup_ingestion_request(
                {
                    "after_cursor": "105",
                    "page_amount": 100,
                },
                "scheduled",
            )

    def test_airflow_schedule_backfill_cannot_run_trade_catchup(self):
        with self.assertRaisesRegex(ValueError, "only be started manually"):
            build_catchup_ingestion_request(
                {"after_cursor": "105", "page_amount": 100},
                "backfill",
            )


if __name__ == "__main__":
    unittest.main()
