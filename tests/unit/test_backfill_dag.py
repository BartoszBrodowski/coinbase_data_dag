import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path


DAGS_DIRECTORY = Path(__file__).parents[2] / "dags"
sys.path.insert(0, str(DAGS_DIRECTORY))

from candles.backfill_dag import (
    MAX_ACTIVE_API_TASKS,
    dag,
    parse_utc_datetime,
    split_backfill_interval,
)


class BackfillIntervalTest(unittest.TestCase):
    def test_splits_one_year_into_coinbase_safe_intervals(self):
        start = datetime(2025, 1, 1, tzinfo=timezone.utc)
        end = datetime(2026, 1, 1, tzinfo=timezone.utc)

        intervals = split_backfill_interval("BTC-USD", 3600, start, end)

        self.assertEqual(len(intervals), 30)
        self.assertEqual(parse_utc_datetime(intervals[0]["start"]), start)
        self.assertEqual(parse_utc_datetime(intervals[-1]["end"]), end)

        for current, following in zip(intervals, intervals[1:]):
            current_start = parse_utc_datetime(current["start"])
            current_end = parse_utc_datetime(current["end"])
            following_start = parse_utc_datetime(following["start"])

            self.assertLessEqual((current_end - current_start).total_seconds(), 300 * 3600)
            self.assertEqual(current_end, following_start)

    def test_rejects_invalid_interval(self):
        value = datetime(2025, 1, 1, tzinfo=timezone.utc)

        with self.assertRaisesRegex(ValueError, "start must be earlier than end"):
            split_backfill_interval("BTC-USD", 3600, value, value)

    def test_requires_timezone_in_input(self):
        with self.assertRaisesRegex(ValueError, "must include a timezone"):
            parse_utc_datetime("2025-01-01T00:00:00")


class BackfillDagTest(unittest.TestCase):
    def test_is_manual_only_and_allows_one_active_run(self):
        self.assertIsNone(dag.schedule)
        self.assertFalse(dag.catchup)
        self.assertEqual(dag.max_active_runs, 1)

    def test_pipeline_dependencies(self):
        self.assertEqual(
            dag.get_task("create_backfill_intervals").downstream_task_ids,
            {"ingest_backfill_interval"},
        )
        self.assertEqual(
            dag.get_task("ingest_backfill_interval").downstream_task_ids,
            {"load_backfill_silver"},
        )
        self.assertEqual(
            dag.get_task("load_backfill_silver").downstream_task_ids,
            {"load_backfill_gold"},
        )
        self.assertEqual(
            dag.get_task("load_backfill_gold").downstream_task_ids,
            {"test_backfill_gold_quality"},
        )

    def test_only_api_tasks_are_retried_and_throttled(self):
        validation_task = dag.get_task("create_backfill_intervals")
        ingest_task = dag.get_task("ingest_backfill_interval")

        self.assertEqual(validation_task.retries, 0)
        self.assertEqual(ingest_task.retries, 2)
        self.assertEqual(ingest_task.max_active_tis_per_dag, MAX_ACTIVE_API_TASKS)


if __name__ == "__main__":
    unittest.main()
