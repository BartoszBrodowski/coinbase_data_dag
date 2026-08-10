import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from airflow.timetables.interval import CronDataIntervalTimetable

DAGS_DIRECTORY = Path(__file__).parents[2] / "dags"
sys.path.insert(0, str(DAGS_DIRECTORY))

from candles.scheduled_dag import DAG_SCHEDULE, DAG_START_DATE, dag, get_data_interval


class AirflowDagTest(unittest.TestCase):
    def test_schedule_and_retry_policy(self):
        self.assertIsInstance(DAG_SCHEDULE, CronDataIntervalTimetable)
        self.assertIsInstance(dag.timetable, CronDataIntervalTimetable)
        self.assertEqual(dag.start_date, DAG_START_DATE)
        self.assertFalse(dag.catchup)

        for airflow_task in dag.tasks:
            self.assertEqual(airflow_task.retries, 2)
            self.assertEqual(airflow_task.retry_delay, timedelta(minutes=1))
            self.assertTrue(airflow_task.retry_exponential_backoff)
            self.assertEqual(airflow_task.max_retry_delay, timedelta(minutes=5))

    def test_pipeline_dependencies(self):
        self.assertEqual(
            dag.get_task("ingest_bronze").downstream_task_ids,
            {"load_silver"},
        )
        self.assertEqual(
            dag.get_task("load_silver").downstream_task_ids,
            {"load_gold"},
        )
        self.assertEqual(
            dag.get_task("load_gold").downstream_task_ids,
            {"test_gold_quality"},
        )
        self.assertEqual(
            dag.get_task("test_gold_quality").downstream_task_ids,
            set(),
        )

    def test_uses_interval_supplied_by_airflow(self):
        start = datetime(2025, 1, 1, tzinfo=timezone.utc)
        end = start + timedelta(hours=1)

        self.assertEqual(
            get_data_interval(
                {
                    "data_interval_start": start,
                    "data_interval_end": end,
                }
            ),
            (start, end),
        )

    def test_infers_closed_hour_for_manual_run_without_interval(self):
        run_after = datetime(2025, 1, 1, 12, 30, tzinfo=timezone.utc)
        context = {
            "dag_run": SimpleNamespace(run_after=run_after),
        }

        start, end = get_data_interval(context)

        self.assertEqual(start, datetime(2025, 1, 1, 11, tzinfo=timezone.utc))
        self.assertEqual(end, datetime(2025, 1, 1, 12, tzinfo=timezone.utc))
