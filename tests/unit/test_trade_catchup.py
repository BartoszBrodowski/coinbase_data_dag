import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path


DAGS_DIRECTORY = Path(__file__).parents[2] / "dags"
sys.path.insert(0, str(DAGS_DIRECTORY))

from trades.ingestion_state import (
    live_ingestion_key,
    start_or_resume_ingestion,
    validate_catchup_request,
)


class _Cursor:
    def __init__(self, results):
        self.results = iter(results)
        self.executions = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return None

    def execute(self, sql, parameters):
        self.executions.append((sql, parameters))

    def fetchone(self):
        return next(self.results)


class _Connection:
    def __init__(self, results):
        self.cursor_instance = _Cursor(results)
        self.committed = False

    def __enter__(self):
        return self

    def __exit__(self, exception_type, *_):
        self.committed = exception_type is None

    def cursor(self):
        return self.cursor_instance


class TradeCatchupRequestTest(unittest.TestCase):
    def test_requires_an_explicit_time_or_cursor_boundary(self):
        with self.assertRaisesRegex(ValueError, "requires oldest_time or after_cursor"):
            validate_catchup_request("BTC-USD", None, None, 3)

    def test_normalizes_time_and_builds_a_stable_key(self):
        first = validate_catchup_request(
            "BTC-USD",
            "2026-08-01T02:00:00+02:00",
            None,
            3,
        )
        second = validate_catchup_request(
            "BTC-USD",
            "2026-08-01T00:00:00Z",
            None,
            10,
        )

        self.assertEqual(first["oldest_time"], "2026-08-01T00:00:00+00:00")
        self.assertEqual(first["ingestion_key"], second["ingestion_key"])

    def test_rejects_an_unbounded_page_count(self):
        with self.assertRaisesRegex(ValueError, "between 1 and 20"):
            validate_catchup_request(
                "BTC-USD",
                datetime(2026, 8, 1, tzinfo=timezone.utc).isoformat(),
                None,
                21,
            )

    def test_accepts_an_explicit_cursor_without_a_time_boundary(self):
        request = validate_catchup_request(
            "BTC-USD",
            None,
            " 1068720621 ",
            1,
        )

        self.assertEqual(request["initial_after_cursor"], "1068720621")
        self.assertIsNone(request["oldest_time"])


class TradeCatchupStateTest(unittest.TestCase):
    def test_live_mode_uses_the_same_process_state(self):
        request = {
            "ingestion_key": live_ingestion_key("BTC-USD"),
            "source": "coinbase",
            "product_id": "BTC-USD",
            "mode": "live",
            "oldest_time": None,
            "initial_after_cursor": None,
        }
        connection = _Connection((None,))

        cursor, status = start_or_resume_ingestion(connection, request, "run-live")

        self.assertEqual((cursor, status), (None, "ACTIVE"))
        self.assertTrue(connection.committed)
        self.assertEqual(len(connection.cursor_instance.executions), 2)

    def test_new_time_bounded_job_starts_from_live_watermark(self):
        request = validate_catchup_request(
            "BTC-USD",
            "2026-08-01T00:00:00Z",
            None,
            3,
        )
        connection = _Connection((None, ("live-cursor",)))

        cursor, status = start_or_resume_ingestion(connection, request, "run-1")

        self.assertEqual((cursor, status), ("live-cursor", "ACTIVE"))
        self.assertTrue(connection.committed)
        self.assertEqual(len(connection.cursor_instance.executions), 3)
        insert_parameters = connection.cursor_instance.executions[2][1]
        self.assertEqual(insert_parameters["resume_after_cursor"], "live-cursor")

    def test_existing_job_resumes_from_its_committed_cursor(self):
        request = validate_catchup_request(
            "BTC-USD",
            "2026-08-01T00:00:00Z",
            None,
            3,
        )
        connection = _Connection((("saved-cursor", "PAUSED"),))

        cursor, status = start_or_resume_ingestion(connection, request, "run-2")

        self.assertEqual((cursor, status), ("saved-cursor", "PAUSED"))
        self.assertTrue(connection.committed)
        self.assertEqual(len(connection.cursor_instance.executions), 2)


if __name__ == "__main__":
    unittest.main()
