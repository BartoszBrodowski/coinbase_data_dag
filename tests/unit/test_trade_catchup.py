import sys
import unittest
from pathlib import Path


DAGS_DIRECTORY = Path(__file__).parents[2] / "dags"
sys.path.insert(0, str(DAGS_DIRECTORY))

from trades.ingestion_state import (
    committed_after_cursor_exists,
    live_process_key,
    start_or_resume_ingestion,
    validate_catchup_request,
)
from trades.settings import MAX_CATCHUP_PAGE_AMOUNT


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
    def test_builds_a_stable_key_for_the_same_page_request(self):
        first = validate_catchup_request(
            "test-exchange",
            "BTC-USD",
            None,
            100,
        )
        second = validate_catchup_request(
            "test-exchange",
            "BTC-USD",
            None,
            100,
        )

        self.assertIsNone(first["oldest_time"])
        self.assertEqual(first["page_amount"], 100)
        self.assertEqual(first["process_key"], second["process_key"])

    def test_rejects_a_page_amount_that_exceeds_the_safety_limit(self):
        with self.assertRaisesRegex(
            ValueError,
            f"between 1 and {MAX_CATCHUP_PAGE_AMOUNT}",
        ):
            validate_catchup_request(
                "test-exchange",
                "BTC-USD",
                None,
                MAX_CATCHUP_PAGE_AMOUNT + 1,
            )

    def test_normalizes_an_optional_explicit_cursor(self):
        request = validate_catchup_request(
            "test-exchange",
            "BTC-USD",
            " 1068720621 ",
            25,
        )

        self.assertEqual(request["initial_after_cursor"], "1068720621")
        self.assertIsNone(request["oldest_time"])
        self.assertEqual(request["page_amount"], 25)


class TradeCatchupStateTest(unittest.TestCase):
    def test_live_mode_uses_the_same_process_state(self):
        request = {
            "process_key": live_process_key("test-exchange", "BTC-USD"),
            "source": "test-exchange",
            "product_id": "BTC-USD",
            "mode": "live",
            "oldest_time": None,
            "cursor_direction": "before",
            "initial_before_cursor": None,
            "initial_after_cursor": None,
        }
        connection = _Connection((None,))

        cursor, status, pages_committed = start_or_resume_ingestion(
            connection,
            request,
            "run-live",
        )

        self.assertEqual((cursor, status, pages_committed), (None, "ACTIVE", 0))
        self.assertTrue(connection.committed)
        self.assertEqual(len(connection.cursor_instance.executions), 2)
        insert_parameters = connection.cursor_instance.executions[1][1]
        self.assertIsNone(insert_parameters["resume_before_cursor"])

    def test_new_page_count_job_starts_from_live_watermark(self):
        request = validate_catchup_request(
            "test-exchange",
            "BTC-USD",
            None,
            100,
        )
        connection = _Connection((None, ("live-cursor",)))

        cursor, status, pages_committed = start_or_resume_ingestion(
            connection,
            request,
            "run-1",
        )

        self.assertEqual(
            (cursor, status, pages_committed),
            ("live-cursor", "ACTIVE", 0),
        )
        self.assertTrue(connection.committed)
        self.assertEqual(len(connection.cursor_instance.executions), 3)
        insert_parameters = connection.cursor_instance.executions[2][1]
        self.assertEqual(insert_parameters["initial_after_cursor"], "live-cursor")
        self.assertEqual(insert_parameters["resume_after_cursor"], "live-cursor")

    def test_new_page_count_job_requires_a_live_or_explicit_cursor(self):
        request = validate_catchup_request(
            "test-exchange",
            "BTC-USD",
            None,
            100,
        )
        connection = _Connection((None, None))

        with self.assertRaisesRegex(ValueError, "committed live cursor"):
            start_or_resume_ingestion(connection, request, "run-1")

    def test_existing_live_job_resumes_from_before_cursor(self):
        request = {
            "process_key": live_process_key("test-exchange", "BTC-USD"),
            "source": "test-exchange",
            "product_id": "BTC-USD",
            "mode": "live",
            "oldest_time": None,
            "cursor_direction": "before",
            "initial_before_cursor": None,
            "initial_after_cursor": None,
        }
        connection = _Connection(
            (("saved-before", "saved-after", "ACTIVE", 12),)
        )

        cursor, status, pages_committed = start_or_resume_ingestion(
            connection,
            request,
            "run-live-2",
        )

        self.assertEqual(
            (cursor, status, pages_committed),
            ("saved-before", "ACTIVE", 12),
        )

    def test_existing_job_resumes_from_its_committed_cursor(self):
        request = validate_catchup_request(
            "test-exchange",
            "BTC-USD",
            None,
            100,
        )
        connection = _Connection(((None, "saved-cursor", "PAUSED", 8),))

        cursor, status, pages_committed = start_or_resume_ingestion(
            connection,
            request,
            "run-2",
        )

        self.assertEqual(
            (cursor, status, pages_committed),
            ("saved-cursor", "ACTIVE", 8),
        )
        self.assertTrue(connection.committed)
        self.assertEqual(len(connection.cursor_instance.executions), 2)

    def test_detects_an_exact_cursor_page_already_committed_to_silver(self):
        connection = _Connection(((1,),))

        exists = committed_after_cursor_exists(
            connection,
            "BTC-USD",
            "1068720621",
        )

        self.assertTrue(exists)
        _, parameters = connection.cursor_instance.executions[0]
        self.assertEqual(parameters["product_id"], "BTC-USD")
        self.assertEqual(parameters["requested_after"], "1068720621")


if __name__ == "__main__":
    unittest.main()
