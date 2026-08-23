import json
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

DAGS_DIRECTORY = Path(__file__).parents[2] / "dags"
sys.path.insert(0, str(DAGS_DIRECTORY))

from candles.bronze_ingest import save_bronze_response
from shared.coinbase_client import CoinbaseResponse


class _Cursor:
    def __init__(self, results):
        self.results = iter(results)
        self.parameters = None
        self.executions = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return None

    def execute(self, sql, parameters):
        self.parameters = parameters
        self.executions.append((sql, parameters))

    def fetchone(self):
        return next(self.results)


class _Connection:
    def __init__(self, results=((42,),)):
        self.cursor_instance = _Cursor(results)
        self.committed = False

    def __enter__(self):
        return self

    def __exit__(self, exception_type, *_):
        self.committed = exception_type is None

    def cursor(self):
        return self.cursor_instance


class BronzeIngestTest(unittest.TestCase):
    def test_rejects_products_not_enabled_for_candle_ingestion(self):
        start = datetime(2025, 1, 1, tzinfo=timezone.utc)
        end = datetime(2025, 1, 1, 1, tzinfo=timezone.utc)
        response = CoinbaseResponse(200, "[]", "application/json", {}, 10)

        with self.assertRaisesRegex(ValueError, "not enabled for ETH-USD"):
            save_bronze_response(
                _Connection(),
                bronze_request_key="unsupported-product",
                product_id="ETH-USD",
                granularity_seconds=3600,
                start=start,
                end=end,
                response=response,
            )

    def test_saves_payload_and_metadata_in_one_transaction(self):
        connection = _Connection()
        start = datetime(2025, 1, 1, tzinfo=timezone.utc)
        end = datetime(2025, 1, 1, 1, tzinfo=timezone.utc)
        response = CoinbaseResponse(
            status_code=200,
            body="[[1735689600,1,2,1,2,3]]",
            content_type="application/json",
            headers={"Retry-After": "1"},
            duration_ms=125,
        )

        ingestion_id = save_bronze_response(
            connection,
            bronze_request_key="run-1:fetch:1",
            product_id="BTC-USD",
            granularity_seconds=3600,
            start=start,
            end=end,
            response=response,
        )

        parameters = connection.cursor_instance.parameters
        self.assertEqual(ingestion_id, 42)
        self.assertTrue(connection.committed)
        self.assertEqual(parameters["response_body"], response.body)
        self.assertEqual(parameters["http_status"], 200)
        self.assertEqual(parameters["requested_start"], start)
        self.assertEqual(json.loads(parameters["response_headers"]), response.headers)

    def test_returns_existing_id_for_an_existing_bronze_request_key(self):
        start = datetime(2025, 1, 1, tzinfo=timezone.utc)
        end = datetime(2025, 1, 1, 1, tzinfo=timezone.utc)
        response = CoinbaseResponse(
            status_code=200,
            body="[[1735689600,1,2,1,2,3]]",
            content_type="application/json",
            headers={},
            duration_ms=125,
        )
        connection = _Connection(results=(None, (42,)))

        ingestion_id = save_bronze_response(
            connection,
            bronze_request_key="dag:run:fetch:1",
            product_id="BTC-USD",
            granularity_seconds=3600,
            start=start,
            end=end,
            response=response,
        )

        self.assertEqual(ingestion_id, 42)
        self.assertTrue(connection.committed)
        self.assertEqual(len(connection.cursor_instance.executions), 2)

    def test_fails_if_existing_ingestion_cannot_be_read(self):
        start = datetime(2025, 1, 1, tzinfo=timezone.utc)
        end = datetime(2025, 1, 1, 1, tzinfo=timezone.utc)
        response = CoinbaseResponse(
            status_code=200,
            body="[]",
            content_type="application/json",
            headers={},
            duration_ms=125,
        )
        connection = _Connection(results=(None, None))

        with self.assertRaisesRegex(RuntimeError, "could not be read"):
            save_bronze_response(
                connection,
                bronze_request_key="missing",
                product_id="BTC-USD",
                granularity_seconds=3600,
                start=start,
                end=end,
                response=response,
            )
