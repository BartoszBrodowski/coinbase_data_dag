import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


DAGS_DIRECTORY = Path(__file__).parents[2] / "dags"
sys.path.insert(0, str(DAGS_DIRECTORY))

from shared.coinbase_client import CoinbaseResponse
from shared.validation import require_active_product
from trades.bronze_ingest import ingest_trade_page, save_trade_page


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
    def __init__(self, results=((91,),)):
        self.cursor_instance = _Cursor(results)
        self.committed = False

    def __enter__(self):
        return self

    def __exit__(self, exception_type, *_):
        self.committed = exception_type is None

    def cursor(self):
        return self.cursor_instance


class TradeBronzeIngestTest(unittest.TestCase):
    def test_rejects_products_not_approved_for_initial_release(self):
        with self.assertRaisesRegex(ValueError, "not enabled for ETH-USD"):
            require_active_product("ETH-USD")

    def test_saves_page_and_pagination_metadata_in_one_transaction(self):
        response = CoinbaseResponse(
            status_code=200,
            body='[{"trade_id": 1068720622}]',
            content_type="application/json",
            headers={"CB-BEFORE": "1068720622", "CB-AFTER": "1068720621"},
            duration_ms=125,
        )
        connection = _Connection()

        ingestion_id = save_trade_page(
            connection,
            bronze_request_key="trade-page-1",
            process_key="live:coinbase:BTC-USD",
            product_id="BTC-USD",
            response=response,
            requested_after="1068720000",
        )

        _, parameters = connection.cursor_instance.executions[0]
        self.assertEqual(ingestion_id, 91)
        self.assertTrue(connection.committed)
        self.assertEqual(
            parameters["process_key"],
            "live:coinbase:BTC-USD",
        )
        self.assertEqual(parameters["response_cb_before"], "1068720622")
        self.assertEqual(parameters["response_cb_after"], "1068720621")
        self.assertIsNone(parameters["requested_before"])
        self.assertEqual(parameters["requested_after"], "1068720000")
        self.assertEqual(json.loads(parameters["response_headers"]), response.headers)

    def test_saves_requested_before_cursor(self):
        response = CoinbaseResponse(200, "[]", "application/json", {}, 10)
        connection = _Connection()

        save_trade_page(
            connection,
            bronze_request_key="trade-page-before-1",
            process_key="live:coinbase:BTC-USD",
            product_id="BTC-USD",
            response=response,
            requested_before="1068720623",
        )

        _, parameters = connection.cursor_instance.executions[0]
        self.assertEqual(parameters["requested_before"], "1068720623")
        self.assertIsNone(parameters["requested_after"])

    def test_fetches_before_opening_database_connection(self):
        events = []
        response = CoinbaseResponse(
            status_code=200,
            body="[]",
            content_type="application/json",
            headers={"CB-BEFORE": "106", "CB-AFTER": "105"},
            duration_ms=10,
        )
        connection = _Connection(results=((91,), ("106", "105")))

        class _ConnectionContext:
            def __enter__(self):
                events.append("open")
                return connection

            def __exit__(self, *_):
                events.append("close")

        def fetch_page(**_):
            events.append("fetch")
            return response

        with patch(
            "trades.bronze_ingest.fetch_trades",
            side_effect=fetch_page,
        ):
            with patch(
                "trades.bronze_ingest.market_data_connection",
                return_value=_ConnectionContext(),
            ):
                result = ingest_trade_page(
                    bronze_request_key="trade-page-2",
                    process_key="live:coinbase:BTC-USD",
                    product_id="BTC-USD",
                    before="107",
                )

        self.assertEqual(events, ["fetch", "open", "close"])
        self.assertEqual(
            result,
            {
                "ingestion_id": 91,
                "response_cb_before": "106",
                "response_cb_after": "105",
            },
        )

    def test_reuses_existing_page_for_the_same_bronze_request_key(self):
        response = CoinbaseResponse(200, "[]", "application/json", {}, 10)
        connection = _Connection(results=(None, (91,)))

        ingestion_id = save_trade_page(
            connection,
            bronze_request_key="trade-page-1",
            process_key="live:coinbase:BTC-USD",
            product_id="BTC-USD",
            response=response,
        )

        self.assertEqual(ingestion_id, 91)
        self.assertEqual(len(connection.cursor_instance.executions), 2)
