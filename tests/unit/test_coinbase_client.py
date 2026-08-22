import json
import sys
import unittest
from email.message import Message
from io import BytesIO
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlsplit

DAGS_DIRECTORY = Path(__file__).parents[2] / "dags"
sys.path.insert(0, str(DAGS_DIRECTORY))

from shared.coinbase_client import (
    CoinbaseNonRetryableAPIError,
    CoinbaseResponse,
    CoinbaseAPIError,
    fetch_candles,
    fetch_trades,
)


def fetch_error_response(status_code):
    return CoinbaseResponse(
        status_code=status_code,
        body='{"message":"failure"}',
        content_type="application/json",
        headers={},
        duration_ms=1,
    )


class _Response:
    def __init__(self, body):
        self._body = body.encode("utf-8")
        self.status = 200
        self.headers = {"Content-Type": "application/json"}

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return None

    def read(self):
        return self._body


class CoinbaseClientTest(unittest.TestCase):
    def setUp(self):
        self.start = datetime(2025, 1, 1, tzinfo=timezone.utc)
        self.end = self.start + timedelta(hours=1)

    def test_fetches_exact_explicit_interval(self):
        fixture = [[1735689600, 92743.63, 94960.91, 93347.59, 94383.59, 6871.73848196]]
        raw_body = json.dumps(fixture)
        with patch("shared.coinbase_client.urlopen", return_value=_Response(raw_body)) as urlopen:
            result = fetch_candles(
                product_id="BTC-USD",
                granularity_seconds=3600,
                start=self.start,
                end=self.end,
            )

        request = urlopen.call_args.args[0]
        parsed_url = urlsplit(request.full_url)
        self.assertEqual(parsed_url.path, "/products/BTC-USD/candles")
        self.assertEqual(
            parse_qs(parsed_url.query),
            {
                "start": ["2025-01-01T00:00:00Z"],
                "end": ["2025-01-01T01:00:00Z"],
                "granularity": ["3600"],
            },
        )
        self.assertEqual(result.status_code, 200)
        self.assertEqual(result.body, raw_body)
        self.assertEqual(result.content_type, "application/json")

    def test_returns_api_error_body_for_bronze_storage(self):
        headers = Message()
        headers["Content-Type"] = "application/json"
        error = HTTPError(
            url="https://api.exchange.coinbase.com/products/UNKNOWN-USD/candles",
            code=404,
            msg="Not Found",
            hdrs=headers,
            fp=BytesIO(b'{"message":"NotFound"}'),
        )

        with patch("shared.coinbase_client.urlopen", side_effect=error):
            response = fetch_candles(
                product_id="UNKNOWN-USD",
                granularity_seconds=3600,
                start=self.start,
                end=self.end,
            )

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.body, '{"message":"NotFound"}')
        with self.assertRaisesRegex(CoinbaseNonRetryableAPIError, "HTTP 404: NotFound"):
            response.raise_for_status()

    def test_marks_rate_limit_and_server_errors_as_retryable(self):
        for status_code in (429, 500, 503):
            with self.subTest(status_code=status_code):
                response = fetch_error_response(status_code)
                with self.assertRaises(CoinbaseAPIError) as raised:
                    response.raise_for_status()
                self.assertEqual(raised.exception.status_code, status_code)

    def test_marks_other_http_failures_as_non_retryable(self):
        for status_code in (400, 401, 404):
            with self.subTest(status_code=status_code):
                response = fetch_error_response(status_code)
                with self.assertRaises(CoinbaseNonRetryableAPIError):
                    response.raise_for_status()

    def test_rejects_implicit_timezone(self):
        with self.assertRaisesRegex(ValueError, "timezone-aware"):
            fetch_candles(
                product_id="BTC-USD",
                granularity_seconds=3600,
                start=datetime(2025, 1, 1),
                end=datetime(2025, 1, 1, 1),
            )

    def test_rejects_more_than_300_candles_before_network_call(self):
        with self.assertRaisesRegex(ValueError, "300-candle"):
            fetch_candles(
                product_id="BTC-USD",
                granularity_seconds=60,
                start=self.start,
                end=self.start + timedelta(minutes=301),
            )

    def test_fetches_one_trade_page_with_cursor_headers(self):
        response = _Response("[]")
        response.headers["CB-BEFORE"] = "1068720622"
        response.headers["CB-AFTER"] = "1068720621"

        with patch("shared.coinbase_client.urlopen", return_value=response) as urlopen:
            result = fetch_trades(product_id="BTC-USD", after="1068720000")

        request = urlopen.call_args.args[0]
        self.assertEqual(urlsplit(request.full_url).path, "/products/BTC-USD/trades")
        self.assertEqual(
            parse_qs(urlsplit(request.full_url).query),
            {"limit": ["1000"], "after": ["1068720000"]},
        )
        self.assertEqual(result.headers["CB-BEFORE"], "1068720622")
        self.assertEqual(result.headers["CB-AFTER"], "1068720621")


if __name__ == "__main__":
    unittest.main()
