import json
import unittest
from email.message import Message
from io import BytesIO
from datetime import datetime, timedelta, timezone
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlsplit

from dags.coinbase_client import CoinbaseAPIError, fetch_candles


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
        with patch("dags.coinbase_client.urlopen", return_value=_Response(raw_body)) as urlopen:
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

        with patch("dags.coinbase_client.urlopen", side_effect=error):
            response = fetch_candles(
                product_id="UNKNOWN-USD",
                granularity_seconds=3600,
                start=self.start,
                end=self.end,
            )

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.body, '{"message":"NotFound"}')
        with self.assertRaisesRegex(CoinbaseAPIError, "HTTP 404: NotFound"):
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


if __name__ == "__main__":
    unittest.main()
