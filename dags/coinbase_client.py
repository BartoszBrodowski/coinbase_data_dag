"""Minimal Coinbase Exchange client for fetching one explicit candle range."""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


COINBASE_API_URL = "https://api.exchange.coinbase.com"
SUPPORTED_GRANULARITIES = frozenset({60, 300, 900, 3600, 21600, 86400})
MAX_CANDLES_PER_REQUEST = 300
_PRODUCT_ID_PATTERN = re.compile(r"^[A-Z0-9]+-[A-Z0-9]+$")


class CoinbaseAPIError(RuntimeError):
    """An error response returned by Coinbase."""


@dataclass(frozen=True)
class CoinbaseResponse:
    status_code: int
    body: str
    content_type: str | None
    headers: dict[str, str]
    duration_ms: int

    def raise_for_status(self) -> None:
        if self.status_code < 400:
            return

        try:
            message = json.loads(self.body).get("message", self.body)
        except (json.JSONDecodeError, AttributeError):
            message = self.body

        raise CoinbaseAPIError(f"Coinbase returned HTTP {self.status_code}: {message}")


def fetch_candles(
    *,
    product_id: str,
    granularity_seconds: int,
    start: datetime,
    end: datetime,
) -> CoinbaseResponse:
    """Fetch candles for one caller-supplied UTC interval.

    ``start`` and ``end`` are mandatory on purpose: ingestion must never fall
    back to Coinbase's implicit range ending at the current time.
    """

    _validate_request(product_id, granularity_seconds, start, end)

    query = urlencode(
        {
            "start": _format_utc(start),
            "end": _format_utc(end),
            "granularity": granularity_seconds,
        }
    )
    request = Request(
        f"{COINBASE_API_URL}/products/{product_id}/candles?{query}",
        headers={"Accept": "application/json", "User-Agent": "coinbase-client/1.0"},
    )

    request_started = time.perf_counter()
    try:
        with urlopen(request, timeout=10) as response:
            body = response.read().decode("utf-8")
            return _build_response(response.status, body, response.headers, request_started)
    except HTTPError as error:
        with error:
            body = error.read().decode("utf-8")
            return _build_response(error.code, body, error.headers, request_started)


def _build_response(status_code, body, headers, request_started) -> CoinbaseResponse:
    stored_headers = {}
    if headers.get("Retry-After"):
        stored_headers["Retry-After"] = headers["Retry-After"]

    return CoinbaseResponse(
        status_code=status_code,
        body=body,
        content_type=headers.get("Content-Type"),
        headers=stored_headers,
        duration_ms=round((time.perf_counter() - request_started) * 1000),
    )


def _validate_request(
    product_id: str,
    granularity_seconds: int,
    start: datetime,
    end: datetime,
) -> None:
    if not _PRODUCT_ID_PATTERN.fullmatch(product_id):
        raise ValueError("product_id must look like 'BTC-USD'")
    if granularity_seconds not in SUPPORTED_GRANULARITIES:
        raise ValueError("unsupported Coinbase candle granularity")
    if start.tzinfo is None or end.tzinfo is None:
        raise ValueError("start and end must be timezone-aware")
    if start >= end:
        raise ValueError("start must be earlier than end")

    requested_candles = (end - start).total_seconds() / granularity_seconds
    if requested_candles > MAX_CANDLES_PER_REQUEST:
        raise ValueError("requested interval exceeds Coinbase's 300-candle limit")


def _format_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
