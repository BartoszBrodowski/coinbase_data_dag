"""Small Coinbase Exchange client shared by market-data ingestion modules."""

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
COINBASE_SOURCE = "coinbase"
SUPPORTED_GRANULARITIES = frozenset({60, 300, 900, 3600, 21600, 86400})
MAX_CANDLES_PER_REQUEST = 300
MAX_TRADES_PER_REQUEST = 1_000
_PRODUCT_ID_PATTERN = re.compile(r"^[A-Z0-9]+-[A-Z0-9]+$")
_STORED_RESPONSE_HEADERS = ("Retry-After", "CB-BEFORE", "CB-AFTER")
_REQUEST_TIMEOUT_SECONDS = 10


class CoinbaseAPIError(RuntimeError):
    """An error response returned by Coinbase."""

    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        super().__init__(f"Coinbase returned HTTP {status_code}: {message}")


class CoinbaseNonRetryableAPIError(CoinbaseAPIError):
    """A deterministic Coinbase response that should fail immediately."""


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
            payload = json.loads(self.body)
        except json.JSONDecodeError:
            payload = None

        if isinstance(payload, dict):
            message = payload.get("message", self.body)
        else:
            message = self.body

        if self.status_code == 429 or 500 <= self.status_code < 600:
            raise CoinbaseAPIError(self.status_code, message)

        raise CoinbaseNonRetryableAPIError(self.status_code, message)


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

    _validate_candle_request(product_id, granularity_seconds, start, end)

    return _fetch(
        f"/products/{product_id}/candles",
        {
            "start": _format_utc(start),
            "end": _format_utc(end),
            "granularity": granularity_seconds,
        },
    )


def fetch_trades(
    *,
    product_id: str,
    before: str | None = None,
    after: str | None = None,
    limit: int = MAX_TRADES_PER_REQUEST,
) -> CoinbaseResponse:
    """Fetch one cursor-paginated page of product trades."""

    _validate_trade_request(product_id, before, after, limit)

    query = {"limit": limit}
    if before is not None:
        query["before"] = before
    if after is not None:
        query["after"] = after

    return _fetch(f"/products/{product_id}/trades", query)


def _fetch(path, query) -> CoinbaseResponse:
    request = Request(
        f"{COINBASE_API_URL}{path}?{urlencode(query)}",
        headers={"Accept": "application/json", "User-Agent": "coinbase-client/1.0"},
    )

    request_started = time.perf_counter()
    try:
        with urlopen(request, timeout=_REQUEST_TIMEOUT_SECONDS) as response:
            body = response.read().decode("utf-8")
            return _build_response(response.status, body, response.headers, request_started)
    except HTTPError as error:
        with error:
            body = error.read().decode("utf-8")
            return _build_response(error.code, body, error.headers, request_started)


def _build_response(status_code, body, headers, request_started) -> CoinbaseResponse:
    stored_headers = {}
    # Only save the useful headers relevant to ingestion logic
    for header_name in _STORED_RESPONSE_HEADERS:
        value = headers.get(header_name)
        if value:
            stored_headers[header_name] = value

    return CoinbaseResponse(
        status_code=status_code,
        body=body,
        content_type=headers.get("Content-Type"),
        headers=stored_headers,
        duration_ms=round((time.perf_counter() - request_started) * 1000),
    )


def _validate_candle_request(
    product_id: str,
    granularity_seconds: int,
    start: datetime,
    end: datetime,
) -> None:
    _validate_product_id(product_id)
    if granularity_seconds not in SUPPORTED_GRANULARITIES:
        raise ValueError("unsupported Coinbase candle granularity")
    if start.tzinfo is None or end.tzinfo is None:
        raise ValueError("start and end must be timezone-aware")
    if start >= end:
        raise ValueError("start must be earlier than end")

    requested_seconds = (end - start).total_seconds()
    maximum_seconds = granularity_seconds * MAX_CANDLES_PER_REQUEST
    
    if requested_seconds > maximum_seconds:
        raise ValueError("requested interval exceeds Coinbase's 300-candle limit")

def _validate_trade_request(
    product_id: str,
    before: str | None,
    after: str | None,
    limit: int,
) -> None:
    _validate_product_id(product_id)
    if before is not None and after is not None:
        raise ValueError("before and after cursors cannot be used together")
    if not isinstance(limit, int) or not 1 <= limit <= MAX_TRADES_PER_REQUEST:
        raise ValueError("trade limit must be between 1 and 1000")


def _validate_product_id(product_id: str) -> None:
    if not _PRODUCT_ID_PATTERN.fullmatch(product_id):
        raise ValueError("product_id must look like 'BTC-USD'")


def _format_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
