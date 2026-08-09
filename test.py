"""Manually inspect one small Coinbase candle response."""

import json
from datetime import datetime, timezone

from dags.coinbase_client import fetch_candles
from dags.bronze_ingest import ingest_one_interval


PRODUCT_ID = "BTC-USD"
GRANULARITY_SECONDS = 3600
START = datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)
END = datetime(2025, 1, 1, 6, 0, tzinfo=timezone.utc)


def main():
    print("Coinbase candle request")
    print(f"  Product:     {PRODUCT_ID}")
    print(f"  Granularity: {GRANULARITY_SECONDS} seconds")
    print(f"  Start:       {START.isoformat()}")
    print(f"  End:         {END.isoformat()}")

    response = fetch_candles(
        product_id=PRODUCT_ID,
        granularity_seconds=GRANULARITY_SECONDS,
        start=START,
        end=END,
    )

    print("\nHTTP response")
    print(f"  Status:       {response.status_code}")
    print(f"  Content-Type: {response.content_type}")
    print(f"  Duration:     {response.duration_ms} ms")
    print(f"  Headers:      {response.headers or '{}'}")

    print("\nRaw response body")
    try:
        payload = json.loads(response.body)
        print(json.dumps(payload, indent=2))
    except json.JSONDecodeError:
        print(response.body)
        response.raise_for_status()
        return

    response.raise_for_status()

    print(f"\nCandles returned: {len(payload)}")
    print(
        f"{'UTC candle start':<22} "
        f"{'Open':>12} {'High':>12} {'Low':>12} {'Close':>12} {'Volume':>16}"
    )
    print("-" * 92)

    for timestamp, low, high, opening, close, volume in payload:
        candle_start = datetime.fromtimestamp(timestamp, timezone.utc)
        print(
            f"{candle_start:%Y-%m-%d %H:%M:%S}Z "
            f"{opening:>12} {high:>12} {low:>12} {close:>12} {volume:>16}"
        )

if __name__ == "__main__":
    main()
