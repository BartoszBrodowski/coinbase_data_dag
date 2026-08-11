"""Initial configuration for the Coinbase trades ingestion release."""


ACTIVE_PRODUCT_IDS = frozenset({"BTC-USD"})
MAX_PAGES_PER_RUN = 3
TASK_TIMEOUT_MINUTES = 1
TRADES_API_POOL = "coinbase_trades_api"


def require_active_product(product_id):
    if product_id not in ACTIVE_PRODUCT_IDS:
        raise ValueError(
            f"trade ingestion is not enabled for {product_id}; "
            f"approved products: {', '.join(sorted(ACTIVE_PRODUCT_IDS))}"
        )
