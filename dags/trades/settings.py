"""Explicitly approved products for the initial trades ingestion release."""


ACTIVE_PRODUCT_IDS = frozenset({"BTC-USD"})


def require_active_product(product_id):
    if product_id not in ACTIVE_PRODUCT_IDS:
        raise ValueError(
            f"trade ingestion is not enabled for {product_id}; "
            f"approved products: {', '.join(sorted(ACTIVE_PRODUCT_IDS))}"
        )
