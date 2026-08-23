"""Validation helpers shared by market-data domains."""


ACTIVE_PRODUCT_IDS = frozenset({"BTC-USD"})


def require_active_product(product_id: str) -> None:
    """Reject products that are not enabled for market-data ingestion."""

    if product_id not in ACTIVE_PRODUCT_IDS:
        approved_products = ", ".join(sorted(ACTIVE_PRODUCT_IDS))
        raise ValueError(
            f"market-data ingestion is not enabled for {product_id}; "
            f"approved products: {approved_products}"
        )
