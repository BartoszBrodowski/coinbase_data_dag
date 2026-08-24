"""Initial configuration for the Coinbase trades ingestion release."""

MAX_LIVE_PAGES_PER_RUN = 3
# Four sequential requests fit within the one-minute task timeout at the
# Coinbase client's ten-second request timeout, with room for database work.
CATCHUP_PAGES_PER_BATCH = 4
DEFAULT_CATCHUP_PAGE_AMOUNT = 100
MAX_CATCHUP_PAGE_AMOUNT = 1_000
TASK_TIMEOUT_MINUTES = 1
TRADES_API_POOL = "coinbase_trades_api"
