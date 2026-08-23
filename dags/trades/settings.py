"""Initial configuration for the Coinbase trades ingestion release."""

MAX_LIVE_PAGES_PER_RUN = 3
# Four sequential requests fit within the one-minute task timeout at the
# Coinbase client's ten-second request timeout, with room for database work.
MAX_CATCHUP_PAGES_PER_RUN = 4
TASK_TIMEOUT_MINUTES = 1
TRADES_API_POOL = "coinbase_trades_api"
