"""Initial configuration for the Coinbase trades ingestion release."""

MAX_LIVE_PAGES_PER_RUN = 3
# Four sequential requests fit within the one-minute task timeout at the
# Coinbase client's ten-second request timeout, with room for database work.
CATCHUP_PAGES_PER_BATCH = 4
DEFAULT_CATCHUP_PAGE_AMOUNT = 100
MAX_CATCHUP_PAGE_AMOUNT = 1_000
TASK_TIMEOUT_MINUTES = 1
TRADES_API_POOL = "coinbase_trades_api"

# Gap repair intentionally ignores recent data that a live ingestion may still
# be filling and scans only a bounded operational window.
GAP_REPAIR_LOOKBACK_DAYS = 7
GAP_REPAIR_RECENT_DATA_MINUTES = 30
GAP_REPAIR_MINIMUM_SECONDS = 30
GAP_REPAIR_COMPLETION_POKE_SECONDS = 60
GAP_REPAIR_COMPLETION_TIMEOUT_HOURS = 24
