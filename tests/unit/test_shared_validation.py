import sys
import unittest
from pathlib import Path


DAGS_DIRECTORY = Path(__file__).parents[2] / "dags"
sys.path.insert(0, str(DAGS_DIRECTORY))

from shared.validation import require_active_product


class SharedValidationTest(unittest.TestCase):
    def test_accepts_an_active_product(self):
        require_active_product("BTC-USD")

    def test_reports_the_approved_products(self):
        with self.assertRaisesRegex(
            ValueError,
            "market-data ingestion is not enabled for ETH-USD; "
            "approved products: BTC-USD",
        ):
            require_active_product("ETH-USD")


if __name__ == "__main__":
    unittest.main()
