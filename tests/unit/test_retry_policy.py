import sys
import unittest
from pathlib import Path

from airflow.sdk.exceptions import AirflowFailException


DAGS_DIRECTORY = Path(__file__).parents[2] / "dags"
sys.path.insert(0, str(DAGS_DIRECTORY))

from dags.shared.coinbase_client import CoinbaseAPIError
from dags.shared.coinbase_client import (
    CoinbaseNonRetryableAPIError,
)
from dags.shared.utils import fail_without_retry_on


class RetryPolicyTest(unittest.TestCase):
    def test_contract_errors_fail_without_airflow_retry(self):
        with self.assertRaises(AirflowFailException):
            with fail_without_retry_on(CoinbaseNonRetryableAPIError, ValueError):
                raise ValueError("trade payload violates its contract")

    def test_non_retryable_http_errors_fail_without_airflow_retry(self):
        with self.assertRaises(AirflowFailException):
            with fail_without_retry_on(CoinbaseNonRetryableAPIError, ValueError):
                raise CoinbaseNonRetryableAPIError(404, "NotFound")

    def test_retryable_http_errors_escape_for_airflow_retry(self):
        with self.assertRaises(CoinbaseAPIError):
            with fail_without_retry_on(CoinbaseNonRetryableAPIError, ValueError):
                raise CoinbaseAPIError(503, "Unavailable")

    def test_timeouts_escape_for_airflow_retry(self):
        with self.assertRaises(TimeoutError):
            with fail_without_retry_on(CoinbaseNonRetryableAPIError, ValueError):
                raise TimeoutError("request timed out")


if __name__ == "__main__":
    unittest.main()
