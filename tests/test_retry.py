import unittest
from unittest import mock

from dupfilter.retry import CircuitBreaker, RetryingFilter


class FlakyFilter:
    def __init__(self, failures_before_success, fallback=False):
        self.calls = 0
        self.failures_before_success = failures_before_success
        self.fallback = fallback

    def exists(self, value, raise_on_error=False):
        self.calls += 1
        if self.calls <= self.failures_before_success and raise_on_error:
            raise RuntimeError("boom")
        return self.calls > self.failures_before_success or self.fallback


class CircuitBreakerTests(unittest.TestCase):
    def test_opens_after_threshold(self):
        breaker = CircuitBreaker(failure_threshold=2, reset_timeout=60)
        self.assertFalse(breaker.is_open())
        breaker.record_failure()
        self.assertFalse(breaker.is_open())
        breaker.record_failure()
        self.assertTrue(breaker.is_open())

    def test_resets_after_timeout(self):
        breaker = CircuitBreaker(failure_threshold=1, reset_timeout=60)
        with mock.patch("dupfilter.retry.time.monotonic",
                        side_effect=[100.0, 100.0, 161.0]):
            breaker.record_failure()          # opened at t=100
            self.assertTrue(breaker.is_open())  # t=100: still open
            self.assertFalse(breaker.is_open())  # t=161: half-open probe

    def test_success_closes_breaker(self):
        breaker = CircuitBreaker(failure_threshold=2, reset_timeout=60)
        breaker.record_failure()
        breaker.record_failure()
        breaker.record_success()
        self.assertFalse(breaker.is_open())


class RetryingFilterTests(unittest.TestCase):
    def test_retries_until_success(self):
        flt = FlakyFilter(2)
        wrapped = RetryingFilter(flt, attempts=5, backoff=0.0)
        self.assertTrue(wrapped.exists("x"))
        self.assertEqual(flt.calls, 3)

    def test_returns_fallback_when_exhausted(self):
        flt = FlakyFilter(99, fallback=False)
        wrapped = RetryingFilter(flt, attempts=3, backoff=0.0)
        self.assertFalse(wrapped.exists("x"))
        self.assertEqual(flt.calls, 4)  # 3 attempts + 1 fallback call

    def test_raises_when_configured(self):
        flt = FlakyFilter(99)
        wrapped = RetryingFilter(
            flt, attempts=2, backoff=0.0, raise_on_error=True)
        with self.assertRaises(RuntimeError):
            wrapped.exists("x")

    def test_backoff_sleeps_between_attempts(self):
        flt = FlakyFilter(1)
        wrapped = RetryingFilter(
            flt, attempts=3, backoff=0.1, multiplier=2.0)
        with mock.patch("dupfilter.retry.time.sleep") as sleep:
            wrapped.exists("x")
            self.assertEqual(sleep.call_count, 1)
            self.assertAlmostEqual(sleep.call_args[0][0], 0.1, places=2)

    def test_circuit_breaker_skips_retries(self):
        breaker = CircuitBreaker(failure_threshold=2, reset_timeout=60)
        flt = FlakyFilter(99)
        wrapped = RetryingFilter(
            flt, attempts=3, backoff=0.0, breaker=breaker)
        wrapped.exists("x")
        self.assertTrue(breaker.is_open())
        self.assertEqual(flt.calls, 3)  # 2 failed attempts + fallback
        wrapped.exists("x")
        self.assertEqual(flt.calls, 4)  # fail fast, fallback only

    def test_parameter_validation(self):
        with self.assertRaises(ValueError):
            RetryingFilter(object(), attempts=0)
        with self.assertRaises(ValueError):
            RetryingFilter(object(), multiplier=1.0)
        with self.assertRaises(ValueError):
            CircuitBreaker(failure_threshold=0)
