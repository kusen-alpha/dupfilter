from __future__ import annotations

import random
import time
from typing import Any


class CircuitBreaker:
    """Fail-fast helper that opens after repeated failures.

    While open, retries are skipped. The breaker becomes half-open
    after ``reset_timeout`` seconds and allows a single probe.
    """

    def __init__(self, failure_threshold: int = 5,
                 reset_timeout: float = 30.0) -> None:
        if failure_threshold <= 0:
            raise ValueError("failure_threshold must be > 0")
        if reset_timeout <= 0:
            raise ValueError("reset_timeout must be > 0")
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self._failures = 0
        self._opened_at: float | None = None

    @property
    def failures(self) -> int:
        return self._failures

    def record_failure(self) -> None:
        self._failures += 1
        if self._opened_at is None and self._failures >= self.failure_threshold:
            self._opened_at = time.monotonic()

    def record_success(self) -> None:
        self._failures = 0
        self._opened_at = None

    def is_open(self) -> bool:
        if self._opened_at is None:
            return False
        if time.monotonic() - self._opened_at >= self.reset_timeout:
            self._opened_at = None  # half-open: allow a probe
            return False
        return True


class RetryingFilter:
    """Retry wrapper around a filter's core operations.

    Each call goes through the wrapped filter with ``raise_on_error=True``
    so transient failures are visible; failed attempts are retried with
    exponential backoff. When the attempts are exhausted or the circuit
    breaker is open, the call falls back to the filter's normal
    fail-soft behavior (or re-raises when ``raise_on_error`` is set).
    """

    def __init__(self, flt: Any, attempts: int = 3, backoff: float = 0.05,
                 max_backoff: float = 1.0, multiplier: float = 2.0,
                 jitter: float = 0.0,
                 breaker: CircuitBreaker | None = None,
                 raise_on_error: bool = False) -> None:
        if attempts <= 0:
            raise ValueError("attempts must be > 0")
        if backoff < 0 or max_backoff < 0 or multiplier <= 1 or jitter < 0:
            raise ValueError("invalid backoff parameters")
        self._flt = flt
        self._attempts = attempts
        self._backoff = backoff
        self._max_backoff = max_backoff
        self._multiplier = multiplier
        self._jitter = jitter
        self._breaker = breaker
        self._raise_on_error = raise_on_error

    def _wait(self, attempt: int) -> None:
        delay = min(self._backoff * (self._multiplier ** (attempt - 1)),
                    self._max_backoff)
        if self._jitter:
            delay += random.uniform(0.0, self._jitter)
        time.sleep(delay)

    def _fallback(self, method: str, value: Any) -> Any:
        return getattr(self._flt, method)(value)

    def _call(self, method: str, value: Any) -> Any:
        if self._breaker is not None and self._breaker.is_open():
            return self._fallback(method, value)
        last_error: Exception | None = None
        for attempt in range(1, self._attempts + 1):
            try:
                result = getattr(self._flt, method)(
                    value, raise_on_error=True)
                if self._breaker is not None:
                    self._breaker.record_success()
                return result
            except Exception as error:
                last_error = error
                if self._breaker is not None:
                    self._breaker.record_failure()
                    if self._breaker.is_open():
                        break
                if attempt < self._attempts:
                    self._wait(attempt)
        if self._raise_on_error and last_error is not None:
            raise last_error
        return self._fallback(method, value)

    def exists(self, value: Any) -> Any:
        return self._call("exists", value)

    def exists_many(self, values: Any) -> Any:
        return self._call("exists_many", values)

    def insert(self, value: Any) -> Any:
        return self._call("insert", value)

    def insert_many(self, values: Any) -> Any:
        return self._call("insert_many", values)

    def exists_and_insert(self, value: Any) -> Any:
        return self._call("exists_and_insert", value)

    def exists_and_insert_many(self, values: Any) -> Any:
        return self._call("exists_and_insert_many", values)

    def close(self) -> None:
        close = getattr(self._flt, "close", None)
        if close is not None:
            close()
