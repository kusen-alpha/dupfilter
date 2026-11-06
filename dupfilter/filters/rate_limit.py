# -*-coding:utf-8 -*-
from __future__ import annotations

import threading
import time
import uuid
from collections import deque
from collections.abc import Callable
from typing import Any

ALLOW_SCRIPT = """
local key = KEYS[1]
local now = ARGV[1]
local window = ARGV[2]
local limit = tonumber(ARGV[3])
local member = ARGV[4]
redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
redis.call('ZADD', key, 'NX', now, member)
local count = redis.call('ZCARD', key)
if count > limit then
    redis.call('ZREM', key, member)
    return 0
end
return 1
"""

REMAINING_SCRIPT = """
local key = KEYS[1]
local now = ARGV[1]
local window = ARGV[2]
redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
return redis.call('ZCARD', key)
"""


class SlidingWindowLimiter:
    """In-process sliding-window rate limiter.

    ``allow(key)`` is True while fewer than ``limit`` calls happened in the
    last ``window`` seconds. Process-local; use
    :class:`RedisSlidingWindowLimiter` when the limit must be shared.
    """

    def __init__(self, limit: int, window: float) -> None:
        if limit <= 0:
            raise ValueError('limit must be > 0')
        if window <= 0:
            raise ValueError('window must be > 0')
        self.limit = limit
        self.window = window
        self._times: dict[str, deque[float]] = {}
        self._lock = threading.Lock()

    def _prune(self, now: float, queue: deque[float]) -> None:
        while queue and queue[0] <= now - self.window:
            queue.popleft()

    def allow(self, key: str, cost: int = 1) -> bool:
        if cost <= 0:
            raise ValueError('cost must be > 0')
        now = time.monotonic()
        with self._lock:
            queue = self._times.setdefault(key, deque())
            self._prune(now, queue)
            if len(queue) + cost > self.limit:
                return False
            for _ in range(cost):
                queue.append(now)
            return True

    def remaining(self, key: str) -> int:
        with self._lock:
            queue = self._times.get(key)
            if queue is None:
                return self.limit
            self._prune(time.monotonic(), queue)
            return max(0, self.limit - len(queue))

    def count(self, key: str) -> int:
        with self._lock:
            queue = self._times.get(key)
            if queue is None:
                return 0
            self._prune(time.monotonic(), queue)
            return len(queue)

    def clear(self) -> None:
        with self._lock:
            self._times = {}


class RedisSlidingWindowLimiter:
    """Redis-backed sliding-window rate limiter.

    Entries are timestamps in a SortedSet; expired entries are pruned by a
    Lua script on every call, so a key can never grow without bound.
    """

    def __init__(self, server: Any, limit: int, window: float,
                 key: str | None = None, clock: Callable[[], float] | None
                 = None) -> None:
        if limit <= 0:
            raise ValueError('limit must be > 0')
        if window <= 0:
            raise ValueError('window must be > 0')
        self.server = server
        self.limit = limit
        self.window = window
        self.key = key or "dupfilter:rate"
        self._clock = clock or time.time
        self._allow_script = server.register_script(ALLOW_SCRIPT)
        self._remaining_script = server.register_script(REMAINING_SCRIPT)

    def _member(self) -> str:
        return f"{self._clock()}:{uuid.uuid4().hex}"

    def allow(self, key: str, cost: int = 1) -> bool:
        if cost <= 0:
            raise ValueError('cost must be > 0')
        full_key = f"{self.key}:{key}"
        result = True
        for _ in range(cost):
            raw = self._allow_script(
                keys=[full_key],
                args=[self._clock(), self.window, self.limit, self._member()])
            if not raw:
                result = False
                break
        return result

    def remaining(self, key: str) -> int:
        raw = self._remaining_script(
            keys=[f"{self.key}:{key}"],
            args=[self._clock(), self.window])
        return int(raw)

    def count(self, key: str) -> int:
        return self.remaining(key)


class AsyncSlidingWindowLimiter:
    """Async API wrapper over :class:`SlidingWindowLimiter`."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._limiter: Any = SlidingWindowLimiter(*args, **kwargs)

    async def allow(self, key: str, cost: int = 1) -> bool:
        return bool(self._limiter.allow(key, cost))

    async def remaining(self, key: str) -> int:
        return int(self._limiter.remaining(key))

    async def count(self, key: str) -> int:
        return int(self._limiter.count(key))

    async def clear(self) -> None:
        self._limiter.clear()


class AsyncRedisSlidingWindowLimiter:
    """Async version of :class:`RedisSlidingWindowLimiter`."""

    def __init__(self, server: Any, limit: int, window: float,
                 key: str | None = None,
                 clock: Callable[[], float] | None = None) -> None:
        if limit <= 0:
            raise ValueError('limit must be > 0')
        if window <= 0:
            raise ValueError('window must be > 0')
        self.server = server
        self.limit = limit
        self.window = window
        self.key = key or "dupfilter:rate"
        self._clock = clock or time.time
        self._allow_script = server.register_script(ALLOW_SCRIPT)
        self._remaining_script = server.register_script(REMAINING_SCRIPT)

    def _member(self) -> str:
        return f"{self._clock()}:{uuid.uuid4().hex}"

    async def allow(self, key: str, cost: int = 1) -> bool:
        if cost <= 0:
            raise ValueError('cost must be > 0')
        full_key = f"{self.key}:{key}"
        result = True
        for _ in range(cost):
            raw = await self._allow_script(
                keys=[full_key],
                args=[self._clock(), self.window, self.limit, self._member()])
            if not raw:
                result = False
                break
        return result

    async def remaining(self, key: str) -> int:
        raw = await self._remaining_script(
            keys=[f"{self.key}:{key}"],
            args=[self._clock(), self.window])
        return int(raw)

    async def count(self, key: str) -> int:
        return await self.remaining(key)
