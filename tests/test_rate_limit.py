# -*-coding:utf-8 -*-
import asyncio
import time
import unittest

from dupfilter.filters.rate_limit import (
    AsyncRedisSlidingWindowLimiter,
    AsyncSlidingWindowLimiter,
    RedisSlidingWindowLimiter,
    SlidingWindowLimiter,
)


class RecordingServer:
    def __init__(self, result=1):
        self.result = result
        self.sources = []
        self.calls = []

    def register_script(self, source):
        self.sources.append(source)

        def script(**kwargs):
            self.calls.append(kwargs)
            return self.result

        return script


class AsyncRecordingServer:
    def __init__(self, result=1):
        self.result = result
        self.sources = []
        self.calls = []

    def register_script(self, source):
        self.sources.append(source)

        async def script(**kwargs):
            self.calls.append(kwargs)
            return self.result

        return script


class SlidingWindowLimiterTests(unittest.TestCase):
    def test_allow_until_limit(self):
        limiter = SlidingWindowLimiter(limit=2, window=10)
        self.assertTrue(limiter.allow("u"))
        self.assertTrue(limiter.allow("u"))
        self.assertFalse(limiter.allow("u"))
        self.assertEqual(limiter.remaining("u"), 0)
        self.assertTrue(limiter.allow("other"))

    def test_window_slides(self):
        limiter = SlidingWindowLimiter(limit=2, window=0.05)
        self.assertTrue(limiter.allow("u"))
        self.assertTrue(limiter.allow("u"))
        time.sleep(0.08)
        self.assertTrue(limiter.allow("u"))

    def test_cost_counts(self):
        limiter = SlidingWindowLimiter(limit=3, window=10)
        self.assertFalse(limiter.allow("u", cost=4))
        self.assertTrue(limiter.allow("u", cost=3))
        self.assertEqual(limiter.count("u"), 3)

    def test_clear(self):
        limiter = SlidingWindowLimiter(limit=1, window=10)
        limiter.allow("u")
        limiter.clear()
        self.assertTrue(limiter.allow("u"))

    def test_validation(self):
        with self.assertRaises(ValueError):
            SlidingWindowLimiter(limit=0, window=10)
        with self.assertRaises(ValueError):
            SlidingWindowLimiter(limit=1, window=0)


class RedisSlidingWindowLimiterTests(unittest.TestCase):
    def test_allow_and_remaining(self):
        server = RecordingServer(result=1)
        limiter = RedisSlidingWindowLimiter(server, limit=3, window=10)
        self.assertTrue(limiter.allow("u"))
        self.assertEqual(limiter.remaining("u"), 1)
        self.assertEqual(len(server.sources), 2)
        self.assertIn("ZREMRANGEBYSCORE", server.sources[0])

    def test_blocked_when_over_limit(self):
        server = RecordingServer(result=0)
        limiter = RedisSlidingWindowLimiter(server, limit=1, window=10)
        self.assertFalse(limiter.allow("u"))

    def test_key_namespace(self):
        server = RecordingServer(result=1)
        limiter = RedisSlidingWindowLimiter(
            server, limit=1, window=10, key="ns")
        limiter.allow("u")
        self.assertEqual(server.calls[0]["keys"], ["ns:u"])


class AsyncLimiterTests(unittest.TestCase):
    def test_async_memory(self):
        limiter = AsyncSlidingWindowLimiter(limit=1, window=10)

        async def run():
            self.assertTrue(await limiter.allow("u"))
            self.assertFalse(await limiter.allow("u"))
            self.assertEqual(await limiter.remaining("u"), 0)

        asyncio.run(run())

    def test_async_redis(self):
        server = AsyncRecordingServer(result=1)
        limiter = AsyncRedisSlidingWindowLimiter(server, limit=2, window=10)

        async def run():
            self.assertTrue(await limiter.allow("u"))
            self.assertEqual(await limiter.remaining("u"), 1)
            self.assertEqual(await limiter.count("u"), 1)

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
