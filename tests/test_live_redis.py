import asyncio
import os
import unittest

from dupfilter.filters.redis.bloomfilter import AsyncRedisBloomFilter, RedisBloomFilter
from dupfilter.filters.redis.setfilter import AsyncRedisSetFilter, RedisSetFilter
from dupfilter.filters.redis.sortedsetfilter import (
    AsyncRedisSortedSetFilter,
    RedisSortedSetFilter,
)
from dupfilter.filters.redis.stringfilter import (
    AsyncRedisStringFilter,
    RedisStringFilter,
)

REDIS_URL = os.environ.get("DUPFILTER_REDIS_URL", "")


@unittest.skipUnless(REDIS_URL, "set DUPFILTER_REDIS_URL to run live Redis tests")
class LiveRedisTests(unittest.TestCase):
    def setUp(self):
        import redis

        self.server = redis.Redis.from_url(REDIS_URL)
        self.server.flushdb()

    def tearDown(self):
        self.server.flushdb()
        self.server.close()

    def test_set_filter_roundtrip(self):
        flt = RedisSetFilter(self.server, "dup:set")
        self.assertEqual(
            flt.exists_and_insert_many(["a", "b", "c"]),
            [False, False, False])
        self.assertEqual(
            flt.exists_and_insert_many(["a", "b", "d"]),
            [True, True, False])

    def test_bloom_filter_roundtrip_with_auto_params(self):
        flt = RedisBloomFilter(
            self.server, "dup:bf",
            expected_items=1000, false_positive_rate=0.01)
        self.assertEqual(flt.exists_many(["1", "2", "3"]),
                         [False, False, False])
        flt.insert_many(["1", "2", "3"])
        self.assertEqual(flt.exists_many(["1", "2", "3"]),
                         [True, True, True])

    def test_string_filter_lock_and_confirm(self):
        flt = RedisStringFilter(self.server)
        self.assertFalse(flt.exists_and_lock("k", expire=3600))
        self.assertTrue(flt.exists("k", expire=3600))
        self.assertTrue(flt.confirm("k", expire=3600))

    def test_set_two_phase_roundtrip(self):
        flt = RedisSetFilter(self.server, "dup:set2p")
        self.assertFalse(flt.exists_and_lock("a"))
        self.assertTrue(flt.exists_and_lock("a"))
        self.assertTrue(flt.confirm("a"))
        self.assertTrue(flt.release("a"))
        self.assertFalse(flt.exists("a"))

    def test_sorted_set_two_phase_roundtrip(self):
        flt = RedisSortedSetFilter(self.server, "dup:zset2p")
        self.assertFalse(flt.exists_and_lock("a"))
        self.assertTrue(flt.exists_and_lock("a"))
        self.assertTrue(flt.confirm("a"))
        self.assertTrue(flt.release("a"))
        self.assertFalse(flt.exists("a"))


@unittest.skipUnless(REDIS_URL, "set DUPFILTER_REDIS_URL to run live Redis tests")
class LiveAsyncRedisTests(unittest.TestCase):
    def setUp(self):
        import redis.asyncio as aioredis

        self._aioredis = aioredis
        self.server = None

    def tearDown(self):
        if self.server is not None:
            try:
                asyncio.run(self.server.aclose())
            except Exception:
                pass

    def _run(self, coro):
        async def _main():
            try:
                return await coro
            finally:
                if self.server is not None:
                    await self.server.aclose()
                    self.server = None

        return asyncio.run(_main())

    async def _make(self):
        self.server = self._aioredis.Redis.from_url(REDIS_URL)
        await self.server.flushdb()
        return self.server

    def test_async_set_roundtrip(self):
        async def case():
            server = await self._make()
            flt = AsyncRedisSetFilter(server, "dup:aset")
            self.assertEqual(
                await flt.exists_and_insert_many(["a", "b", "c"]),
                [False, False, False])
            self.assertEqual(
                await flt.exists_and_insert_many(["a", "b", "d"]),
                [True, True, False])

        self._run(case())

    def test_async_bloom_roundtrip_with_auto_params(self):
        async def case():
            server = await self._make()
            flt = AsyncRedisBloomFilter(
                server, "dup:abf",
                expected_items=1000, false_positive_rate=0.01)
            self.assertEqual(
                await flt.exists_many(["1", "2", "3"]),
                [False, False, False])
            await flt.insert_many(["1", "2", "3"])
            self.assertEqual(
                await flt.exists_many(["1", "2", "3"]),
                [True, True, True])

        self._run(case())

    def test_async_string_lock_confirm(self):
        async def case():
            server = await self._make()
            flt = AsyncRedisStringFilter(server)
            self.assertFalse(await flt.exists_and_lock("k", expire=3600))
            self.assertTrue(await flt.exists("k", expire=3600))
            self.assertTrue(await flt.confirm("k", expire=3600))

        self._run(case())

    def test_async_sorted_set_two_phase(self):
        async def case():
            server = await self._make()
            flt = AsyncRedisSortedSetFilter(server, "dup:azset2p")
            self.assertFalse(await flt.exists_and_lock("a"))
            self.assertTrue(await flt.exists_and_lock("a"))
            self.assertTrue(await flt.confirm("a"))
            self.assertTrue(await flt.release("a"))
            self.assertFalse(await flt.exists("a"))

        self._run(case())
