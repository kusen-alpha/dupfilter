# -*-coding:utf-8 -*-
import asyncio
import unittest

from dupfilter.filters.redis.bloomnative import (
    AsyncRedisBloomNativeFilter,
    RedisBloomNativeFilter,
)

from .helpers import quiet_logger


class FakeSyncServer:
    def __init__(self):
        self.calls = []

    def execute_command(self, *args):
        self.calls.append(args)
        name = args[0]
        if name == "BF.EXISTS":
            return 1
        if name == "BF.MEXISTS":
            return [1, 0]
        if name == "BF.ADD":
            return 0
        if name == "BF.MADD":
            return [0, 1]
        if name == "BF.INFO":
            return ["Capacity", 100000, "NumberOfItemsInserted", 42]
        return None


class RedisBloomNativeFilterTests(unittest.TestCase):
    def setUp(self):
        self.server = FakeSyncServer()
        self.flt = RedisBloomNativeFilter(
            self.server, "bf", logger=quiet_logger())

    def test_reserve_on_init(self):
        self.assertEqual(self.server.calls[0][0:3],
                         ("BF.RESERVE", "bf", 0.01))
        self.assertIn("EXPANSION", self.server.calls[0])

    def test_auto_create_false_skips_reserve(self):
        server = FakeSyncServer()
        RedisBloomNativeFilter(
            server, "bf", auto_create=False, logger=quiet_logger())
        self.assertEqual(server.calls, [])

    def test_reserve_ignores_existing_key(self):
        server = FakeSyncServer()

        def raising(*args):
            raise Exception("item exists")

        server.execute_command = raising
        RedisBloomNativeFilter(server, "bf", logger=quiet_logger())

    def test_exists_maps_to_bf_exists(self):
        self.assertTrue(self.flt.exists("a"))
        self.assertEqual(self.server.calls[-1][0:3],
                         ("BF.EXISTS", "bf", "a"))

    def test_exists_and_insert_maps_bf_add(self):
        self.assertTrue(self.flt.exists_and_insert("a"))
        self.assertEqual(self.server.calls[-1][0:3], ("BF.ADD", "bf", "a"))

    def test_exists_and_insert_many(self):
        self.assertEqual(
            self.flt.exists_and_insert_many(["a", "b"]), [True, False])

    def test_count_and_info(self):
        self.assertEqual(self.flt.count(), 42)
        self.assertEqual(
            self.flt.info()["NumberOfItemsInserted"], 42)

    def test_release_not_supported(self):
        with self.assertRaises(NotImplementedError):
            self.flt.release("a")


class FakeAsyncServer:
    def __init__(self):
        self.calls = []

    async def execute_command(self, *args):
        self.calls.append(args)
        name = args[0]
        if name == "BF.EXISTS":
            return 1
        if name == "BF.ADD":
            return 0
        if name == "BF.MADD":
            return [0, 1]
        if name == "BF.INFO":
            return ["Capacity", 100000, "NumberOfItemsInserted", 42]
        return None


class AsyncRedisBloomNativeFilterTests(unittest.TestCase):
    def test_async_reserves_lazily(self):
        server = FakeAsyncServer()
        flt = AsyncRedisBloomNativeFilter(
            server, "bf", logger=quiet_logger())
        self.assertTrue(asyncio.run(flt.exists_and_insert("a")))
        self.assertEqual(server.calls[0][0], "BF.RESERVE")
        self.assertEqual(server.calls[1][0:3], ("BF.ADD", "bf", "a"))

    def test_async_info_and_count(self):
        server = FakeAsyncServer()
        flt = AsyncRedisBloomNativeFilter(
            server, "bf", logger=quiet_logger())
        self.assertEqual(asyncio.run(flt.count()), 42)
        self.assertEqual(
            asyncio.run(flt.info())["NumberOfItemsInserted"], 42)


if __name__ == "__main__":
    unittest.main()
