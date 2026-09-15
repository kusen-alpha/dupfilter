# -*- coding:utf-8 -*-
import asyncio
import unittest

from dupfilter import utils
from dupfilter.filters.redis.bloomfilter import (
    AsyncRedisBloomFilter, RedisBloomFilter)
from dupfilter.filters.redis.setfilter import (
    AsyncRedisSetFilter, RedisSetFilter)
from dupfilter.filters.redis.sortedsetfilter import (
    AsyncRedisSortedSetFilter, RedisSortedSetFilter)
from dupfilter.filters.redis.stringfilter import (
    AsyncRedisStringFilter, RedisStringFilter)

from .helpers import quiet_logger


class RecordingAsyncServer(object):
    def __init__(self):
        self.calls = []

    def register_script(self, source):
        async def script(**kwargs):
            self.calls.append(source)
            return [1]
        return script


class RecordingSyncServer(object):
    def __init__(self):
        self.calls = []

    def register_script(self, source):
        def script(**kwargs):
            self.calls.append(source)
            return [1]
        return script


class AsyncRedisFilterTests(unittest.TestCase):
    def test_set_exists_and_insert_reaches_script(self):
        server = RecordingAsyncServer()
        flt = AsyncRedisSetFilter(
            server, "key", logger=quiet_logger())
        self.assertTrue(asyncio.run(flt.exists_and_insert("x")))
        self.assertEqual(len(server.calls), 1)

    def test_sorted_set_exists_and_insert_reaches_script(self):
        server = RecordingAsyncServer()
        flt = AsyncRedisSortedSetFilter(
            server, "key", logger=quiet_logger())
        self.assertTrue(asyncio.run(flt.exists_and_insert("x")))
        self.assertEqual(len(server.calls), 1)

    def test_string_alias_methods_are_awaitable(self):
        server = RecordingAsyncServer()
        flt = AsyncRedisStringFilter(server, logger=quiet_logger())
        self.assertTrue(asyncio.run(flt.exists_and_lock("x")))
        self.assertTrue(asyncio.run(flt.confirm("x")))
        self.assertEqual(len(server.calls), 2)

    def test_keyword_arguments_on_async_methods(self):
        server = RecordingAsyncServer()
        flt = AsyncRedisBloomFilter(
            server, "bf", bit=8, hash_num=2, logger=quiet_logger())
        self.assertTrue(asyncio.run(flt.exists(value="x")))


class RedisBlockTests(unittest.TestCase):
    def setUp(self):
        self.server = RecordingSyncServer()

    def test_block_num_one_has_no_suffix(self):
        flt = RedisSetFilter(self.server, "key")
        self.assertEqual(flt._get_block("ab"), "")
        flt2 = RedisSortedSetFilter(self.server, "key")
        self.assertEqual(flt2._get_block("ab"), "")
        flt3 = RedisBloomFilter(self.server, "key", bit=8)
        self.assertEqual(flt3._get_block("ab"), "")

    def test_block_num_many_has_suffix(self):
        flt = RedisSetFilter(self.server, "key", block_num=3)
        self.assertEqual(flt._get_block("ad"), "2")

    def test_string_filter_does_not_require_key(self):
        flt = RedisStringFilter(self.server, logger=quiet_logger())
        self.assertIsInstance(flt.exists_script, type(flt.insert_script))

    def test_md5_hash_is_hex_for_block(self):
        value = utils.md5("x")
        self.assertRegex(value, "^[0-9a-f]{32}$")
