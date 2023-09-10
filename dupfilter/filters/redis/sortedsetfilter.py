# -*- coding:utf-8 -*-
# author: kusen
# email: 1194542196@qq.com
# date: 2023/9/10

import sys

import cachetools

from dupfilter.filters.redis import RedisFilter

EXISTS_SCRIPT = """
local keys = KEYS
local values = ARGV
local result = {}
for index, key in pairs(keys) do
    local exist = redis.call('ZSCORE', key, values[index])
    table.insert(result, exist)
end
return result
"""
INSERT_SCRIPT = """
local keys = KEYS
local values = ARGV
for index, key in pairs(keys) do
    local ts = redis.call("TIME")[1]
    redis.call('ZADD', key, 'NX', ts, values[index])
end
return true
"""
EXISTS_AND_INSERT_SCRIPT = """
local keys = KEYS
local values = ARGV
local result = {}
for index, key in pairs(keys) do
    local ts = redis.call("TIME")[1]
    local exist = redis.call('ZADD', key, 'NX', ts, values[index])
    if exist==0 then
        table.insert(result, 1)
    else
        table.insert(result, 0)
    end
end
return result
"""
RESET_SCRIPT = """
local key = KEYS[1]
local expire = ARGV[1]
local ts = redis.call("TIME")[1]
redis.call('ZREMRANGEBYSCORE', key, 0, ts-expire)
return true
"""


class SortedSetFilter(RedisFilter):
    def __init__(self, server, key, block_num=1, maxsize=sys.maxsize,
                 reset=False, reset_proportion=0.8, reset_check_period=7200,
                 reset_expire=2592000,
                 *args, **kwargs):
        self.key = key
        self.block_num = block_num
        self.maxsize = maxsize
        self.reset = reset
        self.reset_proportion = reset_proportion
        self.reset_check_period = reset_check_period
        self.reset_expire = reset_expire
        super(SortedSetFilter, self).__init__(server, *args, **kwargs)
        self._exists_script = self.server.register_script(EXISTS_SCRIPT)
        self._insert_script = self.server.register_script(INSERT_SCRIPT)
        self._exists_and_insert_script = self.server.register_script(
            EXISTS_AND_INSERT_SCRIPT)
        self._reset_script = self.server.register_script(RESET_SCRIPT)
        self.cache = cachetools.TTLCache(maxsize=1, ttl=reset_check_period)
        self.cache['proportions'] = {}
        self._resetting = False

    def exists(self, value):
        return self.exists_many([value])[0]

    def exists_many(self, values):
        keys, values = self._get_keys_and_values(values)
        stats = self._exists_script(keys=keys, args=values)
        return [bool(stat) for stat in stats]

    def exists_and_insert(self, value):
        return self.exists_and_insert_many([value])[0]

    def exists_and_insert_many(self, values):
        keys, values = self._get_keys_and_values(values)
        self._reset(len(keys))
        stats = self._exists_and_insert_script(keys=keys, args=values)
        return [bool(stat) for stat in stats]

    def insert(self, value):
        return self.insert_many([value])

    def insert_many(self, values):
        keys, values = self._get_keys_and_values(values)
        self._reset(len(keys))
        stat = self._insert_script(keys=keys, args=values)
        return bool(stat)

    def _reset(self, count):
        if not self.reset:
            return
        if not self.cache.get('proportions'):
            proportions = self.proportions
            if not proportions:
                return
            self.cache['proportions'] = proportions
        key, proportion = self._get_max_proportion()
        if not proportion or (proportion < self.reset_proportion):
            return
        self._reset_script(keys=[key], args=[count])

    @property
    def proportions(self):
        proportions = {}
        if self._resetting:
            return proportions
        self._resetting = True
        for block in range(self.block_num):
            key = self.key + str(block)
            proportions[key] = self.server.zcard(key) / self.maxsize
        self._resetting = False
        return proportions

    def _get_max_proportion(self):
        try:
            key, proportion = sorted(
                list(self.cache['proportions'].items()),
                key=lambda x: x[1])[-1]
            return key, proportion
        except KeyError:
            return None, None

    def _get_keys_and_values(self, values):
        values = [self._value_hash(value) for value in values]
        keys = [self.key + self._get_block(value) for value in values]
        values = [self._value_compress(value) for value in values]
        return keys, values

    def _get_block(self, value):
        return str(int(value[0:2], 16) % self.block_num)


class AsyncSortedSetFilter(SortedSetFilter):
    async def insert(self, value):
        return await self.insert_many([value])

    async def insert_many(self, values):
        keys, values = self._get_keys_and_values(values)
        await self._reset(len(keys))
        stat = await self._insert_script(keys=keys, args=values)
        return bool(stat)

    async def exists(self, value):
        stats = await self.exists_many([value])
        return stats[0]

    async def exists_many(self, values):
        keys, values = self._get_keys_and_values(values)
        stats = await self._exists_script(keys=keys, args=values)
        return [bool(stat) for stat in stats]

    async def exists_and_insert(self, value):
        stats = await self.exists_and_insert([value])
        return stats[0]

    async def exists_and_insert_many(self, values):
        keys, values = self._get_keys_and_values(values)
        await self._reset(len(keys))
        stats = await self._exists_and_insert_script(keys=keys, args=values)
        return [bool(stat) for stat in stats]

    async def _reset(self, count):
        if not self.reset:
            return
        proportions = self.cache.get('proportions')
        if not proportions:
            proportions = await self.proportions
            if not proportions:
                return
            self.cache['proportions'] = proportions
        key, proportion = self._get_max_proportion()
        if not proportion or (proportion < self.reset_proportion):
            return
        await self._reset_script(keys=[key], args=[count])

    @property
    async def proportions(self):
        proportions = {}
        if self._resetting:
            return proportions
        self._resetting = True
        for block in range(self.block_num):
            key = self.key + str(block)
            proportions[key] = await self.server.zcard(key) / self.maxsize
        self._resetting = False
        return proportions
