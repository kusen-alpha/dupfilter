# -*- coding:utf-8 -*-
# author: kusen
# email: 1194542196@qq.com
# date: 2023/9/6

import sys

import cachetools

from dupfilter.filters.redis import RedisFilter

EXISTS_SCRIPT = """
local keys = KEYS
local values = ARGV
local result = {}
for index, key in pairs(keys) do
    local exist = redis.call('SISMEMBER', key, values[index])
    table.insert(result, exist)
end
return result
"""
INSERT_SCRIPT = """
local keys = KEYS
local values = ARGV
for index, key in pairs(keys) do
    redis.call('SADD', key, values[index])
end
return true
"""
EXISTS_AND_INSERT_SCRIPT = """
local keys = KEYS
local values = ARGV
local result = {}
for index, key in pairs(keys) do
    local exist = redis.call('SISMEMBER', key, values[index])
    if exist==0 then
        redis.call('SADD', key, values[index])
    end
    table.insert(result, exist)
end
return result
"""
RESET_SCRIPT = """
local key = KEYS[1]
local count = ARGV[1]
redis.call('SPOP', key, count)
return true
"""


class SetFilter(RedisFilter):
    def __init__(self, server, key, block_num=1, maxsize=sys.maxsize,
                 reset=False, reset_proportion=0.8, reset_check_period=7200,
                 *args, **kwargs):
        self.key = key
        self.block_num = block_num
        self.maxsize = maxsize
        self.reset = reset
        self.reset_proportion = reset_proportion
        self.reset_check_period = reset_check_period
        super(SetFilter, self).__init__(server, *args, **kwargs)
        self.exists_script = self.server.register_script(EXISTS_SCRIPT)
        self.insert_script = self.server.register_script(INSERT_SCRIPT)
        self.exists_and_insert_script = self.server.register_script(
            EXISTS_AND_INSERT_SCRIPT)
        self.reset_script = self.server.register_script(RESET_SCRIPT)
        self.reset_info = cachetools.TTLCache(maxsize=2, ttl=reset_check_period)
        self.reset_info['proportions'] = {}

    def exists(self, value):
        return self.exists_many([value])[0]

    def exists_many(self, values):
        keys, values = self._get_keys_and_values(values)
        stats = self.exists_script(keys=keys, args=values)
        return [bool(stat) for stat in stats]

    def exists_and_insert(self, value):
        return self.exists_and_insert_many([value])[0]

    def exists_and_insert_many(self, values):
        keys, values = self._get_keys_and_values(values)
        self._reset(len(keys))
        stats = self.exists_and_insert_script(keys=keys, args=values)
        return [bool(stat) for stat in stats]

    def insert(self, value):
        return self.insert_many([value])

    def insert_many(self, values):
        keys, values = self._get_keys_and_values(values)
        self._reset(len(keys))
        return self.insert_script(keys=keys, args=values)

    def _reset(self, count):
        if not self.reset_info.get('proportions'):
            self.reset_info['proportions'] = self.proportions
        key, proportion = sorted(
            list(self.reset_info['proportions'].items()),
            key=lambda x: x[1])[-1]
        if proportion < self.reset_proportion:
            return
        self.reset_script(keys=[key], args=[count])

    @property
    def proportions(self):
        proportions = {}
        for block in range(self.block_num):
            key = self.key + str(block)
            proportions[key] = self.server.scard(key) / self.maxsize
        return proportions

    def _get_keys_and_values(self, values):
        values = [self._value_hash(value) for value in values]
        keys = [self.key + self._get_block(value) for value in values]
        values = [self._value_compress(value) for value in values]
        return keys, values

    def _get_block(self, value):
        return str(int(value[0:2], 16) % self.block_num)


class AsyncSetFilter(SetFilter):
    async def insert(self, value):
        return await self.insert_many([value])

    async def insert_many(self, values):
        keys, values = self._get_keys_and_values(values)
        await self._reset(len(keys))
        return await self.insert_script(keys=keys, args=values)

    async def exists(self, value):
        stats = await self.exists_many([value])
        return stats[0]

    async def exists_many(self, values):
        keys, values = self._get_keys_and_values(values)
        stats = await self.exists_script(keys=keys, args=values)
        return [bool(stat) for stat in stats]

    async def exists_and_insert(self, value):
        stats = await self.insert_many([value])
        return stats[0]

    async def exists_and_insert_many(self, values):
        keys, values = self._get_keys_and_values(values)
        await self._reset(len(keys))
        stats = await self.exists_and_insert_script(keys=keys, args=values)
        return [bool(stat) for stat in stats]

    async def _reset(self, count):
        proportions = self.reset_info.get('proportions')
        if not proportions:
            self.reset_info['proportions'] = await self.proportions
        key, proportion = sorted(
            list(self.reset_info['proportions'].items()),
            key=lambda x: x[1])[-1]
        if proportion < self.reset_proportion:
            return
        await self.reset_script(keys=[key], args=[count])

    @property
    async def proportions(self):
        proportions = {}
        for block in range(self.block_num):
            key = self.key + str(block)
            proportions[key] = await self.server.scard(key) / self.maxsize
        return proportions
