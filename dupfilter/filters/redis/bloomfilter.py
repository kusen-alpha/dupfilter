# -*- coding:utf-8 -*-
# author: kusen
# email: 1194542196@qq.com
# date: 2023/9/6

import random

import cachetools

from dupfilter import utils
from dupfilter.filters.redis import RedisFilter

EXISTS_SCRIPT = """
local keys = KEYS
local offsets = ARGV
local result = {}
for index, key in pairs(keys) do
    local exist = 1
    for offset in string.gmatch(offsets[index], "[^,]+") do
        exist = redis.call('GETBIT', key, offset)
        if exist == 0 then
            break
        end
    end
    table.insert(result, exist)
end
return result
"""

INSERT_SCRIPT = """
local keys = KEYS
local offsets = ARGV
for index, key in pairs(keys) do
    for offset in string.gmatch(offsets[index], "[^,]+") do
        redis.call('SETBIT', key, offset, 1)
    end
end
return true
"""

EXISTS_AND_INSERT_SCRIPT = """
local keys = KEYS
local offsets = ARGV
local result = {}
for index, key in pairs(keys) do
    local exist = 1
    for offset in string.gmatch(offsets[index], "[^,]+") do
        local _exist = redis.call('GETBIT', key, offset)
        if _exist == 0 then
            exist = _exist
            redis.call('SETBIT', key, offset, 1)
        end
    end
    table.insert(result, exist)
end
return result
"""

RESET_SCRIPT = """
local key = KEYS[1]
local offsets = ARGV
for _, offset in pairs(offsets) do
    redis.call('SETBIT', key, offset, 0)
end
return true
"""


class SimpleHash(object):
    def __init__(self, bit, seed):
        self.bit = bit
        self.seed = seed

    def hash(self, value):
        ret = 0
        for i in range(len(value)):
            ret += self.seed * ret + ord(value[i])
        return self.bit & ret


class BloomFilter(RedisFilter):
    def __init__(self, server, key, bit=32, hash_num=6, block_num=1,
                 reset=False, reset_proportion=0.8, reset_check_period=7200,
                 *args, **kwargs):
        """

        :param server: The redis obj, redis.Redis()
        :param key:
        :param bit:
        :param hash_num:
        :param block_num:
        :param reset: 是否进行重置/清除，
        :param reset_proportion: 达到比例后进行重置/清除，
        :param reset_check_period: 重置/清除比例获取计算周期，
        """
        self.key = key
        if bit > 32:
            raise ValueError('The bit value not > 32!')
        self.bit = (1 << bit) - 1  # Redis的String类型最大容量为512M，即bit=32
        self.hash_num = hash_num
        self.seeds = range(1, hash_num + 1)
        self.shs = [SimpleHash(self.bit, seed) for seed in self.seeds]
        self.block_num = block_num
        self.reset = reset
        self.reset_proportion = reset_proportion
        super(BloomFilter, self).__init__(server, *args, **kwargs)
        self._exists_script = self.server.register_script(EXISTS_SCRIPT)
        self._insert_script = self.server.register_script(INSERT_SCRIPT)
        self._exists_insert_script = self.server.register_script(
            EXISTS_AND_INSERT_SCRIPT)
        self._reset_script = self.server.register_script(RESET_SCRIPT)
        self.cache = cachetools.TTLCache(maxsize=1, ttl=reset_check_period)
        self.cache['proportions'] = {}
        self._resetting = False

    def exists(self, value):
        return self.exists_many([value])[0]

    def exists_many(self, values):
        keys, offsets = self._get_keys_and_offsets(values)
        stats = self._exists_script(keys=keys, args=offsets)
        return [bool(stat) for stat in stats]

    def insert(self, value):
        return self.insert_many([value])

    def insert_many(self, values):
        keys, offsets = self._get_keys_and_offsets(values)
        self._reset(','.join(offsets))
        stat = self._insert_script(keys=keys, args=offsets)
        return bool(stat)

    def exists_and_insert(self, value):
        return self.exists_and_insert_many([value])[0]

    def exists_and_insert_many(self, values):
        keys, offsets = self._get_keys_and_offsets(values)
        self._reset(','.join(offsets))
        stats = self._exists_insert_script(keys=keys, args=offsets)
        return [bool(stat) for stat in stats]

    @property
    def proportions(self):
        proportions = {}
        if self._resetting:
            return {}
        self._resetting = True
        for block in range(self.block_num):
            key = self.key + str(block)
            proportions[key] = self.server.bitcount(key) / self.bit
        self._resetting = False
        return proportions

    def _reset(self, current_offsets):
        proportions = self.cache.get('proportions')
        if not proportions:
            proportions = self.proportions
            if not proportions:
                return
            self.cache['proportions'] = proportions
        key, proportion = sorted(
            list(self.cache['proportions'].items()),
            key=lambda x: x[1])[-1]
        if proportion < self.reset_proportion:
            return
        current_offsets = [int(offset) for offset in current_offsets.split(',')]
        offsets = self._get_reset_offsets(current_offsets)
        self._reset_script(keys=[key], args=offsets)

    def _get_keys_and_offsets(self, values):
        values = [self._value_hash(value) for value in values]
        keys = [self.key + self._get_block(value) for value in values]
        offsets = [','.join([str(sh.hash(
            self._value_compress(value))) for sh in self.shs]
        ) for value in values]
        return keys, offsets

    def _get_block(self, value):
        return str(int(value[0:2], 16) % self.block_num)

    def _get_reset_offsets(self, current_offsets):
        offsets = set()
        random_end = self.block_num + 1
        for offset in current_offsets:
            offsets.add(
                (offset + random.choice(range(1, random_end))) % self.bit
            )
        return list(offsets)


class AsyncBloomFilter(BloomFilter):

    async def exists(self, value):
        stats = await self.exists_many([value])
        return stats[0]

    async def exists_many(self, values):
        keys, offsets = self._get_keys_and_offsets(values)
        stats = await self._exists_script(keys=keys, args=offsets)
        return [bool(stat) for stat in stats]

    async def insert(self, value):
        return await self.insert_many([value])

    async def insert_many(self, values):
        keys, offsets = self._get_keys_and_offsets(values)
        await self._reset(','.join(offsets))
        stat = await self._insert_script(keys=keys, args=offsets)
        return bool(stat)

    async def exists_and_insert(self, value):
        stats = await self.exists_and_insert_many([value])
        return stats[0]

    async def exists_and_insert_many(self, values):
        keys, offsets = self._get_keys_and_offsets(values)
        await self._reset(','.join(offsets))
        stats = await self._exists_insert_script(keys=keys, args=offsets)
        return [bool(stat) for stat in stats]

    async def _reset(self, current_offsets):
        if not self.cache.get('proportions'):
            proportions = await self.proportions
            if not proportions:
                return
            self.cache['proportions'] = proportions
        key, proportion = sorted(
            list(self.cache['proportions'].items()),
            key=lambda x: x[1])[-1]
        if proportion < self.reset_proportion:
            return
        current_offsets = [int(offset) for offset in current_offsets.split(',')]
        offsets = self._get_reset_offsets(current_offsets)
        await self._reset_script(keys=[key], args=offsets)

    @property
    async def proportions(self):
        proportions = {}
        if self._resetting:
            return proportions
        self._resetting = True
        for block in range(self.block_num):
            key = self.key + str(block)
            proportions[key] = await self.server.bitcount(key) / self.bit
        self._resetting = False
        return proportions


if __name__ == '__main__':
    bf = BloomFilter({}, '1', value_compress_func=utils.hex2b64)
    bf.insert("123")
