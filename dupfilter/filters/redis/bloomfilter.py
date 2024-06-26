# -*- coding:utf-8 -*-
# author: kusen
# email: 1194542196@qq.com
# date: 2023/9/6


from dupfilter import utils
from dupfilter.filters import decorate_warning
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


class RedisBloomFilter(RedisFilter):
    def __init__(self, server, key, bit=32, hash_num=6, block_num=1,
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
        super(RedisBloomFilter, self).__init__(server, *args, **kwargs)
        self._exists_script = self.server.register_script(EXISTS_SCRIPT)
        self._insert_script = self.server.register_script(INSERT_SCRIPT)
        self._exists_insert_script = self.server.register_script(
            EXISTS_AND_INSERT_SCRIPT)

    @decorate_warning
    def exists(self, value):
        return self.exists_many([value])[0]

    @decorate_warning
    def exists_many(self, values):
        keys, offsets, new_values = self._get_keys_and_offsets(values)
        stats = self._exists_script(keys=keys, args=offsets)
        stats = [bool(stat) for stat in stats]
        self._log_exists(values, new_values, stats)
        return stats

    @decorate_warning
    def insert(self, value):
        return self.insert_many([value])[0]

    @decorate_warning
    def insert_many(self, values):
        keys, offsets, new_values = self._get_keys_and_offsets(values)
        stat = self._insert_script(keys=keys, args=offsets)
        return [bool(stat) for _ in range(len(values))]

    @decorate_warning
    def exists_and_insert(self, value):
        return self.exists_and_insert_many([value])[0]

    @decorate_warning
    def exists_and_insert_many(self, values):
        keys, offsets, new_values = self._get_keys_and_offsets(values)
        stats = self._exists_insert_script(keys=keys, args=offsets)
        stats = [bool(stat) for stat in stats]
        self._log_exists(values, new_values, stats)
        return stats

    def _get_keys_and_offsets(self, values):
        values = [self._value_hash(value) for value in values]
        keys = [self.key + self._get_block(value) for value in values]
        offsets = [','.join([str(sh.hash(
            self._value_compress(value))) for sh in self.shs]
        ) for value in values]
        return keys, offsets, values

    def _get_block(self, value):
        return str(int(value[0:2], 16) % self.block_num)


class AsyncRedisBloomFilter(RedisBloomFilter):
    @decorate_warning
    async def exists(self, value):
        stats = await self.exists_many([value])
        return stats[0]

    @decorate_warning
    async def exists_many(self, values):
        keys, offsets, new_values = self._get_keys_and_offsets(values)
        stats = await self._exists_script(keys=keys, args=offsets)
        stats = [bool(stat) for stat in stats]
        self._log_exists(values, new_values, stats)
        return stats

    @decorate_warning
    async def insert(self, value):
        stats = await self.insert_many([value])
        return stats[0]

    @decorate_warning
    async def insert_many(self, values):
        keys, offsets, new_values = self._get_keys_and_offsets(values)
        stat = await self._insert_script(keys=keys, args=offsets)
        return [bool(stat) for _ in range(len(values))]

    @decorate_warning
    async def exists_and_insert(self, value):
        stats = await self.exists_and_insert_many([value])
        return stats[0]

    @decorate_warning
    async def exists_and_insert_many(self, values):
        keys, offsets, new_values = self._get_keys_and_offsets(values)
        stats = await self._exists_insert_script(keys=keys, args=offsets)
        stats = [bool(stat) for stat in stats]
        self._log_exists(values, new_values, stats)
        return stats

if __name__ == '__main__':
    bf = BloomFilter({}, '1', value_compress_func=utils.hex2b64)
    bf.insert("123")
