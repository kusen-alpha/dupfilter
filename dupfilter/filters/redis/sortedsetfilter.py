# -*- coding:utf-8 -*-
# author: kusen
# email: 1194542196@qq.com
# date: 2023/9/10


from dupfilter.filters import decorate_warning
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


class RedisSortedSetFilter(RedisFilter):
    def __init__(self, server, key, block_num=1,
                 *args, **kwargs):
        self.key = key
        self.block_num = block_num
        super(RedisSortedSetFilter, self).__init__(server, *args, **kwargs)
        self._exists_script = self.server.register_script(EXISTS_SCRIPT)
        self._insert_script = self.server.register_script(INSERT_SCRIPT)
        self._exists_and_insert_script = self.server.register_script(
            EXISTS_AND_INSERT_SCRIPT)

    @decorate_warning
    def exists(self, value):
        return self.exists_many([value])[0]

    @decorate_warning
    def exists_many(self, values):
        keys, new_values = self._get_keys_and_values(values)
        stats = self._exists_script(keys=keys, args=new_values)
        stats = [bool(stat) for stat in stats]
        self._log_exists(values, new_values, stats)
        return stats

    @decorate_warning
    def exists_and_insert(self, value):
        return self.exists_and_insert_many([value])[0]

    @decorate_warning
    def exists_and_insert_many(self, values):
        keys, new_values = self._get_keys_and_values(values)
        stats = self._exists_and_insert_script(keys=keys, args=new_values)
        stats = [bool(stat) for stat in stats]
        self._log_exists(values, new_values, stats)
        return stats

    @decorate_warning
    def insert(self, value):
        return self.insert_many([value])[0]

    @decorate_warning
    def insert_many(self, values):
        keys, values = self._get_keys_and_values(values)
        stat = self._insert_script(keys=keys, args=values)
        return [bool(stat) for _ in range(len(values))]

    def _get_keys_and_values(self, values):
        values = [self._value_hash(value) for value in values]
        keys = [self.key + self._get_block(value) for value in values]
        values = [self._value_compress(value) for value in values]
        return keys, values

    def _get_block(self, value):
        return str(int(value[0:2], 16) % self.block_num)


class AsyncRedisSortedSetFilter(RedisSortedSetFilter):
    @decorate_warning
    async def insert(self, value):
        stats = await self.insert_many([value])
        return stats[0]

    @decorate_warning
    async def insert_many(self, values):
        keys, values = self._get_keys_and_values(values)
        stat = await self._insert_script(keys=keys, args=values)
        return [bool(stat) for _ in range(len(values))]

    @decorate_warning
    async def exists(self, value):
        stats = await self.exists_many([value])
        return stats[0]

    @decorate_warning
    async def exists_many(self, values):
        keys, new_values = self._get_keys_and_values(values)
        stats = await self._exists_script(keys=keys, args=new_values)
        stats = [bool(stat) for stat in stats]
        self._log_exists(values, new_values, stats)
        return stats

    @decorate_warning
    async def exists_and_insert(self, value):
        stats = await self.exists_and_insert([value])
        return stats[0]

    @decorate_warning
    async def exists_and_insert_many(self, values):
        keys, new_values = self._get_keys_and_values(values)
        stats = await self._exists_and_insert_script(keys=keys, args=new_values)
        stats = [bool(stat) for stat in stats]
        self._log_exists(values, new_values, stats)
        return stats