# -*- coding:utf-8 -*-
# author: kusen
# email: 1194542196@qq.com
# date: 2023/9/6
from dupfilter.filters import decorate_warning
from dupfilter.filters.redis import RedisFilter

EXISTS_SCRIPT = """
local keys = KEYS
local expire = ARGV[1]
local result = {}
for _, key in pairs(keys) do
    local exist = redis.call('SETNX', key, '')
    if exist==1 then
        table.insert(result, 0)
        redis.call('EXPIRE', key, expire)
    else
        table.insert(result, 1)
    end
end
return result

"""
INSERT_SCRIPT = """
local keys = KEYS
local expire = ARGV[1]
local result = {}
for _, key in pairs(keys) do
    local exist = redis.call('EXISTS', key)
    table.insert(result, exist)
    if exist==1 then
        redis.call('EXPIRE', key, expire)
    end
end
return result
"""


class RedisStringFilter(RedisFilter):
    def __init__(self, server, *args, **kwargs):
        super(RedisStringFilter, self).__init__(server, *args, **kwargs)
        self.exists_script = self.server.register_script(EXISTS_SCRIPT)
        self.insert_script = self.server.register_script(INSERT_SCRIPT)

    @decorate_warning
    def exists(self, value, expire=7200):
        return self.exists_many([value], expire)[0]

    @decorate_warning
    def exists_many(self, values, expire=7200):
        keys = [self._value_hash_and_compress(value) for value in values]
        stats = self.exists_script(keys=keys, args=[expire])
        stats = [bool(stat) for stat in stats]
        self._log_exists(values, keys, stats)
        return stats

    @decorate_warning
    def insert(self, value, expire=2592000):
        return self.insert_many([value], expire)[0]

    @decorate_warning
    def insert_many(self, values, expire=2592000):
        keys = [self._value_hash_and_compress(value) for value in values]
        stats = self.insert_script(keys=keys, args=[expire])
        return [bool(stat) for stat in stats]

    exists_and_lock = exists
    exists_and_lock_many = exists_many
    confirm = insert
    confirm_many = insert_many


class AsyncRedisStringFilter(RedisStringFilter):
    @decorate_warning
    async def insert(self, value, expire=2592000):
        stats = await self.insert_many([value], expire)
        return stats[0]

    @decorate_warning
    async def insert_many(self, values, expire=2592000):
        keys = [self._value_hash_and_compress(value) for value in values]
        stats = await self.insert_script(keys=keys, args=[expire])
        return [bool(stat) for stat in stats]

    @decorate_warning
    async def exists(self, value, expire=7200):
        stats = await self.exists_many([value], expire)
        return stats[0]

    @decorate_warning
    async def exists_many(self, values, expire=7200):
        keys = [self._value_hash_and_compress(value) for value in values]
        stats = await self.exists_script(keys=keys, args=[expire])
        stats = [bool(stat) for stat in stats]
        self._log_exists(values, keys, stats)
        return stats
