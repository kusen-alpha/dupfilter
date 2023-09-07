# -*- coding:utf-8 -*-
# author: kusen
# email: 1194542196@qq.com
# date: 2023/9/6

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


class SetFilter(RedisFilter):
    def __init__(self, server, key, block_num=1, *args, **kwargs):
        self.key = key
        self.block_num = block_num
        super(SetFilter, self).__init__(server, *args, **kwargs)
        self.exists_script = self.server.register_script(EXISTS_SCRIPT)
        self.insert_script = self.server.register_script(INSERT_SCRIPT)
        self.exists_and_insert_script = self.server.register_script(
            EXISTS_AND_INSERT_SCRIPT)

    def exists(self, value):
        return self.exists_many([value])

    def exists_many(self, values):
        keys, values = self._get_keys_and_values(values)
        stats = self.exists_script(keys=keys, args=values)
        return [bool(stat) for stat in stats]

    def exists_and_insert(self, value):
        added = self.server.sadd(self._value_hash_and_compress(value))
        return added == 0

    def exists_and_insert_many(self, values):
        keys, values = self._get_keys_and_values(values)
        stats = self.exists_and_insert_script(keys=keys, args=values)
        return [bool(stat) for stat in stats]

    def insert(self, value):
        return self.insert_many([value])

    def insert_many(self, values):
        keys, values = self._get_keys_and_values(values)
        return self.insert_script(keys=keys, args=values)

    def _get_keys_and_values(self, values):
        values = [self._value_hash(value) for value in values]
        keys = [self.key + self._get_block(value) for value in values]
        values = [self._value_compress(value) for value in values]
        return keys, values

    def _get_block(self, value):
        return str(int(value[0:2], 16) % self.block_num)
