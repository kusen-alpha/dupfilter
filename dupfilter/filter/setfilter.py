# -*- coding:utf-8 -*-
# author: kusen
# email: 1194542196@qq.com
# date: 2023/9/6
from dupfilter.filter import Filter

EXISTS_SCRIPT = """
local key = KEYS[1]
local offsets = ARGV
local exist = true
for _, offset in pairs(offsets) do
    exist = redis.call('GETBIT', key, offset)
    if not exist then
        break
    end
end
return exist

"""
INSERT_SCRIPT = """
local key = KEYS[1]
local offsets = ARGV
for _, offset in pairs(offsets) do
    redis.call('SETBIT', key, offset, 1)
end
return true
"""

class SetFilter(Filter):
    def __init__(self, key, block_num=1, *args, **kwargs):
        super(SetFilter, self).__init__(*args, **kwargs)
        self.key = key
        self.block_num = block_num

    def exists(self, value):
        return bool(self.server.sismember(
            self._value_hash_and_compress(value)))

    def exists_many(self, values):
        pass

    def exists_and_insert(self, value):
        added = self.server.sadd(self._value_hash_and_compress(value))
        return added == 0

    def exists_and_insert_many(self, values):
        pass

    def insert(self, value):
        added = self.server.sadd(self._value_hash_and_compress(value))
        return added == 1

    def insert_many(self, values):
        pass
