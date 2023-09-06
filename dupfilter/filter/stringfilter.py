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


class StringFilter(Filter):
    def __init__(self, *args, **kwargs):
        super(StringFilter, self).__init__(*args, **kwargs)

    def insert(self, value, expire=2592000):
        key = self._value_hash_and_compress(value)

    def insert_many(self, values):
        pass

    def exists(self, value, expire=7200):
        key = self._value_hash_and_compress(value)

    def exists_many(self, values):
        pass

    exists_and_lock = exists
    exists_and_lock_many = exists_many
    confirm = insert
    confirm_many = insert_many
