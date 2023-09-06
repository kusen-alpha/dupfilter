# -*- coding:utf-8 -*-
# author: kusen
# email: 1194542196@qq.com
# date: 2023/9/6
from dupfilter.filter import Filter


class SetFilter(Filter):
    def __init__(self, key, block_num=1, *args, **kwargs):
        super(SetFilter, self).__init__(*args, **kwargs)
        self.key = key
        self.block_num = block_num

    def exists(self, value):
        return bool(self.server.sismember(
            self._value_hash_and_compress(value)))

    def exists_and_insert(self, value):
        added = self.server.sadd(self._value_hash_and_compress(value))
        return added == 0

    def insert(self, value):
        added = self.server.sadd(self._value_hash_and_compress(value))
        return added == 1
