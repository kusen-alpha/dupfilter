# -*- coding:utf-8 -*-
# author: kusen
# email: 1194542196@qq.com
# date: 2023/9/7
from dupfilter.filters import Filter


class MemoryFilter(Filter):
    def __init__(self, *args, **kwargs):
        self.dups = set()
        super(MemoryFilter, self).__init__(*args, **kwargs)

    def exists(self, value):
        value = self._value_hash_and_compress(value)
        return value in self.dups

    def insert(self, value):
        value = self._value_hash_and_compress(value)
        self.dups.add(value)
        return True

    def exists_and_insert(self, value):
        value = self._value_hash_and_compress(value)
        stats = value in self.dups
        if not stats:
            self.dups.add(value)
        return stats

    def exists_many(self, values):
        return [self.exists(value) for value in values]

    def insert_many(self, values):
        return [self.insert(value) for value in values]

    def exists_and_insert_many(self, values):
        return [self.exists_and_insert(value) for value in values]
