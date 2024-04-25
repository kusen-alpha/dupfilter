# -*- coding:utf-8 -*-
# author: kusen
# email: 1194542196@qq.com
# date: 2023/9/7


from dupfilter.filters import Filter, decorate_warning
from dupfilter.filters import Reset, decorate_reset


class MemoryReset(Reset):
    def current_count(self):
        return len(self.flt.dups)


class MemoryFilter(Filter):
    def __init__(self, *args, **kwargs):
        self.dups = set()
        super(MemoryFilter, self).__init__(*args, **kwargs)

    @decorate_warning
    @decorate_reset
    def exists(self, value):
        new_value = self._value_hash_and_compress(value)
        stat = new_value in self.dups
        self._log_exists([value], [new_value], [stat])
        return stat

    @decorate_warning
    def exists_many(self, values):
        return [self.exists(value) for value in values]

    @decorate_warning
    def insert(self, value):
        new_value = self._value_hash_and_compress(value)
        self.dups.add(new_value)
        return True

    @decorate_warning
    def insert_many(self, values):
        return [self.insert(value) for value in values]

    @decorate_warning
    def exists_and_insert(self, value):
        new_value = self._value_hash_and_compress(value)
        stat = new_value in self.dups
        self._log_exists([value], [new_value], [stat])
        if not stat:
            self.dups.add(new_value)
        return stat

    @decorate_warning
    def exists_and_insert_many(self, values):
        return [self.exists_and_insert(value) for value in values]
