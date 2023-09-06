# -*- coding:utf-8 -*-
# author: kusen
# email: 1194542196@qq.com
# date: 2023/9/6

from dupfilter.filter import Filter


class StringFilter(Filter):
    def __init__(self, *args, **kwargs):
        super(StringFilter, self).__init__(*args, **kwargs)

    def insert(self, value, expire=2592000):
        key = self._value_hash_and_compress(value)

    def exists(self, value, expire=7200):
        key = self._value_hash_and_compress(value)

    exists_and_lock = exists
    confirm = insert
