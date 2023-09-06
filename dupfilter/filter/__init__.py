# -*- coding:utf-8 -*-
# author: kusen
# email: 1194542196@qq.com
# date: 2023/9/6
from dupfilter import utils


class Filter(object):
    def __init__(self, server, value_hash_func=utils.md5,
                 value_compress_func=None):
        self.server = server
        self.value_hash_func = value_hash_func
        self.value_compress_func = value_compress_func or (lambda value: value)

    def exists(self, value):
        raise NotImplemented

    def insert(self, value):
        raise NotImplemented

    def exists_many(self, values):
        raise NotImplemented

    def insert_many(self, values):
        raise NotImplemented

    def _value_hash(self, value):
        return self.value_hash_func(value)

    def _value_compress(self, value):
        return self.value_compress_func(value)

    def _value_hash_and_compress(self, value):
        return self.value_compress_func(self.value_hash_func(value))
