# -*- coding:utf-8 -*-
# author: kusen
# email: 1194542196@qq.com
# date: 2023/9/6
from dupfilter import utils


class Filter(object):
    def __init__(self,
                 value_hash_func=utils.md5,
                 value_compress_func=None,
                 ):
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


class ResetFilter(Filter):
    def __init__(self,
                 reset=False,
                 reset_proportion=0.8,
                 reset_check_period=7200,
                 *args, **kwargs
                 ):
        self.reset = reset
        self.reset_proportion = reset_proportion
        self.reset_check_period = reset_check_period
        self._resetting = False
        super(ResetFilter, self).__init__(*args, **kwargs)
