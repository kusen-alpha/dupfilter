# -*- coding:utf-8 -*-
# author: kusen
# email: 1194542196@qq.com
# date: 2023/9/6


import asyncio
import functools

from dupfilter import utils


def decorate_warning(func):
    @functools.wraps(func)
    def wrapper(obj, value_or_values, *args, **kwargs):
        try:
            result = func(obj, value_or_values, *args, **kwargs)
        except Exception as e:
            if isinstance(value_or_values, list):
                result = [obj.default_stat for _ in value_or_values]
            else:
                result = obj.default_stat
        return result

    @functools.wraps(func)
    async def async_wrapper(obj, value_or_values, *args, **kwargs):
        try:
            result = await func(obj, value_or_values, *args, **kwargs)
        except Exception as e:
            if isinstance(value_or_values, list):
                result = [obj.default_stat for _ in value_or_values]
            else:
                result = obj.default_stat
        return result

    if asyncio.iscoroutinefunction(func):
        return async_wrapper
    else:
        return wrapper


class Reset(object):
    def __init__(self, max_rate=0.8, reset_rate=0.5):
        self.max_rate = max_rate
        self.reset_rate = reset_rate
        self.resetting = False

    def used(self):
        pass

    def reset(self):
        pass


class Filter(object):
    def __init__(
            self,
            value_hash_func=utils.md5,
            value_compress_func=None,
            default_stat=False
    ):
        self.value_hash_func = value_hash_func
        self.value_compress_func = value_compress_func or (lambda value: value)
        self.default_stat = bool(default_stat)

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


class DefaultFilter(object):
    def __init__(self, default_stat=False):
        self.default_stat = bool(default_stat)

    def exists(self, value):
        return self.default_stat

    def insert(self, value):
        return True

    def exists_many(self, values):
        return [self.exists(value) for value in values]

    def insert_many(self, values):
        return [self.insert(value) for value in values]
