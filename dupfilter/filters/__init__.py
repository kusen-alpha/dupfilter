# -*- coding:utf-8 -*-
# author: kusen
# email: 1194542196@qq.com
# date: 2023/9/6


import asyncio
import functools
import logging

from dupfilter import utils


def decorate_warning(func):
    @functools.wraps(func)
    def wrapper(filter, value_or_values, *args, **kwargs):
        try:
            result = func(filter, value_or_values, *args, **kwargs)
        except Exception as e:
            if isinstance(value_or_values, list):
                result = [filter.default_stat for _ in value_or_values]
            else:
                result = filter.default_stat
            filter.logger.warning('去重操作失败，返回默认值：%s，错误详情：%s' % (
                filter.default_stat, str(e)))
        return result

    @functools.wraps(func)
    async def async_wrapper(filter, value_or_values, *args, **kwargs):
        try:
            result = await func(filter, value_or_values, *args, **kwargs)
        except Exception as e:
            if isinstance(value_or_values, list):
                result = [filter.default_stat for _ in value_or_values]
            else:
                result = filter.default_stat
            filter.logger.warning('去重操作失败，返回默认值：%s，错误详情：%s' % (
                filter.default_stat, str(e)))
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
            default_stat=False,
            logger=None,
            logger_level=logging.INFO
    ):
        self.value_hash_func = value_hash_func
        self.value_compress_func = value_compress_func or (lambda value: value)
        self.default_stat = bool(default_stat)
        if not logger:
            self.logger = logging.getLogger('dupfilter')
            self.logger.setLevel(logger_level)
            ft = logging.Formatter(
                fmt='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            ch = logging.StreamHandler()
            ch.setFormatter(ft)
            ch.setLevel(logger_level)
            self.logger.addHandler(ch)
        else:
            self.logger = logger

    def _log_exists(self, initial_values, handle_values, stats):
        for mixed in zip(stats, handle_values, initial_values):
            self.logger.info('去重查询结果：%s，处理值：%s，原始值：%s' % (
                mixed[0], mixed[1], mixed[2]))

    def exists(self, value):
        raise NotImplemented

    def insert(self, value):
        raise NotImplemented

    def exists_many(self, values):
        raise NotImplemented

    def insert_many(self, values):
        raise NotImplemented

    def exists_and_insert(self, value):
        pass

    def exists_and_insert_many(self, values):
        pass

    def _value_hash(self, value):
        return self.value_hash_func(value)

    def _value_compress(self, value):
        return self.value_compress_func(value)

    def _value_hash_and_compress(self, value):
        return self.value_compress_func(self.value_hash_func(value))

    def close(self):
        pass


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

    def exists_and_insert(self, value):
        return self.default_stat

    def exists_and_insert_many(self, values):
        return [self.exists_and_insert(value) for value in values]
