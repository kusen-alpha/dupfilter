# -*- coding:utf-8 -*-
# author: kusen
# email: 1194542196@qq.com
# date: 2023/9/6


import asyncio
import functools
import logging

from cachetools import TTLCache

from dupfilter import utils


def decorate_warning(func):
    @functools.wraps(func)
    def wrapper(flt, value_or_values, *args, **kwargs):
        try:
            result = func(flt, value_or_values, *args, **kwargs)
        except Exception as e:
            if isinstance(value_or_values, list):
                result = [flt.default_stat for _ in value_or_values]
            else:
                result = flt.default_stat
            flt.logger.warning('去重操作失败，返回默认值：%s，错误详情：%s' % (
                flt.default_stat, str(e)))
        return result

    @functools.wraps(func)
    async def async_wrapper(flt, value_or_values, *args, **kwargs):
        try:
            result = await func(flt, value_or_values, *args, **kwargs)
        except Exception as e:
            if isinstance(value_or_values, list):
                result = [flt.default_stat for _ in value_or_values]
            else:
                result = flt.default_stat
            flt.logger.warning('去重操作失败，返回默认值：%s，错误详情：%s' % (
                flt.default_stat, str(e)))
        return result

    if asyncio.iscoroutinefunction(func):
        return async_wrapper
    else:
        return wrapper


class Reset(object):
    def __init__(self, max_count, max_rate=0.8,
                 reset_to_rate=0.5, reset_type=None,
                 monitor_time=3600):
        self.max_count = max_count
        self.max_rate = max_rate
        self.reset_to_rate = reset_to_rate
        self.reset_type = reset_type
        self.monitor_time = monitor_time
        self.resetting = False
        self.timer = TTLCache(1, monitor_time)
        self.set_timer()
        self.flt = None

    def set_timer(self):
        self.timer['timer'] = 'timer'

    def get_timer(self):
        return self.timer.get('timer')

    @property
    def used_rate(self):
        return self.current_count / self.max_count

    @property
    def current_count(self):
        raise NotImplemented

    @property
    def reset_count(self):
        return self.current_count - int(self.max_count * self.reset_to_rate)

    def _reset(self):
        if self.resetting:
            return
        if self.get_timer():
            return
        if self.used_rate < self.max_rate:
            return
        self.resetting = True
        self.reset()
        if self.resetting:
            self.resetting = False
        self.set_timer()

    def reset(self):
        raise NotImplemented


def decorate_reset(func):
    @functools.wraps(func)
    def wrapper(flt, *args, **kwargs):
        try:
            if flt.reset:
                flt.reset._reset()
        except Exception as e:
            flt.reset.resetting = False
            flt.logger.warning("重置操作失败：%s" % str(e))
        result = func(flt, *args, **kwargs)
        return result

    @functools.wraps(func)
    async def async_wrapper(flt, *args, **kwargs):
        try:
            if flt.reset:
                await flt.reset._reset()
        except Exception as e:
            flt.reset.resetting = False
            flt.logger.warning("重置操作失败：%s" % str(e))
        result = await func(flt, *args, **kwargs)
        return result

    if asyncio.iscoroutinefunction(func):
        return async_wrapper
    else:
        return wrapper


class Filter(object):
    def __init__(
            self,
            value_hash_func=utils.md5,
            value_compress_func=None,
            default_stat=False,
            logger=None,
            logger_level=logging.INFO,
            reset=None
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
        if reset:
            reset.flt = self
        self.reset = reset

    def _log_exists(self, initial_values, handle_values, stats):
        for mixed in zip(stats, handle_values, initial_values):
            self.logger.info('去重结果：%s，去重值：%s，原始值：%s' % (
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


class AsyncDefaultFilter(DefaultFilter):
    async def exists(self, *args, **kwargs):
        return super(AsyncDefaultFilter, self).exists(*args, **kwargs)

    async def insert(self, *args, **kwargs):
        return super(AsyncDefaultFilter, self).insert(*args, **kwargs)

    async def exists_many(self, *args, **kwargs):
        return super(AsyncDefaultFilter, self).exists_many(*args, **kwargs)

    async def insert_many(self, *args, **kwargs):
        return super(AsyncDefaultFilter, self).insert_many(*args, **kwargs)

    async def exists_and_insert(self, *args, **kwargs):
        return super(AsyncDefaultFilter, self).exists_and_insert(
            *args, **kwargs)

    async def exists_and_insert_many(self, *args, **kwargs):
        return super(AsyncDefaultFilter, self).exists_and_insert_many(
            *args, **kwargs)


class FilterCounter(object):
    def __init__(self, stats=None):
        if not stats:
            stats = []
        self.stats = [bool(stat) for stat in stats]

    def count(self, exist=True):
        filter_stats = [stat for stat in self.stats if stat == bool(exist)]
        return len(filter_stats)

    def insert_stat(self, stat):
        self.stats.append(bool(stat))

    def insert_stats(self, stats):
        for stat in stats:
            self.insert_stat(stat)

    def any(self, exist=True):
        return self.count(exist) > 0

    def all(self, exist=True):
        return self.count(exist) == len(self.stats)

    def reach(self, count, exist=True):
        return self.count(exist) >= count
