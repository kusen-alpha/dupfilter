# -*- coding:utf-8 -*-
# author: kusen
# email: 1194542196@qq.com
# date: 2023/9/6


import asyncio
import functools
import inspect
import logging
import threading
import time
from collections.abc import Iterable

from dupfilter import utils


def _is_batch(value):
    if isinstance(value, (list, tuple, set, frozenset, range)):
        return True
    return (isinstance(value, Iterable)
            and not isinstance(value, (str, bytes, bytearray, dict)))


def _default_result(value, default):
    if _is_batch(value):
        return [default for _ in value]
    return default


def _resolve_value(value_or_values, kwargs):
    if value_or_values is None and 'value' in kwargs:
        return kwargs.pop('value')
    if value_or_values is None and 'values' in kwargs:
        return kwargs.pop('values')
    return value_or_values


def decorate_warning(func):
    @functools.wraps(func)
    def wrapper(flt, value_or_values=None, *args, **kwargs):
        value_or_values = _resolve_value(value_or_values, kwargs)
        try:
            result = func(flt, value_or_values, *args, **kwargs)
        except Exception as e:
            if getattr(flt, 'raise_on_error', False):
                raise
            result = _default_result(value_or_values, flt.default_stat)
            flt.logger.warning(
                '去重操作失败，返回默认值：%s，错误详情：%s',
                flt.default_stat, str(e))
        return result

    @functools.wraps(func)
    async def async_wrapper(flt, value_or_values=None, *args, **kwargs):
        value_or_values = _resolve_value(value_or_values, kwargs)
        try:
            result = await func(flt, value_or_values, *args, **kwargs)
        except Exception as e:
            if getattr(flt, 'raise_on_error', False):
                raise
            result = _default_result(value_or_values, flt.default_stat)
            flt.logger.warning(
                '去重操作失败，返回默认值：%s，错误详情：%s',
                flt.default_stat, str(e))
        return result

    if asyncio.iscoroutinefunction(func):
        return async_wrapper
    else:
        return wrapper


class Reset(object):
    def __init__(self, max_count, max_rate=0.8,
                 reset_to_rate=0.5, reset_type=None,
                 monitor_time=3600):
        if max_count <= 0:
            raise ValueError('max_count must be > 0')
        if not 0 < max_rate <= 1:
            raise ValueError('max_rate must be in (0, 1]')
        if not 0 < reset_to_rate < max_rate:
            raise ValueError('reset_to_rate must be between 0 and max_rate')
        if monitor_time <= 0:
            raise ValueError('monitor_time must be > 0')
        self.max_count = max_count
        self.max_rate = max_rate
        self.reset_to_rate = reset_to_rate
        self.reset_type = reset_type
        self.monitor_time = monitor_time
        self.resetting = False
        self._lock = threading.Lock()
        self._next_reset_at = 0.0
        self.set_timer()
        self.flt = None

    def _timer_active(self):
        return time.monotonic() < self._next_reset_at

    def set_timer(self):
        with self._lock:
            self._next_reset_at = time.monotonic() + self.monitor_time

    def get_timer(self):
        with self._lock:
            return self._timer_active()

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
        with self._lock:
            if self.resetting or self._timer_active():
                return
            if self.used_rate < self.max_rate:
                return
            self.resetting = True
        try:
            self.reset()
        finally:
            with self._lock:
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
            flt.logger.warning("重置操作失败：%s" % str(e))
        result = func(flt, *args, **kwargs)
        return result

    @functools.wraps(func)
    async def async_wrapper(flt, *args, **kwargs):
        try:
            if flt.reset:
                result = flt.reset._reset()
                if inspect.isawaitable(result):
                    await result
        except Exception as e:
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
            logger_level=logging.DEBUG,
            reset=None,
            raise_on_error=False
    ):
        self.value_hash_func = value_hash_func or (lambda value: value)
        self.value_compress_func = value_compress_func or (lambda value: value)
        self.default_stat = bool(default_stat)
        self.raise_on_error = bool(raise_on_error)
        if not logger:
            self.logger = logging.getLogger('dupfilter')
            self.logger.setLevel(logger_level)
            if not self.logger.handlers:
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
            self.logger.info(
                '去重结果：%s，去重值：%s，原始值：%s',
                mixed[0], mixed[1], mixed[2])

    def exists(self, value):
        raise NotImplemented

    def insert(self, value):
        raise NotImplemented

    def exists_many(self, values):
        raise NotImplemented

    def insert_many(self, values):
        raise NotImplemented

    def exists_and_insert(self, value):
        raise NotImplementedError

    def exists_and_insert_many(self, values):
        raise NotImplementedError

    def _value_hash(self, value):
        return self.value_hash_func(value)

    def _value_compress(self, value):
        return self.value_compress_func(value)

    def _value_hash_and_compress(self, value):
        return self.value_compress_func(self.value_hash_func(value))

    def close(self):
        pass

    def release(self, *args, **kwargs):
        """
        释放去重种子
        :param args:
        :param kwargs:
        :return:
        """
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

    def close(self):
        pass

    def release(self, *args, **kwargs):
        """
        释放去重种子
        :param args:
        :param kwargs:
        :return:
        """
        pass


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

    def count(self, stat=True):
        result = [_stat for _stat in self.stats if _stat == bool(stat)]
        return len(result)

    def insert_stat(self, stat):
        self.stats.append(bool(stat))

    def insert_stats(self, stats):
        for stat in stats:
            self.insert_stat(stat)

    def any(self, stat=True):
        return self.count(stat) > 0

    def all(self, stat=True):
        return self.count(stat) == len(self.stats)

    def reach(self, count, stat=True):
        return self.count(stat) >= count

    def appeared(self, index, stat=True):
        return (1 <= index <= len(self.stats)
                and self.stats[index - 1] == stat)


class PageFilterCounter(FilterCounter):
    def __init__(self, stats=None, max_page=None, current_page=None,
                 duplicate=False):
        super(PageFilterCounter, self).__init__(stats)
        self.max_page = max_page
        if not duplicate:
            self.max_page = None
        self.current_page = 1 if current_page == 0 else current_page

    def has_next(self):
        if not self.max_page or not self.current_page:
            return True
        if self.current_page < self.max_page:
            return True
        return False
