# -*- coding:utf-8 -*-
# author: kusen
# email: 1194542196@qq.com
# date: 2023/9/6

from __future__ import annotations

import asyncio
import functools
import inspect
import logging
import threading
import time
from collections.abc import Iterable
from typing import Any, Callable, TypeVar, cast

from dupfilter import utils

F = TypeVar("F", bound=Callable[..., Any])


def _is_batch(value: Any) -> bool:
    if isinstance(value, (list, tuple, set, frozenset, range)):
        return True
    return (isinstance(value, Iterable)
            and not isinstance(value, (str, bytes, bytearray, dict)))


def _default_result(value: Any, default: Any) -> Any:
    if _is_batch(value):
        return [default for _ in value]
    return default


def _resolve_value(value_or_values: Any,
                   kwargs: dict[str, Any]) -> Any:
    if value_or_values is None and 'value' in kwargs:
        return kwargs.pop('value')
    if value_or_values is None and 'values' in kwargs:
        return kwargs.pop('values')
    return value_or_values


async def to_thread(func: Callable[..., Any], *args: Any,
                    **kwargs: Any) -> Any:
    """Run a blocking call in a worker thread (3.8-compatible asyncio.to_thread)."""
    impl = getattr(asyncio, "to_thread", None)
    if impl is not None:
        return await impl(func, *args, **kwargs)
    return await asyncio.get_event_loop().run_in_executor(
        None, functools.partial(func, *args, **kwargs))


def decorate_warning(func: F) -> F:
    @functools.wraps(func)
    def wrapper(flt: Any, value_or_values: Any = None,
                *args: Any, **kwargs: Any) -> Any:
        value_or_values = _resolve_value(value_or_values, kwargs)
        raise_on_error = kwargs.pop('raise_on_error', None)
        try:
            result = func(flt, value_or_values, *args, **kwargs)
        except Exception as e:
            if raise_on_error is None:
                raise_on_error = getattr(flt, 'raise_on_error', False)
            if raise_on_error:
                raise
            result = _default_result(value_or_values, flt.default_stat)
            on_error = getattr(flt, 'on_error', None)
            if on_error:
                on_error(e, result)
            flt.logger.warning(
                '去重操作失败，返回默认值：%s，错误详情：%s',
                flt.default_stat, str(e))
        return result

    @functools.wraps(func)
    async def async_wrapper(flt: Any, value_or_values: Any = None,
                            *args: Any, **kwargs: Any) -> Any:
        value_or_values = _resolve_value(value_or_values, kwargs)
        raise_on_error = kwargs.pop('raise_on_error', None)
        try:
            result = await func(flt, value_or_values, *args, **kwargs)
        except Exception as e:
            if raise_on_error is None:
                raise_on_error = getattr(flt, 'raise_on_error', False)
            if raise_on_error:
                raise
            result = _default_result(value_or_values, flt.default_stat)
            on_error = getattr(flt, 'on_error', None)
            if on_error:
                on_error(e, result)
            flt.logger.warning(
                '去重操作失败，返回默认值：%s，错误详情：%s',
                flt.default_stat, str(e))
        return result

    if asyncio.iscoroutinefunction(func):
        return cast(F, async_wrapper)
    else:
        return cast(F, wrapper)


class Reset:
    def __init__(self, max_count: int, max_rate: float = 0.8,
                 reset_to_rate: float = 0.5, reset_type: Any = None,
                 monitor_time: int = 3600) -> None:
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
        self.flt: Any = None

    def _timer_active(self) -> bool:
        return time.monotonic() < self._next_reset_at

    def set_timer(self) -> None:
        with self._lock:
            self._next_reset_at = time.monotonic() + self.monitor_time

    def get_timer(self) -> bool:
        with self._lock:
            return self._timer_active()

    @property
    def used_rate(self) -> float:
        return self.current_count / self.max_count

    @property
    def current_count(self) -> int:
        raise NotImplementedError

    @property
    def reset_count(self) -> int:
        return self.current_count - int(self.max_count * self.reset_to_rate)

    def _reset(self) -> None:
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

    def reset(self) -> None:
        raise NotImplementedError


def decorate_reset(func: F) -> F:
    @functools.wraps(func)
    def wrapper(flt: Any, *args: Any, **kwargs: Any) -> Any:
        try:
            if flt.reset:
                flt.reset._reset()
        except Exception as e:
            flt.logger.warning(f"重置操作失败：{str(e)}")
        result = func(flt, *args, **kwargs)
        return result

    @functools.wraps(func)
    async def async_wrapper(flt: Any, *args: Any, **kwargs: Any) -> Any:
        try:
            if flt.reset:
                result = flt.reset._reset()
                if inspect.isawaitable(result):
                    await result
        except Exception as e:
            flt.logger.warning(f"重置操作失败：{str(e)}")
        result = await func(flt, *args, **kwargs)
        return result

    if asyncio.iscoroutinefunction(func):
        return cast(F, async_wrapper)
    else:
        return cast(F, wrapper)


class Filter:
    logger: logging.Logger

    def __init__(
            self,
            value_hash_func: Callable[[Any], Any] | None = utils.md5,
            value_compress_func: Callable[[Any], Any] | None = None,
            default_stat: bool = False,
            logger: logging.Logger | None = None,
            logger_level: int = logging.DEBUG,
            reset: Reset | None = None,
            raise_on_error: bool = False,
            on_error: Callable[[BaseException, Any], None] | None = None
    ) -> None:
        self.value_hash_func = value_hash_func or (lambda value: value)
        self.value_compress_func = value_compress_func or (lambda value: value)
        self.default_stat = bool(default_stat)
        self.raise_on_error = bool(raise_on_error)
        self.on_error = on_error
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

    def _log_exists(self, initial_values: Any,
                    handle_values: Any, stats: Any) -> None:
        for mixed in zip(stats, handle_values, initial_values):
            self.logger.info(
                '去重结果：%s，去重值：%s，原始值：%s',
                mixed[0], mixed[1], mixed[2])

    def exists(self, value: Any) -> Any:
        raise NotImplementedError

    def insert(self, value: Any) -> Any:
        raise NotImplementedError

    def exists_many(self, values: Iterable[Any]) -> Any:
        raise NotImplementedError

    def insert_many(self, values: Iterable[Any]) -> Any:
        raise NotImplementedError

    def exists_and_insert(self, value: Any) -> Any:
        raise NotImplementedError

    def exists_and_insert_many(self, values: Iterable[Any]) -> Any:
        raise NotImplementedError

    def _value_hash(self, value: Any) -> Any:
        return self.value_hash_func(value)

    def _value_compress(self, value: Any) -> Any:
        return self.value_compress_func(value)

    def _value_hash_and_compress(self, value: Any) -> Any:
        return self.value_compress_func(self.value_hash_func(value))

    def close(self) -> Any:
        pass

    def release(self, *args: Any, **kwargs: Any) -> Any:
        """
        释放去重种子
        :param args:
        :param kwargs:
        :return:
        """
        pass


class DefaultFilter:
    def __init__(self, default_stat: bool = False) -> None:
        self.default_stat = bool(default_stat)

    def exists(self, value: Any) -> Any:
        return self.default_stat

    def insert(self, value: Any) -> Any:
        return True

    def exists_many(self, values: Iterable[Any]) -> Any:
        return [self.exists(value) for value in values]

    def insert_many(self, values: Iterable[Any]) -> Any:
        return [self.insert(value) for value in values]

    def exists_and_insert(self, value: Any) -> Any:
        return self.default_stat

    def exists_and_insert_many(self, values: Iterable[Any]) -> Any:
        return [self.exists_and_insert(value) for value in values]

    def close(self) -> None:
        pass

    def release(self, *args: Any, **kwargs: Any) -> None:
        """
        释放去重种子
        :param args:
        :param kwargs:
        :return:
        """
        pass


class AsyncDefaultFilter(DefaultFilter):
    async def exists(self, *args: Any, **kwargs: Any) -> Any:
        return super().exists(*args, **kwargs)

    async def insert(self, *args: Any, **kwargs: Any) -> Any:
        return super().insert(*args, **kwargs)

    async def exists_many(self, *args: Any, **kwargs: Any) -> Any:
        return super().exists_many(*args, **kwargs)

    async def insert_many(self, *args: Any, **kwargs: Any) -> Any:
        return super().insert_many(*args, **kwargs)

    async def exists_and_insert(self, *args: Any, **kwargs: Any) -> Any:
        return super().exists_and_insert(
            *args, **kwargs)

    async def exists_and_insert_many(self, *args: Any, **kwargs: Any) -> Any:
        return super().exists_and_insert_many(
            *args, **kwargs)


class FilterCounter:
    def __init__(self, stats: Iterable[Any] | None = None) -> None:
        if not stats:
            stats = []
        self.stats: list[bool] = [bool(stat) for stat in stats]

    def count(self, stat: bool = True) -> int:
        result = [_stat for _stat in self.stats if _stat == bool(stat)]
        return len(result)

    def insert_stat(self, stat: Any) -> None:
        self.stats.append(bool(stat))

    def insert_stats(self, stats: Iterable[Any]) -> None:
        for stat in stats:
            self.insert_stat(stat)

    def any(self, stat: bool = True) -> bool:
        return self.count(stat) > 0

    def all(self, stat: bool = True) -> bool:
        return self.count(stat) == len(self.stats)

    def reach(self, count: int, stat: bool = True) -> bool:
        return self.count(stat) >= count

    def appeared(self, index: int, stat: bool = True) -> bool:
        return (1 <= index <= len(self.stats)
                and self.stats[index - 1] == stat)


class PageFilterCounter(FilterCounter):
    def __init__(self, stats: Iterable[Any] | None = None,
                 max_page: int | None = None,
                 current_page: int | None = None,
                 duplicate: bool = False) -> None:
        super().__init__(stats)
        self.max_page = max_page
        if not duplicate:
            self.max_page = None
        self.current_page = 1 if current_page == 0 else current_page

    def has_next(self) -> bool:
        if not self.max_page or not self.current_page:
            return True
        if self.current_page < self.max_page:
            return True
        return False
