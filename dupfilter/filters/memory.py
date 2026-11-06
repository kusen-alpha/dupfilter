# -*- coding:utf-8 -*-
# author: kusen
# email: 1194542196@qq.com
# date: 2023/9/7

from __future__ import annotations

import random
from typing import Any

from dupfilter.filters import Filter, Reset, decorate_reset, decorate_warning


class MemoryReset(Reset):
    @property
    def current_count(self) -> int:
        return len(self.flt.dups)

    def reset(self) -> None:
        rm_items = set(random.sample(self.flt.dups, self.reset_count))
        self.flt.dups -= rm_items


class MemoryFilter(Filter):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.dups: set[Any] = set()
        super().__init__(*args, **kwargs)

    @decorate_warning
    def exists(self, value: Any) -> bool:
        new_value = self._value_hash_and_compress(value)
        stat = new_value in self.dups
        self._log_exists([value], [new_value], [stat])
        return stat

    @decorate_warning
    def exists_many(self, values: Any) -> list[bool]:
        return [self.exists(value) for value in values]

    @decorate_warning
    @decorate_reset
    def insert(self, value: Any) -> bool:
        new_value = self._value_hash_and_compress(value)
        self.dups.add(new_value)
        return True

    @decorate_warning
    def insert_many(self, values: Any) -> list[bool]:
        return [self.insert(value) for value in values]

    @decorate_warning
    @decorate_reset
    def exists_and_insert(self, value: Any) -> bool:
        new_value = self._value_hash_and_compress(value)
        stat = new_value in self.dups
        self._log_exists([value], [new_value], [stat])
        if not stat:
            self.dups.add(new_value)
        return stat

    @decorate_warning
    def exists_and_insert_many(self, values: Any) -> list[bool]:
        return [self.exists_and_insert(value) for value in values]


class AsyncMemoryFilter:
    """Async API wrapper over :class:`MemoryFilter`.

    Memory dedup is pure in-memory work with no I/O, so the async
    methods simply delegate and return immediately (no executor).
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._flt: Any = MemoryFilter(*args, **kwargs)

    @property
    def dups(self) -> Any:
        return self._flt.dups

    async def exists(self, value: Any, **kwargs: Any) -> Any:
        return self._flt.exists(value, **kwargs)

    async def exists_many(self, values: Any, **kwargs: Any) -> Any:
        return self._flt.exists_many(values, **kwargs)

    async def insert(self, value: Any, **kwargs: Any) -> Any:
        return self._flt.insert(value, **kwargs)

    async def insert_many(self, values: Any, **kwargs: Any) -> Any:
        return self._flt.insert_many(values, **kwargs)

    async def exists_and_insert(self, value: Any, **kwargs: Any) -> Any:
        return self._flt.exists_and_insert(value, **kwargs)

    async def exists_and_insert_many(self, values: Any,
                                     **kwargs: Any) -> Any:
        return self._flt.exists_and_insert_many(values, **kwargs)

    async def close(self) -> None:
        self._flt.close()
