# -*- coding:utf-8 -*-
from __future__ import annotations

import hashlib
import math
from collections.abc import Iterable
from typing import Any

from dupfilter.filters import Filter, Reset, decorate_reset, decorate_warning

_GOLDEN = 0x9E3779B97F4A7C15


def _md5_int(value: Any) -> int:
    data = value if isinstance(value, bytes) else str(value).encode("utf-8")
    return int.from_bytes(hashlib.md5(data).digest(), "big")


def _hash_indexes(value: Any, bit_count: int, hash_num: int) -> list[int]:
    """Double hashing from one md5 digest: h_i = h1 + i * h2 (mod m)."""
    h = _md5_int(value)
    h1 = h & 0xFFFFFFFFFFFFFFFF
    h2 = (h >> 64) & 0xFFFFFFFFFFFFFFFF
    if not h2:
        h2 = _GOLDEN
    return [(h1 + i * h2) % bit_count for i in range(1, hash_num + 1)]


class BloomReset(Reset):
    """Bloom filters cannot drop single elements, so reset clears the bitmap."""

    @property
    def current_count(self) -> int:
        return int(self.flt.count())

    def reset(self) -> None:
        self.flt.clear()


class MemoryBloomFilter(Filter):
    """Local, zero-dependency bloom filter.

    Sizing can be derived from ``expected_items`` and
    ``false_positive_rate``, or passed explicitly via ``bit`` / ``hash_num``.
    Deletion is not supported because bits are shared between values.
    """

    def __init__(self, expected_items: int | None = None,
                 false_positive_rate: float | None = None,
                 bit: int | None = None, hash_num: int | None = None,
                 *args: Any, **kwargs: Any) -> None:
        if expected_items is not None or bit is None:
            if expected_items is None or expected_items <= 0:
                raise ValueError('expected_items must be > 0')
            if false_positive_rate is None or not 0 < false_positive_rate < 1:
                raise ValueError('false_positive_rate must be in (0, 1)')
            bits = -expected_items * math.log(false_positive_rate) / (
                math.log(2) ** 2)
            bit = max(1, math.ceil(bits))
            hash_num = max(1, round(bits / expected_items * math.log(2)))
        if bit is None or bit <= 0:
            raise ValueError('bit must be > 0')
        if hash_num is None or hash_num <= 0:
            raise ValueError('hash_num must be > 0')
        self.bit_count = bit
        self.hash_num = hash_num
        self._bits = bytearray(math.ceil(bit / 8))
        self._inserted = 0
        self.expected_items = expected_items
        super().__init__(*args, **kwargs)

    def _indexes(self, value: Any) -> list[int]:
        return _hash_indexes(
            self._value_hash_and_compress(value), self.bit_count, self.hash_num)

    def _set(self, index: int) -> None:
        self._bits[index >> 3] |= 1 << (index & 7)

    def _test(self, index: int) -> bool:
        return bool(self._bits[index >> 3] & (1 << (index & 7)))

    @decorate_warning
    def exists(self, value: Any) -> bool:
        return all(self._test(i) for i in self._indexes(value))

    @decorate_warning
    def exists_many(self, values: Iterable[Any]) -> list[bool]:
        return [self.exists(value) for value in values]

    @decorate_warning
    @decorate_reset
    def insert(self, value: Any) -> bool:
        for index in self._indexes(value):
            self._set(index)
        self._inserted += 1
        return True

    @decorate_warning
    def insert_many(self, values: Iterable[Any]) -> list[bool]:
        return [self.insert(value) for value in values]

    @decorate_warning
    @decorate_reset
    def exists_and_insert(self, value: Any) -> bool:
        indexes = self._indexes(value)
        stat = all(self._test(i) for i in indexes)
        if not stat:
            for index in indexes:
                self._set(index)
            self._inserted += 1
        return stat

    @decorate_warning
    def exists_and_insert_many(self, values: Iterable[Any]) -> list[bool]:
        return [self.exists_and_insert(value) for value in values]

    @property
    def used_rate(self) -> float:
        capacity = self.expected_items or self.bit_count
        return min(1.0, self._inserted / max(1, capacity))

    def count(self) -> int:
        return self._inserted

    def clear(self) -> None:
        self._bits = bytearray(math.ceil(self.bit_count / 8))
        self._inserted = 0


class ScalableBloomFilter(Filter):
    """Bloom that grows: once the active filter fills up, a new one is
    appended with larger capacity and a tighter error rate. Deletion and
    reset are not supported; :meth:`clear` drops every layer.
    """

    def __init__(self, initial_capacity: int = 100000,
                 false_positive_rate: float = 0.01, growth: float = 2.0,
                 tightening_ratio: float = 0.9,
                 *args: Any, **kwargs: Any) -> None:
        if initial_capacity <= 0:
            raise ValueError('initial_capacity must be > 0')
        if not 0 < false_positive_rate < 1:
            raise ValueError('false_positive_rate must be in (0, 1)')
        if growth <= 1:
            raise ValueError('growth must be > 1')
        if not 0 < tightening_ratio < 1:
            raise ValueError('tightening_ratio must be in (0, 1)')
        self.initial_capacity = initial_capacity
        self.false_positive_rate = false_positive_rate
        self.growth = growth
        self.tightening_ratio = tightening_ratio
        self._filters: list[MemoryBloomFilter] = []
        super().__init__(*args, **kwargs)

    def _active(self) -> MemoryBloomFilter:
        if not self._filters:
            self._filters.append(MemoryBloomFilter(
                expected_items=self.initial_capacity,
                false_positive_rate=self.false_positive_rate,
                logger=self.logger, raise_on_error=self.raise_on_error))
        return self._filters[-1]

    @decorate_warning
    def exists(self, value: Any) -> bool:
        return any(flt.exists(value) for flt in self._filters)

    @decorate_warning
    def exists_many(self, values: Iterable[Any]) -> list[bool]:
        return [self.exists(value) for value in values]

    @decorate_warning
    def insert(self, value: Any) -> bool:
        active = self._active()
        if active.used_rate >= 0.8:
            capacity = int(self.initial_capacity * (
                self.growth ** len(self._filters)))
            self._filters.append(MemoryBloomFilter(
                expected_items=capacity,
                false_positive_rate=self.false_positive_rate * (
                    self.tightening_ratio ** len(self._filters)),
                logger=self.logger, raise_on_error=self.raise_on_error))
            active = self._filters[-1]
        return active.insert(value)

    @decorate_warning
    def insert_many(self, values: Iterable[Any]) -> list[bool]:
        return [self.insert(value) for value in values]

    @decorate_warning
    def exists_and_insert(self, value: Any) -> bool:
        stat = self.exists(value)
        if not stat:
            self.insert(value)
        return stat

    @decorate_warning
    def exists_and_insert_many(self, values: Iterable[Any]) -> list[bool]:
        return [self.exists_and_insert(value) for value in values]

    def count(self) -> int:
        return sum(flt.count() for flt in self._filters)

    def clear(self) -> None:
        self._filters = []


class AsyncMemoryBloomFilter:
    """Async API wrapper over :class:`MemoryBloomFilter`.

    Pure in-memory work with no I/O, so methods delegate and return
    immediately (no executor).
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._flt: Any = MemoryBloomFilter(*args, **kwargs)

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

    async def count(self) -> int:
        return int(self._flt.count())

    async def close(self) -> None:
        self._flt.close()


class AsyncScalableBloomFilter(AsyncMemoryBloomFilter):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._flt: Any = ScalableBloomFilter(*args, **kwargs)
