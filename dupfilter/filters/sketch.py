# -*-coding:utf-8 -*-
from __future__ import annotations

import math
import threading
from typing import Any

from dupfilter.filters.bloom import _md5_int


def _alpha(m: int) -> float:
    if m < 16:
        return 0.673
    if m < 32:
        return 0.697
    if m < 64:
        return 0.709
    return 0.7213 / (1 + 1.079 / m)


class HyperLogLog:
    """Cardinality estimator: add values, then read an approximate count.

    With the default ``precision=14`` (16384 registers) the relative error
    is about 1.04 / sqrt(16384) ~= 0.8%. Useful alongside dedup to answer
    "how many distinct items" without keeping the full set.
    """

    def __init__(self, precision: int = 14) -> None:
        if not 4 <= precision <= 18:
            raise ValueError('precision must be in [4, 18]')
        self.precision = precision
        self._m = 1 << precision
        self._alpha = _alpha(self._m)
        self._registers = bytearray(self._m)
        self._lock = threading.Lock()

    @staticmethod
    def _rank(hash_value: int) -> int:
        if not hash_value:
            return 65
        return 65 - hash_value.bit_length()

    def add(self, value: Any) -> None:
        h = _md5_int(value)
        index = h & (self._m - 1)
        rank = self._rank((h >> self.precision) & 0xFFFFFFFFFFFFFFFF)
        with self._lock:
            if rank > self._registers[index]:
                self._registers[index] = rank

    def add_many(self, values: Any) -> None:
        for value in values:
            self.add(value)

    def count(self) -> float:
        with self._lock:
            registers = bytes(self._registers)
        m = self._m
        estimate = self._alpha * m * m / sum(
            2.0 ** -register for register in registers)
        zeros = registers.count(0)
        if zeros:
            estimate = m * math.log(m / zeros)
        return estimate

    def clear(self) -> None:
        with self._lock:
            self._registers = bytearray(self._m)


class CountMinSketch:
    """Frequency estimator: add weighted events, then read approximate
    counts. Underestimates are impossible; estimates may over-count due to
    hash collisions, bounded by the configured width/depth."""

    def __init__(self, width: int = 10000, depth: int = 5) -> None:
        if width <= 0 or depth <= 0:
            raise ValueError('width and depth must be > 0')
        self.width = width
        self.depth = depth
        self._counts = [[0] * width for _ in range(depth)]
        self._lock = threading.RLock()

    @classmethod
    def from_error_rate(cls, expected_items: int,
                        error_rate: float = 0.001,
                        confidence: float = 0.99) -> CountMinSketch:
        if expected_items <= 0:
            raise ValueError('expected_items must be > 0')
        if not 0 < error_rate < 1 or not 0 < confidence < 1:
            raise ValueError('error_rate and confidence must be in (0, 1)')
        width = max(1, math.ceil(expected_items / error_rate))
        depth = max(1, math.ceil(math.log(1 / (1 - confidence))))
        return cls(width, depth)

    def _indexes(self, value: Any) -> list[int]:
        h = _md5_int(value)
        h1 = h & 0xFFFFFFFFFFFFFFFF
        h2 = (h >> 64) & 0xFFFFFFFFFFFFFFFF
        if not h2:
            h2 = 0x9E3779B97F4A7C15
        return [(h1 + row * h2) % self.width
                for row in range(self.depth)]

    def add(self, value: Any, count: int = 1) -> None:
        if count < 0:
            raise ValueError('count must be >= 0')
        with self._lock:
            for row, index in enumerate(self._indexes(value)):
                self._counts[row][index] += count

    def add_many(self, values: Any, count: int = 1) -> None:
        for value in values:
            self.add(value, count)

    def estimate(self, value: Any) -> int:
        with self._lock:
            return min(self._counts[row][index]
                       for row, index in enumerate(self._indexes(value)))

    def clear(self) -> None:
        with self._lock:
            self._counts = [[0] * self.width for _ in range(self.depth)]


class AsyncHyperLogLog:
    """Async API wrapper over :class:`HyperLogLog`."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._sketch: Any = HyperLogLog(*args, **kwargs)

    async def add(self, value: Any) -> None:
        self._sketch.add(value)

    async def add_many(self, values: Any) -> None:
        self._sketch.add_many(values)

    async def count(self) -> float:
        return float(self._sketch.count())

    async def clear(self) -> None:
        self._sketch.clear()


class AsyncCountMinSketch:
    """Async API wrapper over :class:`CountMinSketch`."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._sketch: Any = CountMinSketch(*args, **kwargs)

    async def add(self, value: Any, count: int = 1) -> None:
        self._sketch.add(value, count)

    async def add_many(self, values: Any, count: int = 1) -> None:
        self._sketch.add_many(values, count)

    async def estimate(self, value: Any) -> int:
        return int(self._sketch.estimate(value))

    async def clear(self) -> None:
        self._sketch.clear()
