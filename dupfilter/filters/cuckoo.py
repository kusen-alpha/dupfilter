# -*- coding:utf-8 -*-
from __future__ import annotations

import math
import random
from collections.abc import Iterable
from typing import Any

from dupfilter.filters import Filter, decorate_warning
from dupfilter.filters.bloom import _md5_int

_EMPTY = b"\x00"


class CuckooFullError(RuntimeError):
    """Raised when the cuckoo filter has no free slot left to place a value."""


class CuckooFilter(Filter):
    """A compact probabilistic filter that supports deletion.

    Unlike bloom filters, values are stored as short fingerprints, so
    :meth:`release` / :meth:`release_many` can remove a value again.
    Insertion may kick existing fingerprints around; when no slot can be
    found after ``max_kicks``, :class:`CuckooFullError` is raised.
    """

    def __init__(self, expected_items: int = 100000,
                 false_positive_rate: float = 0.01,
                 bucket_size: int = 4, fingerprint_bits: int = 16,
                 max_kicks: int = 500, *args: Any, **kwargs: Any) -> None:
        if expected_items <= 0:
            raise ValueError('expected_items must be > 0')
        if not 0 < false_positive_rate < 1:
            raise ValueError('false_positive_rate must be in (0, 1)')
        if bucket_size <= 0:
            raise ValueError('bucket_size must be > 0')
        target = expected_items / (bucket_size * 0.95)
        num_buckets = 1 << max(0, math.ceil(math.log2(max(target, 1))))
        fbits = max(
            8, math.ceil(math.log2(2 * bucket_size / false_positive_rate)))
        if fingerprint_bits is not None and fingerprint_bits > 0:
            fbits = max(fbits, fingerprint_bits)
        self.bucket_size = bucket_size
        self.fingerprint_bytes = math.ceil(fbits / 8)
        self.num_buckets = num_buckets
        self.mask = num_buckets - 1
        self.max_kicks = max_kicks
        self._stride = self.bucket_size * self.fingerprint_bytes
        self._buckets = [
            bytearray(self._stride) for _ in range(num_buckets)]
        self._occupied = bytearray(num_buckets)
        super().__init__(*args, **kwargs)

    def _fingerprint(self, value: Any) -> bytes:
        h = _md5_int(self._value_hash_and_compress(value))
        return h.to_bytes(16, 'big')[0:self.fingerprint_bytes]

    def _indexes(self, value: Any) -> tuple[int, int]:
        h = _md5_int(self._value_hash_and_compress(value))
        i1 = h & self.mask
        fp = h.to_bytes(16, 'big')[0:self.fingerprint_bytes]
        i2 = i1 ^ (_md5_int(fp) & self.mask)
        return i1, i2

    def _slot(self, bucket: int, slot: int) -> bytes:
        start = slot * self.fingerprint_bytes
        return bytes(self._buckets[bucket][start:start + self.fingerprint_bytes])

    def _set_slot(self, bucket: int, slot: int, fp: bytes) -> None:
        start = slot * self.fingerprint_bytes
        self._buckets[bucket][start:start + self.fingerprint_bytes] = fp

    def _has(self, bucket: int, fp: bytes) -> bool:
        return any(self._slot(bucket, s) == fp
                   for s in range(self.bucket_size))

    def _place(self, bucket: int, fp: bytes) -> bool:
        if self._has(bucket, fp):
            return True
        for slot in range(self.bucket_size):
            if self._slot(bucket, slot) == _EMPTY * self.fingerprint_bytes:
                self._set_slot(bucket, slot, fp)
                self._occupied[bucket] += 1
                return True
        return False

    def _insert_fp(self, fp: bytes, i1: int, i2: int) -> None:
        if self._place(i1, fp) or self._place(i2, fp):
            return
        bucket = i1 if random.random() < 0.5 else i2
        for _ in range(self.max_kicks):
            slot = random.randrange(self.bucket_size)
            evicted = self._slot(bucket, slot)
            self._set_slot(bucket, slot, fp)
            if evicted == _EMPTY * self.fingerprint_bytes:
                self._occupied[bucket] += 1
                return
            alternate = bucket ^ (_md5_int(evicted) & self.mask)
            if alternate == bucket:
                alternate = (bucket + 1) & self.mask
            if self._place(alternate, evicted):
                return
            bucket = alternate
            fp = evicted
        raise CuckooFullError(
            'cuckoo filter is full, raise expected_items or clear()')

    @decorate_warning
    def exists(self, value: Any) -> bool:
        fp = self._fingerprint(value)
        i1, i2 = self._indexes(value)
        return self._has(i1, fp) or self._has(i2, fp)

    @decorate_warning
    def exists_many(self, values: Iterable[Any]) -> list[bool]:
        return [self.exists(value) for value in values]

    @decorate_warning
    def insert(self, value: Any) -> bool:
        if not self.exists(value):
            self._insert_fp(self._fingerprint(value), *self._indexes(value))
        return True

    @decorate_warning
    def insert_many(self, values: Iterable[Any]) -> list[bool]:
        return [self.insert(value) for value in values]

    @decorate_warning
    def exists_and_insert(self, value: Any) -> bool:
        stat = self.exists(value)
        if not stat:
            self._insert_fp(self._fingerprint(value), *self._indexes(value))
        return stat

    @decorate_warning
    def exists_and_insert_many(self, values: Iterable[Any]) -> list[bool]:
        return [self.exists_and_insert(value) for value in values]

    @decorate_warning
    def release(self, value: Any) -> bool:
        fp = self._fingerprint(value)
        i1, i2 = self._indexes(value)
        for bucket in (i1, i2):
            for slot in range(self.bucket_size):
                if self._slot(bucket, slot) == fp:
                    self._set_slot(bucket, slot, _EMPTY * self.fingerprint_bytes)
                    self._occupied[bucket] -= 1
                    return True
        return False

    @decorate_warning
    def release_many(self, values: Iterable[Any]) -> list[bool]:
        return [self.release(value) for value in values]

    def count(self) -> int:
        return sum(self._occupied)

    def clear(self) -> None:
        self._buckets = [
            bytearray(self._stride) for _ in range(self.num_buckets)]
        self._occupied = bytearray(self.num_buckets)


class AsyncCuckooFilter:
    """Async API wrapper over :class:`CuckooFilter`."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._flt: Any = CuckooFilter(*args, **kwargs)

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

    async def release(self, value: Any, **kwargs: Any) -> Any:
        return self._flt.release(value, **kwargs)

    async def release_many(self, values: Any, **kwargs: Any) -> Any:
        return self._flt.release_many(values, **kwargs)

    async def count(self) -> int:
        return int(self._flt.count())

    async def close(self) -> None:
        self._flt.close()
