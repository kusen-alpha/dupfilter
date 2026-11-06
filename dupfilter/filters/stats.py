# -*-coding:utf-8 -*-
from __future__ import annotations

import threading
from collections.abc import Iterable
from typing import Any


class StatsFilter:
    """Wraps a filter and tracks hit / miss statistics.

    A hit means the underlying judgment reported a duplicate; a miss means
    the value was new. Statistics cover the judging methods (``exists``,
    ``exists_and_insert`` and their ``_many`` variants). Backend failures
    stay inside the wrapped filter and are not counted here.
    """

    def __init__(self, flt: Any) -> None:
        self._flt = flt
        self.total = 0
        self.hits = 0
        self.misses = 0
        self._lock = threading.Lock()

    def _record(self, stat: Any) -> None:
        with self._lock:
            self.total += 1
            if bool(stat):
                self.hits += 1
            else:
                self.misses += 1

    def _record_many(self, stats: Iterable[Any]) -> None:
        for stat in stats:
            self._record(stat)

    def exists(self, value: Any, **kwargs: Any) -> Any:
        stat = self._flt.exists(value, **kwargs)
        self._record(stat)
        return stat

    def exists_many(self, values: Any, **kwargs: Any) -> Any:
        stats = self._flt.exists_many(values, **kwargs)
        self._record_many(stats)
        return stats

    def exists_and_insert(self, value: Any, **kwargs: Any) -> Any:
        stat = self._flt.exists_and_insert(value, **kwargs)
        self._record(stat)
        return stat

    def exists_and_insert_many(self, values: Any, **kwargs: Any) -> Any:
        stats = self._flt.exists_and_insert_many(values, **kwargs)
        self._record_many(stats)
        return stats

    def insert(self, value: Any, **kwargs: Any) -> Any:
        return self._flt.insert(value, **kwargs)

    def insert_many(self, values: Any, **kwargs: Any) -> Any:
        return self._flt.insert_many(values, **kwargs)

    @property
    def hit_rate(self) -> float:
        return self.hits / self.total if self.total else 0.0

    @property
    def miss_rate(self) -> float:
        return self.misses / self.total if self.total else 0.0

    def reset_stats(self) -> None:
        with self._lock:
            self.total = 0
            self.hits = 0
            self.misses = 0

    def count(self) -> int | None:
        """Dedup set size of the wrapped backend, when available."""
        dups = getattr(self._flt, "dups", None)
        if dups is not None:
            return len(dups)
        count = getattr(self._flt, "count", None)
        if count is None:
            return None
        try:
            return int(count())
        except Exception:
            return None

    def __getattr__(self, name: str) -> Any:
        return getattr(self._flt, name)

    def close(self) -> None:
        self._flt.close()


class AsyncStatsFilter:
    """Async version of :class:`StatsFilter`."""

    def __init__(self, flt: Any) -> None:
        self._flt = flt
        self.total = 0
        self.hits = 0
        self.misses = 0
        self._lock = threading.Lock()

    def _record(self, stat: Any) -> None:
        with self._lock:
            self.total += 1
            if bool(stat):
                self.hits += 1
            else:
                self.misses += 1

    async def exists(self, value: Any, **kwargs: Any) -> Any:
        stat = await self._flt.exists(value, **kwargs)
        self._record(stat)
        return stat

    async def exists_many(self, values: Any, **kwargs: Any) -> Any:
        stats = await self._flt.exists_many(values, **kwargs)
        for stat in stats:
            self._record(stat)
        return stats

    async def exists_and_insert(self, value: Any, **kwargs: Any) -> Any:
        stat = await self._flt.exists_and_insert(value, **kwargs)
        self._record(stat)
        return stat

    async def exists_and_insert_many(self, values: Any,
                                     **kwargs: Any) -> Any:
        stats = await self._flt.exists_and_insert_many(values, **kwargs)
        for stat in stats:
            self._record(stat)
        return stats

    async def insert(self, value: Any, **kwargs: Any) -> Any:
        return await self._flt.insert(value, **kwargs)

    async def insert_many(self, values: Any, **kwargs: Any) -> Any:
        return await self._flt.insert_many(values, **kwargs)

    @property
    def hit_rate(self) -> float:
        return self.hits / self.total if self.total else 0.0

    @property
    def miss_rate(self) -> float:
        return self.misses / self.total if self.total else 0.0

    def reset_stats(self) -> None:
        with self._lock:
            self.total = 0
            self.hits = 0
            self.misses = 0

    async def count(self) -> int | None:
        dups = getattr(self._flt, "dups", None)
        if dups is not None:
            return len(dups)
        count = getattr(self._flt, "count", None)
        if count is None:
            return None
        try:
            return int(await count())
        except Exception:
            return None

    def __getattr__(self, name: str) -> Any:
        return getattr(self._flt, name)

    async def close(self) -> None:
        await self._flt.close()
