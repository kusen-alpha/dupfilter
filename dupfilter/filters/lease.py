# -*-coding:utf-8 -*-
from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable, Iterable
from typing import Any


def _default_logger() -> logging.Logger:
    return logging.getLogger("dupfilter")


class LeaseFilter:
    """Two-phase claim/confirm/release with an automatic lease expiry.

    Wraps any backend filter that supports release (SQL, Redis Set /
    SortedSet / String). ``exists_and_lock`` records a lease in-process;
    when the lease expires before ``confirm``, the backend claim is
    released so the value can be reclaimed and retried. The registry is
    process-local - use a TTL-capable backend (e.g. Redis String) when
    leases must survive a process crash.
    """

    def __init__(self, flt: Any, default_lease: float = 300.0,
                 clock: Callable[[], float] | None = None,
                 logger: logging.Logger | None = None) -> None:
        self._flt = flt
        self._default_lease = float(default_lease)
        self._clock = clock or time.monotonic
        self._logger = logger or _default_logger()
        self._leases: dict[str, float] = {}
        self._lock = threading.Lock()

    def _key(self, value: Any) -> str:
        hasher = getattr(self._flt, "_value_hash_and_compress", None)
        if hasher is not None:
            try:
                return str(hasher(value))
            except Exception:
                pass
        return str(value)

    def _release_backend(self, value: Any) -> None:
        release = getattr(self._flt, "release", None)
        if release is None:
            return
        try:
            release(value)
        except NotImplementedError as exc:
            self._logger.warning(
                "租约过期但后端不支持释放：%s", str(exc))

    def exists_and_lock(self, value: Any, lease: float | None = None) -> bool:
        lease = self._default_lease if lease is None else float(lease)
        key = self._key(value)
        now = self._clock()
        with self._lock:
            expiry = self._leases.get(key)
            if expiry is not None:
                if expiry > now:
                    return True
                self._leases.pop(key, None)
                self._release_backend(value)
        stat = self._flt.exists_and_lock(value)
        if not stat:
            with self._lock:
                self._leases[key] = now + lease
        return bool(stat)

    def exists_and_lock_many(self, values: Iterable[Any],
                             lease: float | None = None) -> list[bool]:
        return [self.exists_and_lock(value, lease) for value in values]

    def confirm(self, value: Any) -> Any:
        with self._lock:
            self._leases.pop(self._key(value), None)
        return self._flt.confirm(value)

    def release(self, value: Any) -> Any:
        with self._lock:
            self._leases.pop(self._key(value), None)
        return self._flt.release(value)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._flt, name)

    def count(self) -> int | None:
        count = getattr(self._flt, "count", None)
        if count is None:
            return None
        try:
            return int(count())
        except Exception:
            return None

    def close(self) -> None:
        self._flt.close()


class AsyncLeaseFilter:
    """Async version of :class:`LeaseFilter`."""

    def __init__(self, flt: Any, default_lease: float = 300.0,
                 clock: Callable[[], float] | None = None,
                 logger: logging.Logger | None = None) -> None:
        self._flt = flt
        self._default_lease = float(default_lease)
        self._clock = clock or time.monotonic
        self._logger = logger or _default_logger()
        self._leases: dict[str, float] = {}
        self._lock = threading.Lock()

    def _key(self, value: Any) -> str:
        hasher = getattr(self._flt, "_value_hash_and_compress", None)
        if hasher is not None:
            try:
                return str(hasher(value))
            except Exception:
                pass
        return str(value)

    async def _release_backend(self, value: Any) -> None:
        release = getattr(self._flt, "release", None)
        if release is None:
            return
        try:
            await release(value)
        except NotImplementedError as exc:
            self._logger.warning(
                "租约过期但后端不支持释放：%s", str(exc))

    async def exists_and_lock(self, value: Any,
                              lease: float | None = None) -> bool:
        lease = self._default_lease if lease is None else float(lease)
        key = self._key(value)
        now = self._clock()
        release_needed = False
        with self._lock:
            expiry = self._leases.get(key)
            if expiry is not None:
                if expiry > now:
                    return True
                self._leases.pop(key, None)
                release_needed = True
        if release_needed:
            await self._release_backend(value)
        stat = await self._flt.exists_and_lock(value)
        if not stat:
            with self._lock:
                self._leases[key] = now + lease
        return bool(stat)

    async def exists_and_lock_many(self, values: Iterable[Any],
                                   lease: float | None = None
                                   ) -> list[bool]:
        return [await self.exists_and_lock(value, lease) for value in values]

    async def confirm(self, value: Any) -> Any:
        with self._lock:
            self._leases.pop(self._key(value), None)
        return await self._flt.confirm(value)

    async def release(self, value: Any) -> Any:
        with self._lock:
            self._leases.pop(self._key(value), None)
        return await self._flt.release(value)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._flt, name)

    async def count(self) -> int | None:
        count = getattr(self._flt, "count", None)
        if count is None:
            return None
        try:
            return int(await count())
        except Exception:
            return None

    async def close(self) -> None:
        await self._flt.close()
