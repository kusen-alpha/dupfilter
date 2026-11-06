# -*-coding:utf-8 -*-
from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from dupfilter.filters import decorate_warning
from dupfilter.filters.redis import RedisFilter


class RedisBloomNativeFilter(RedisFilter):
    """Bloom filter backed by the RedisBloom server module.

    Uses the native ``BF.*`` commands, so capacity growth and metadata
    (``BF.INFO``) live on the server side. The key is reserved with
    ``BF.RESERVE`` on construction unless ``auto_create=False``.
    """

    def __init__(self, server: Any, key: str,
                 error_rate: float = 0.01, capacity: int = 100000,
                 expansion: int = 2, nonscaling: bool = False,
                 auto_create: bool = True,
                 *args: Any, **kwargs: Any) -> None:
        if not 0 < error_rate < 1:
            raise ValueError('error_rate must be in (0, 1)')
        if capacity <= 0:
            raise ValueError('capacity must be > 0')
        self.key = key
        self.error_rate = error_rate
        self.capacity = capacity
        self.expansion = expansion
        self.nonscaling = nonscaling
        super().__init__(server, *args, **kwargs)
        if auto_create:
            self.reserve()

    def _reserve_command(self) -> list[Any]:
        command = ["BF.RESERVE", self.key, self.error_rate, self.capacity,
                   "EXPANSION", self.expansion]
        if self.nonscaling:
            command.append("NONSCALING")
        return command

    def reserve(self) -> Any:
        """Create the key with ``BF.RESERVE``; a pre-existing key is fine."""
        try:
            self.server.execute_command(*self._reserve_command())
        except Exception as exc:
            message = str(exc).lower()
            if "exists" in message or "already" in message:
                return
            raise

    @decorate_warning
    def exists(self, value: Any) -> Any:
        result = self.server.execute_command("BF.EXISTS", self.key, value)
        return bool(result)

    @decorate_warning
    def exists_many(self, values: Iterable[Any]) -> Any:
        values = list(values)
        raw = self.server.execute_command("BF.MEXISTS", self.key, *values)
        stats = [bool(stat) for stat in raw]
        self._log_exists(values, values, stats)
        return stats

    @decorate_warning
    def insert(self, value: Any) -> Any:
        self.server.execute_command("BF.ADD", self.key, value)
        return True

    @decorate_warning
    def insert_many(self, values: Iterable[Any]) -> Any:
        values = list(values)
        self.server.execute_command("BF.MADD", self.key, *values)
        return [True for _ in values]

    @decorate_warning
    def exists_and_insert(self, value: Any) -> Any:
        result = self.server.execute_command("BF.ADD", self.key, value)
        return int(result) == 0

    @decorate_warning
    def exists_and_insert_many(self, values: Iterable[Any]) -> Any:
        values = list(values)
        raw = self.server.execute_command("BF.MADD", self.key, *values)
        stats = [int(stat) == 0 for stat in raw]
        self._log_exists(values, values, stats)
        return stats

    def info(self) -> Any:
        raw = self.server.execute_command("BF.INFO", self.key)
        return dict(zip(raw[0::2], raw[1::2]))

    def count(self) -> Any:
        info = self.info()
        for name in ("NumberOfItemsInserted", b"NumberOfItemsInserted"):
            if name in info:
                return int(info[name])
        return 0

    # Two-phase claim/confirm mirrors the in-house bloom: bits are shared,
    # so release() is not supported.
    exists_and_lock = exists_and_insert
    exists_and_lock_many = exists_and_insert_many
    confirm = insert
    confirm_many = insert_many

    def release(self, value: Any) -> Any:
        raise NotImplementedError(
            "bloom bits are shared; release is not supported")

    def release_many(self, values: Iterable[Any]) -> Any:
        raise NotImplementedError(
            "bloom bits are shared; release is not supported")


class AsyncRedisBloomNativeFilter(RedisBloomNativeFilter):
    """Async variant. ``BF.RESERVE`` cannot run in ``__init__`` because the
    driver call is a coroutine, so the first operation reserves lazily when
    ``auto_create=True``."""

    def __init__(self, server: Any, key: str,
                 auto_create: bool = True,
                 *args: Any, **kwargs: Any) -> None:
        self._pending_reserve = bool(auto_create)
        kwargs["auto_create"] = False
        super().__init__(server, key, *args, **kwargs)

    async def _ensure_reserved(self) -> None:
        if not self._pending_reserve:
            return
        try:
            await self.server.execute_command(*self._reserve_command())
        except Exception as exc:
            message = str(exc).lower()
            if "exists" not in message and "already" not in message:
                raise
        self._pending_reserve = False

    async def reserve(self) -> None:
        await self._ensure_reserved()

    @decorate_warning
    async def exists(self, value: Any) -> bool:
        await self._ensure_reserved()
        result = await self.server.execute_command(
            "BF.EXISTS", self.key, value)
        return bool(result)

    @decorate_warning
    async def exists_many(self, values: Iterable[Any]) -> list[bool]:
        await self._ensure_reserved()
        values = list(values)
        raw = await self.server.execute_command(
            "BF.MEXISTS", self.key, *values)
        stats = [bool(stat) for stat in raw]
        self._log_exists(values, values, stats)
        return stats

    @decorate_warning
    async def insert(self, value: Any) -> bool:
        await self._ensure_reserved()
        await self.server.execute_command("BF.ADD", self.key, value)
        return True

    @decorate_warning
    async def insert_many(self, values: Iterable[Any]) -> list[bool]:
        await self._ensure_reserved()
        values = list(values)
        await self.server.execute_command("BF.MADD", self.key, *values)
        return [True for _ in values]

    @decorate_warning
    async def exists_and_insert(self, value: Any) -> bool:
        await self._ensure_reserved()
        result = await self.server.execute_command("BF.ADD", self.key, value)
        return int(result) == 0

    @decorate_warning
    async def exists_and_insert_many(self, values: Iterable[Any]
                                     ) -> list[bool]:
        await self._ensure_reserved()
        values = list(values)
        raw = await self.server.execute_command("BF.MADD", self.key, *values)
        stats = [int(stat) == 0 for stat in raw]
        self._log_exists(values, values, stats)
        return stats

    async def info(self) -> dict[Any, Any]:
        await self._ensure_reserved()
        raw = await self.server.execute_command("BF.INFO", self.key)
        return dict(zip(raw[0::2], raw[1::2]))

    async def count(self) -> int:
        info = await self.info()
        for name in ("NumberOfItemsInserted", b"NumberOfItemsInserted"):
            if name in info:
                return int(info[name])
        return 0

    async def release(self, value: Any) -> Any:
        raise NotImplementedError(
            "bloom bits are shared; release is not supported")

    async def release_many(self, values: Iterable[Any]) -> Any:
        raise NotImplementedError(
            "bloom bits are shared; release is not supported")
