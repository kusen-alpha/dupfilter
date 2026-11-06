# -*- coding:utf-8 -*-
# author: kusen
# email: 1194542196@qq.com
# date: 2023/9/6

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from dupfilter.filters import decorate_warning
from dupfilter.filters.redis import RedisFilter

EXISTS_SCRIPT = """
local keys = KEYS
local expire = ARGV[1]
local result = {}
for _, key in pairs(keys) do
    local exist = redis.call('SETNX', key, '')
    if exist==1 then
        table.insert(result, 0)
        redis.call('EXPIRE', key, expire)
    else
        table.insert(result, 1)
    end
end
return result

"""
INSERT_SCRIPT = """
local keys = KEYS
local expire = ARGV[1]
local result = {}
for _, key in pairs(keys) do
    local exist = redis.call('EXISTS', key)
    table.insert(result, exist)
    if exist==1 then
        redis.call('EXPIRE', key, expire)
    end
end
return result
"""


class RedisStringFilter(RedisFilter):
    def __init__(self, server: Any, *args: Any, **kwargs: Any) -> None:
        super().__init__(server, *args, **kwargs)
        self.exists_script = self.server.register_script(EXISTS_SCRIPT)
        self.insert_script = self.server.register_script(INSERT_SCRIPT)

    @decorate_warning
    def exists(self, value: Any, expire: int = 7200) -> Any:
        return self.exists_many([value], expire)[0]

    @decorate_warning
    def exists_many(self, values: Iterable[Any],
                    expire: int = 7200) -> Any:
        keys = [self._value_hash_and_compress(value) for value in values]
        raw = self.exists_script(keys=keys, args=[expire])
        stats = [bool(stat) for stat in raw]
        self._log_exists(values, keys, stats)
        return stats

    @decorate_warning
    def insert(self, value: Any, expire: int = 2592000) -> Any:
        return self.insert_many([value], expire)[0]

    @decorate_warning
    def insert_many(self, values: Iterable[Any],
                    expire: int = 2592000) -> Any:
        keys = [self._value_hash_and_compress(value) for value in values]
        stats = self.insert_script(keys=keys, args=[expire])
        return [bool(stat) for stat in stats]

    def exists_and_lock(self, value: Any, expire: int = 7200) -> Any:
        return self.exists(value, expire)

    def exists_and_lock_many(self, values: Iterable[Any],
                             expire: int = 7200) -> Any:
        return self.exists_many(values, expire)

    def confirm(self, value: Any, expire: int = 2592000) -> Any:
        return self.insert(value, expire)

    def confirm_many(self, values: Iterable[Any],
                     expire: int = 2592000) -> Any:
        return self.insert_many(values, expire)


class AsyncRedisStringFilter(RedisStringFilter):
    @decorate_warning
    async def insert(self, value: Any, expire: int = 2592000) -> bool:
        stats = await self.insert_many([value], expire)
        return stats[0]

    @decorate_warning
    async def insert_many(self, values: Iterable[Any],
                          expire: int = 2592000) -> list[bool]:
        keys = [self._value_hash_and_compress(value) for value in values]
        stats = await self.insert_script(keys=keys, args=[expire])
        return [bool(stat) for stat in stats]

    @decorate_warning
    async def exists(self, value: Any, expire: int = 7200) -> bool:
        stats = await self.exists_many([value], expire)
        return stats[0]

    @decorate_warning
    async def exists_many(self, values: Iterable[Any],
                          expire: int = 7200) -> list[bool]:
        keys = [self._value_hash_and_compress(value) for value in values]
        raw = await self.exists_script(keys=keys, args=[expire])
        stats = [bool(stat) for stat in raw]
        self._log_exists(values, keys, stats)
        return stats

    async def exists_and_lock(self, value: Any, expire: int = 7200) -> bool:
        return (await self.exists_and_lock_many([value], expire))[0]

    async def exists_and_lock_many(self, values: Iterable[Any],
                                   expire: int = 7200) -> list[bool]:
        return await self.exists_many(values, expire)

    async def confirm(self, value: Any, expire: int = 2592000) -> bool:
        return (await self.confirm_many([value], expire))[0]

    async def confirm_many(self, values: Iterable[Any],
                           expire: int = 2592000) -> list[bool]:
        return await self.insert_many(values, expire)
