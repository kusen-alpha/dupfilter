# -*- coding:utf-8 -*-
# author: kusen
# email: 1194542196@qq.com
# date: 2023/9/10

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from dupfilter.filters import decorate_warning
from dupfilter.filters.redis import RedisFilter

EXISTS_SCRIPT = """
local keys = KEYS
local values = ARGV
local result = {}
for index, key in pairs(keys) do
    local exist = redis.call('ZSCORE', key, values[index])
    table.insert(result, exist)
end
return result
"""
INSERT_SCRIPT = """
local keys = KEYS
local values = ARGV
for index, key in pairs(keys) do
    local ts = redis.call("TIME")[1]
    redis.call('ZADD', key, 'NX', ts, values[index])
end
return true
"""
EXISTS_AND_INSERT_SCRIPT = """
local keys = KEYS
local values = ARGV
local result = {}
for index, key in pairs(keys) do
    local ts = redis.call("TIME")[1]
    local exist = redis.call('ZADD', key, 'NX', ts, values[index])
    if exist==0 then
        table.insert(result, 1)
    else
        table.insert(result, 0)
    end
end
return result
"""
RESET_SCRIPT = """
local key = KEYS[1]
local expire = ARGV[1]
local ts = redis.call("TIME")[1]
redis.call('ZREMRANGEBYSCORE', key, 0, ts-expire)
return true
"""
RELEASE_SCRIPT = """
local keys = KEYS
local values = ARGV
for index, key in pairs(keys) do
    redis.call('ZREM', key, values[index])
end
return true
"""


class RedisSortedSetFilter(RedisFilter):
    def __init__(self, server: Any, key: str, block_num: int = 1,
                 *args: Any, **kwargs: Any) -> None:
        self.key = key
        self.block_num = block_num
        super().__init__(server, *args, **kwargs)
        self._exists_script = self.server.register_script(EXISTS_SCRIPT)
        self._insert_script = self.server.register_script(INSERT_SCRIPT)
        self._exists_and_insert_script = self.server.register_script(
            EXISTS_AND_INSERT_SCRIPT)
        self._release_script = self.server.register_script(RELEASE_SCRIPT)

    @decorate_warning
    def exists(self, value: Any) -> Any:
        return self.exists_many([value])[0]

    @decorate_warning
    def exists_many(self, values: Iterable[Any]) -> Any:
        keys, new_values = self._get_keys_and_values(values)
        raw = self._exists_script(keys=keys, args=new_values)
        stats = [bool(stat) for stat in raw]
        self._log_exists(values, new_values, stats)
        return stats

    @decorate_warning
    def exists_and_insert(self, value: Any) -> Any:
        return self.exists_and_insert_many([value])[0]

    @decorate_warning
    def exists_and_insert_many(self, values: Iterable[Any]) -> Any:
        keys, new_values = self._get_keys_and_values(values)
        raw = self._exists_and_insert_script(keys=keys, args=new_values)
        stats = [bool(stat) for stat in raw]
        self._log_exists(values, new_values, stats)
        return stats

    @decorate_warning
    def insert(self, value: Any) -> Any:
        return self.insert_many([value])[0]

    @decorate_warning
    def insert_many(self, values: Iterable[Any]) -> Any:
        keys, values = self._get_keys_and_values(values)
        stat = self._insert_script(keys=keys, args=values)
        return [bool(stat) for _ in range(len(values))]

    def _get_keys_and_values(self, values: Iterable[Any]
                             ) -> tuple[list[str], list[Any]]:
        values = [self._value_hash(value) for value in values]
        keys = [self.key + self._get_block(value) for value in values]
        values = [self._value_compress(value) for value in values]
        return keys, values

    def _get_block(self, value: Any) -> str:
        if self.block_num == 1:
            return ''
        return str(int(value[0:2], 16) % self.block_num)

    # Two-phase claim/confirm: exists_and_lock claims a value, confirm()
    # keeps it, release() removes it again so a retry can reclaim it.
    def exists_and_lock(self, value: Any) -> Any:
        return self.exists_and_insert(value)

    def exists_and_lock_many(self, values: Iterable[Any]) -> Any:
        return self.exists_and_insert_many(values)

    def confirm(self, value: Any) -> Any:
        return self.insert(value)

    def confirm_many(self, values: Iterable[Any]) -> Any:
        return self.insert_many(values)

    @decorate_warning
    def release(self, value: Any) -> Any:
        return self.release_many([value])[0]

    @decorate_warning
    def release_many(self, values: Iterable[Any]) -> Any:
        keys, new_values = self._get_keys_and_values(values)
        self._release_script(keys=keys, args=new_values)
        return [True for _ in new_values]


class AsyncRedisSortedSetFilter(RedisSortedSetFilter):
    @decorate_warning
    async def insert(self, value: Any) -> bool:
        stats = await self.insert_many([value])
        return stats[0]

    @decorate_warning
    async def insert_many(self, values: Iterable[Any]) -> list[bool]:
        keys, values = self._get_keys_and_values(values)
        stat = await self._insert_script(keys=keys, args=values)
        return [bool(stat) for _ in range(len(values))]

    @decorate_warning
    async def exists(self, value: Any) -> bool:
        stats = await self.exists_many([value])
        return stats[0]

    @decorate_warning
    async def exists_many(self, values: Iterable[Any]) -> list[bool]:
        keys, new_values = self._get_keys_and_values(values)
        raw = await self._exists_script(keys=keys, args=new_values)
        stats = [bool(stat) for stat in raw]
        self._log_exists(values, new_values, stats)
        return stats

    @decorate_warning
    async def exists_and_insert(self, value: Any) -> bool:
        stats = await self.exists_and_insert_many([value])
        return stats[0]

    @decorate_warning
    async def exists_and_insert_many(self, values: Iterable[Any]
                                     ) -> list[bool]:
        keys, new_values = self._get_keys_and_values(values)
        raw = await self._exists_and_insert_script(keys=keys, args=new_values)
        stats = [bool(stat) for stat in raw]
        self._log_exists(values, new_values, stats)
        return stats

    @decorate_warning
    async def exists_and_lock(self, value: Any) -> bool:
        return await self.exists_and_insert(value)

    @decorate_warning
    async def exists_and_lock_many(self, values: Iterable[Any]) -> list[bool]:
        return await self.exists_and_insert_many(values)

    @decorate_warning
    async def confirm(self, value: Any) -> bool:
        return await self.insert(value)

    @decorate_warning
    async def confirm_many(self, values: Iterable[Any]) -> list[bool]:
        return await self.insert_many(values)

    @decorate_warning
    async def release(self, value: Any) -> bool:
        result = await self.release_many([value])
        return result[0]

    @decorate_warning
    async def release_many(self, values: Iterable[Any]) -> list[bool]:
        keys, new_values = self._get_keys_and_values(values)
        await self._release_script(keys=keys, args=new_values)
        return [True for _ in new_values]
