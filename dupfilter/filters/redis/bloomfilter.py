# -*- coding:utf-8 -*-
# author: kusen
# email: 1194542196@qq.com
# date: 2023/9/6

from __future__ import annotations

import math
from collections.abc import Iterable
from typing import Any

from dupfilter.filters import decorate_warning
from dupfilter.filters.redis import RedisFilter

EXISTS_SCRIPT = """
local keys = KEYS
local offsets = ARGV
local result = {}
for index, key in pairs(keys) do
    local exist = 1
    for offset in string.gmatch(offsets[index], "[^,]+") do
        exist = redis.call('GETBIT', key, offset)
        if exist == 0 then
            break
        end
    end
    table.insert(result, exist)
end
return result
"""

INSERT_SCRIPT = """
local keys = KEYS
local offsets = ARGV
for index, key in pairs(keys) do
    for offset in string.gmatch(offsets[index], "[^,]+") do
        redis.call('SETBIT', key, offset, 1)
    end
end
return true
"""

EXISTS_AND_INSERT_SCRIPT = """
local keys = KEYS
local offsets = ARGV
local result = {}
for index, key in pairs(keys) do
    local exist = 1
    for offset in string.gmatch(offsets[index], "[^,]+") do
        local _exist = redis.call('GETBIT', key, offset)
        if _exist == 0 then
            exist = _exist
            redis.call('SETBIT', key, offset, 1)
        end
    end
    table.insert(result, exist)
end
return result
"""

RESET_SCRIPT = """
local key = KEYS[1]
local offsets = ARGV
for _, offset in pairs(offsets) do
    redis.call('SETBIT', key, offset, 0)
end
return true
"""


class SimpleHash:
    def __init__(self, bit: int, seed: int) -> None:
        self.bit = bit
        self.seed = seed

    def hash(self, value: Any) -> int:
        ret = 0
        for i in range(len(value)):
            ret += self.seed * ret + ord(value[i])
        return self.bit & ret


class RedisBloomFilter(RedisFilter):
    def __init__(self, server: Any, key: str, bit: int = 32,
                 hash_num: int = 6, block_num: int = 1,
                 expected_items: int | None = None,
                 false_positive_rate: float | None = None,
                 *args: Any, **kwargs: Any) -> None:
        """

        :param server: The redis obj, redis.Redis()
        :param key:
        :param bit:
        :param hash_num:
        :param block_num:
        :param reset: 是否进行重置/清除，
        :param reset_proportion: 达到比例后进行重置/清除，
        :param reset_check_period: 重置/清除比例获取计算周期，
        """
        self.key = key
        # Optional sizing from expected cardinality and the target
        # false-positive rate: m = -n*ln(p)/ln(2)^2, k = m/n*ln(2).
        if expected_items is not None:
            if expected_items <= 0:
                raise ValueError('expected_items must be > 0')
            if false_positive_rate is None:
                raise ValueError('false_positive_rate is required')
            if not 0 < false_positive_rate < 1:
                raise ValueError('false_positive_rate must be in (0, 1)')
            bits = -expected_items * math.log(false_positive_rate) / (
                math.log(2) ** 2)
            bit = min(32, max(1, math.ceil(math.log2(bits))))
            hash_num = max(1, round(bits / expected_items * math.log(2)))
        if bit > 32:
            raise ValueError('The bit value not > 32!')
        self.bit = (1 << bit) - 1  # Redis的String类型最大容量为512M，即bit=32
        self.hash_num = hash_num
        self.seeds = range(1, hash_num + 1)
        self.shs = [SimpleHash(self.bit, seed) for seed in self.seeds]
        self.block_num = block_num
        super().__init__(server, *args, **kwargs)
        self._exists_script = self.server.register_script(EXISTS_SCRIPT)
        self._insert_script = self.server.register_script(INSERT_SCRIPT)
        self._exists_insert_script = self.server.register_script(
            EXISTS_AND_INSERT_SCRIPT)

    @decorate_warning
    def exists(self, value: Any) -> Any:
        return self.exists_many([value])[0]

    @decorate_warning
    def exists_many(self, values: Iterable[Any]) -> Any:
        keys, offsets, new_values = self._get_keys_and_offsets(values)
        raw = self._exists_script(keys=keys, args=offsets)
        stats = [bool(stat) for stat in raw]
        self._log_exists(values, new_values, stats)
        return stats

    @decorate_warning
    def insert(self, value: Any) -> Any:
        return self.insert_many([value])[0]

    @decorate_warning
    def insert_many(self, values: Iterable[Any]) -> Any:
        keys, offsets, new_values = self._get_keys_and_offsets(values)
        stat = self._insert_script(keys=keys, args=offsets)
        return [bool(stat) for _ in range(len(new_values))]

    @decorate_warning
    def exists_and_insert(self, value: Any) -> Any:
        return self.exists_and_insert_many([value])[0]

    @decorate_warning
    def exists_and_insert_many(self, values: Iterable[Any]) -> Any:
        keys, offsets, new_values = self._get_keys_and_offsets(values)
        raw = self._exists_insert_script(keys=keys, args=offsets)
        stats = [bool(stat) for stat in raw]
        self._log_exists(values, new_values, stats)
        return stats

    def _get_keys_and_offsets(self, values: Iterable[Any]
                              ) -> tuple[list[str], list[str], list[Any]]:
        values = [self._value_hash(value) for value in values]
        keys = [self.key + self._get_block(value) for value in values]
        offsets = [','.join([str(sh.hash(
            self._value_compress(value))) for sh in self.shs]
        ) for value in values]
        return keys, offsets, values

    def _get_block(self, value: Any) -> str:
        if self.block_num == 1:
            return ''
        return str(int(value[0:2], 16) % self.block_num)

    # Two-phase claim/confirm: exists_and_lock claims a value and
    # confirm() keeps it. release() is intentionally unsupported:
    # bloom bits are shared between values, clearing one value's bits
    # could erase other values.
    def exists_and_lock(self, value: Any) -> Any:
        return self.exists_and_insert(value)

    def exists_and_lock_many(self, values: Iterable[Any]) -> Any:
        return self.exists_and_insert_many(values)

    def confirm(self, value: Any) -> Any:
        return self.insert(value)

    def confirm_many(self, values: Iterable[Any]) -> Any:
        return self.insert_many(values)

    def release(self, value: Any) -> Any:
        raise NotImplementedError(
            "bloom bits are shared; release is not supported")

    def release_many(self, values: Iterable[Any]) -> Any:
        raise NotImplementedError(
            "bloom bits are shared; release is not supported")


class AsyncRedisBloomFilter(RedisBloomFilter):
    @decorate_warning
    async def exists(self, value: Any) -> bool:
        stats = await self.exists_many([value])
        return stats[0]

    @decorate_warning
    async def exists_many(self, values: Iterable[Any]) -> list[bool]:
        keys, offsets, new_values = self._get_keys_and_offsets(values)
        raw = await self._exists_script(keys=keys, args=offsets)
        stats = [bool(stat) for stat in raw]
        self._log_exists(values, new_values, stats)
        return stats

    @decorate_warning
    async def insert(self, value: Any) -> bool:
        stats = await self.insert_many([value])
        return stats[0]

    @decorate_warning
    async def insert_many(self, values: Iterable[Any]) -> list[bool]:
        keys, offsets, new_values = self._get_keys_and_offsets(values)
        stat = await self._insert_script(keys=keys, args=offsets)
        return [bool(stat) for _ in range(len(new_values))]

    @decorate_warning
    async def exists_and_insert(self, value: Any) -> bool:
        stats = await self.exists_and_insert_many([value])
        return stats[0]

    @decorate_warning
    async def exists_and_insert_many(self, values: Iterable[Any]
                                     ) -> list[bool]:
        keys, offsets, new_values = self._get_keys_and_offsets(values)
        raw = await self._exists_insert_script(keys=keys, args=offsets)
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

    async def release(self, value: Any) -> bool:
        raise NotImplementedError(
            "bloom bits are shared; release is not supported")

    async def release_many(self, values: Iterable[Any]) -> list[bool]:
        raise NotImplementedError(
            "bloom bits are shared; release is not supported")
