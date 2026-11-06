# -*- coding:utf-8 -*-
# author: kusen
# email: 1194542196@qq.com
# date: 2024/3/12

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from typing import Any

from dupfilter.filters import Filter, decorate_warning
from dupfilter.filters.sql import SQLFilter


class MySQLFilter(SQLFilter):
    def _exists_table(self) -> bool:
        self.cursor.execute(
            "SELECT TABLE_NAME FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s",
            (self.table,))
        return self.cursor.fetchone() is not None


class AsyncMySQLFilter(SQLFilter, Filter):
    def __init__(self, pool: Any, table: str,
                 record_time: bool = False, mode: str = "atomic",
                 *args: Any, **kwargs: Any) -> None:
        if mode not in ("atomic", "batch"):
            raise ValueError("mode must be 'atomic' or 'batch'")
        if not self._valid_table_name(table):
            raise ValueError(f'invalid table name: {table!r}')
        self.pool = pool
        self.table = table
        self.record_time = record_time
        # "batch" is the fast snapshot path: one SELECT plus one
        # batched INSERT (2 round trips), single-worker only, with
        # unique values per call. Intra-batch repeats are reported as
        # new, and concurrent writers can both see "new".
        self.mode = mode
        self._table_ready = False
        self._table_lock: asyncio.Lock | None = None
        Filter.__init__(self, *args, **kwargs)

    async def _ensure_table(self) -> None:
        if self._table_ready:
            return
        if self._table_lock is None:
            self._table_lock = asyncio.Lock()
        async with self._table_lock:
            if self._table_ready:
                return
            await self.create_table()
            self._table_ready = True

    async def create_table(self) -> None:
        sql = self._create_table_sql()
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(sql)
                    await conn.commit()
                    self.logger.info(f"创建去重表{self.table}成功")
                except Exception:
                    self.logger.info(f"创建去重表{self.table}已经创建或创建失败")
                    return

    @decorate_warning
    async def exists(self, value: Any) -> bool:
        result = await self.exists_many([value])
        return result[0]

    @decorate_warning
    async def exists_many(self, values: Iterable[Any]) -> list[bool]:
        values = list(values)
        await self._ensure_table()
        sql, params, new_values = self._exists_sql(values)
        if not new_values:
            return []
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, params)
                result = await cur.fetchall()
                result = set(res[0] for res in result)
                stats = [value in result for value in new_values]
                self._log_exists(values, new_values, stats)
                return stats

    @decorate_warning
    async def insert(self, value: Any) -> bool:
        result = await self.insert_many([value])
        return result[0]

    @decorate_warning
    async def insert_many(self, values: Iterable[Any]) -> list[bool]:
        await self._ensure_table()
        sql, values = self._insert_sql(values)
        async with self.pool.acquire() as conn:
            try:
                async with conn.cursor() as cur:
                    await cur.executemany(sql, values)
                await conn.commit()
            except Exception:
                await conn.rollback()
                raise
        return [True for _ in values]

    async def count(self) -> Any:
        await self._ensure_table()
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(f"SELECT COUNT(*) FROM {self.table}", ())
                row = await cur.fetchone()
        return int(row[0]) if row else 0

    @decorate_warning
    async def exists_and_insert(self, value: Any) -> bool:
        result = await self.exists_and_insert_many([value])
        return result[0]

    # Two-phase claim/confirm semantics, mirroring the sync backend.
    exists_and_lock = exists_and_insert
    confirm = insert
    confirm_many = insert_many

    @decorate_warning
    async def release(self, value: Any) -> bool:
        result = await self.release_many([value])
        return result[0]

    @decorate_warning
    async def release_many(self, values: list[Any]) -> list[bool]:
        new_values = [self._value_hash_and_compress(value)
                      for value in values]
        if not new_values:
            return []
        placeholders = self._placeholder_many(len(new_values))
        sql = f"DELETE FROM {self.table} WHERE id IN ({placeholders})"
        async with self.pool.acquire() as conn:
            try:
                async with conn.cursor() as cur:
                    await cur.execute(sql, new_values)
                await conn.commit()
            except Exception:
                await conn.rollback()
                raise
        return [True for _ in values]

    @decorate_warning
    async def exists_and_insert_many(self, values: Iterable[Any]) -> list[bool]:
        values = list(values)
        if self.mode == "batch":
            # Snapshot semantics: reports the state before this call,
            # so intra-batch repeats are all reported as new. Fast
            # (2 round trips) but only safe for a single worker with
            # unique values per batch.
            stats = await self.exists_many(values)
            seen = set()
            to_insert = []
            for stat, value in zip(stats, values):
                if stat:
                    continue
                new_value = self._value_hash_and_compress(value)
                if new_value in seen:
                    continue
                seen.add(new_value)
                to_insert.append(value)
            if to_insert:
                await self.insert_many(to_insert)
            return stats
        await self._ensure_table()
        stats = []
        async with self.pool.acquire() as conn:
            try:
                async with conn.cursor() as cur:
                    for value in values:
                        sql, params = self._insert_sql([value])
                        new_value = self._value_hash_and_compress(value)
                        param = params[0]
                        if not isinstance(param, (tuple, list)):
                            param = (param,)
                        await cur.execute(sql, param)
                        stat = cur.rowcount == 0
                        self._log_exists([value], [new_value], [stat])
                        stats.append(stat)
                await conn.commit()
            except Exception:
                await conn.rollback()
                raise
        return stats

    exists_and_lock_many = exists_and_insert_many

    async def close(self) -> None:
        await self.pool.close()


if __name__ == '__main__':
    import asyncio

    import aiomysql


    async def test() -> None:
        configs = {
            'minsize': 1,
            'maxsize': 10,
            'host': '127.0.0.1',
            'user': 'root',
            'password': 'change_me',
            'port': 3306,
            'db': 'dupfilter'
        }
        pool = await aiomysql.create_pool(**configs)
        f = AsyncMySQLFilter(pool=pool, table="dup", record_time=True)
        await f.create_table()
        res = await f.exists_and_insert_many(['1', '2', '3'])
        print(res)


    loop = asyncio.get_event_loop()
    loop.run_until_complete(test())
