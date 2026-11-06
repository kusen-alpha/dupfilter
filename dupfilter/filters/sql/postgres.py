# -*-coding:utf-8 -*-
from __future__ import annotations

import asyncio
import time
from collections.abc import Iterable
from typing import Any

from dupfilter.filters import Filter, decorate_warning
from dupfilter.filters.sql import SQLFilter


class PostgresFilter(SQLFilter):
    """PostgreSQL dedup over a psycopg2 connection.

    Exact dedup based on the primary key with
    ``INSERT ... ON CONFLICT (id) DO NOTHING``.
    """

    def _exists_table(self) -> bool:
        self.cursor.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = current_schema() AND table_name = %s",
            (self.table.lower(),))
        return self.cursor.fetchone() is not None

    def _placeholder_many(self, count: int) -> str:
        return ", ".join(["%s"] * count)

    def _insert_sql(self, values: Iterable[Any]) -> tuple[str, list[Any]]:
        values = [self._value_hash_and_compress(value) for value in values]
        if self.record_time:
            sql = f"""INSERT INTO {self.table} (id, insert_time)
                     VALUES (%s, %s)
                     ON CONFLICT (id) DO NOTHING"""
            values = [(value, int(time.time())) for value in values]
        else:
            sql = f"""INSERT INTO {self.table} (id) VALUES (%s)
                      ON CONFLICT (id) DO NOTHING"""
            values = [(value, ) for value in values]
        return sql, values


class AsyncPostgresFilter(SQLFilter, Filter):
    """PostgreSQL dedup over an asyncpg connection.

    Atomic mode uses ``RETURNING id`` so a conflict is visible as a missing
    row without relying on rowcount. asyncpg commits each statement, so no
    explicit rollback is needed.
    """

    def __init__(self, connection: Any, table: str,
                 record_time: bool = False, mode: str = "atomic",
                 auto_create_table: bool = True,
                 *args: Any, **kwargs: Any) -> None:
        if mode not in ("atomic", "batch"):
            raise ValueError("mode must be 'atomic' or 'batch'")
        if not self._valid_table_name(table):
            raise ValueError(f'invalid table name: {table!r}')
        self.connection = connection
        self.table = table
        self.record_time = record_time
        self.mode = mode
        self._auto_create_table = auto_create_table
        self._table_ready = False
        self._table_lock: asyncio.Lock | None = None
        Filter.__init__(self, *args, **kwargs)

    async def _ensure_table(self) -> None:
        if self._table_ready or not self._auto_create_table:
            return
        if self._table_lock is None:
            self._table_lock = asyncio.Lock()
        async with self._table_lock:
            if self._table_ready:
                return
            await self.create_table()
            self._table_ready = True

    async def _exists_table_async(self) -> bool:
        return bool(await self.connection.fetchval(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = current_schema() AND table_name = $1",
            self.table.lower()))

    async def create_table(self) -> None:
        if await self._exists_table_async():
            self._table_ready = True
            return
        await self.connection.execute(self._create_table_sql())

    @decorate_warning
    async def exists(self, value: Any) -> bool:
        result = await self.exists_many([value])
        return result[0]

    @decorate_warning
    async def exists_many(self, values: Iterable[Any]) -> list[bool]:
        values = list(values)
        await self._ensure_table()
        new_values = [self._value_hash_and_compress(value)
                      for value in values]
        if not new_values:
            return []
        rows = await self.connection.fetch(
            f"SELECT id FROM {self.table} WHERE id = ANY($1)", new_values)
        existing = {row['id'] for row in rows}
        stats = [value in existing for value in new_values]
        self._log_exists(values, new_values, stats)
        return stats

    def _insert_sql(self, values: Iterable[Any]) -> tuple[str, list[Any]]:
        values = [self._value_hash_and_compress(value) for value in values]
        if self.record_time:
            sql = f"""INSERT INTO {self.table} (id, insert_time)
                     VALUES ($1, $2)
                     ON CONFLICT (id) DO NOTHING RETURNING id"""
            values = [(value, int(time.time())) for value in values]
        else:
            sql = f"""INSERT INTO {self.table} (id) VALUES ($1)
                      ON CONFLICT (id) DO NOTHING RETURNING id"""
            values = [(value, ) for value in values]
        return sql, values

    @decorate_warning
    async def insert(self, value: Any) -> bool:
        result = await self.insert_many([value])
        return result[0]

    @decorate_warning
    async def insert_many(self, values: Iterable[Any]) -> list[bool]:
        await self._ensure_table()
        values = list(values)
        sql, rows = self._insert_sql(values)
        await self.connection.executemany(sql, rows)
        return [True for _ in rows]

    async def count(self) -> Any:
        await self._ensure_table()
        return await self.connection.fetchval(
            f"SELECT COUNT(*) FROM {self.table}")

    @decorate_warning
    async def exists_and_insert(self, value: Any) -> bool:
        result = await self.exists_and_insert_many([value])
        return result[0]

    @decorate_warning
    async def exists_and_insert_many(self, values: Iterable[Any]
                                     ) -> list[bool]:
        values = list(values)
        await self._ensure_table()
        if self.mode == "batch":
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
        stats = []
        for value in values:
            sql, params = self._insert_sql([value])
            params = params[0]
            if not isinstance(params, (tuple, list)):
                params = (params,)
            row = await self.connection.fetchrow(sql, *params)
            new_value = self._value_hash_and_compress(value)
            stat = row is None
            self._log_exists([value], [new_value], [stat])
            stats.append(stat)
        return stats

    # Two-phase claim/confirm aliases, mirroring the other backends.
    exists_and_lock = exists_and_insert
    exists_and_lock_many = exists_and_insert_many
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
        await self.connection.execute(
            f"DELETE FROM {self.table} WHERE id = ANY($1)", new_values)
        return [True for _ in values]

    async def close(self) -> None:
        await self.connection.close()
