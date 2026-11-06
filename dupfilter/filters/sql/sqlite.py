# -*- coding:utf-8 -*-
# author: kusen
# email: 1194542196@qq.com
# date: 2024/3/13

from __future__ import annotations

import asyncio
import time
from collections.abc import Iterable
from typing import Any

from dupfilter.filters import Filter, decorate_warning
from dupfilter.filters.sql import SQLFilter


class SQLiteFilter(SQLFilter):
    def _exists_table(self) -> bool:
        self.cursor.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name=?",
            (self.table,))
        return self.cursor.fetchone() is not None

    def _placeholder_many(self, count: int) -> str:
        return ", ".join(["?"] * count)

    def _insert_sql(self, values: Iterable[Any]) -> tuple[str, list[Any]]:
        values = [self._value_hash_and_compress(value) for value in values]
        if self.record_time:
            sql = f"""INSERT INTO {self.table}
                             (id, insert_time) VALUES (?, ?)
                             ON CONFLICT(id) DO NOTHING"""
            values = [(value, int(time.time())) for value in values]
        else:
            sql = f"""INSERT INTO {self.table} (id) VALUES (?)
                      ON CONFLICT(id) DO NOTHING"""
            values = [(value, ) for value in values]
        return sql, values


class AsyncSQLiteFilter(SQLiteFilter):
    """Async SQLite backend built on ``aiosqlite``.

    Takes an ``aiosqlite`` Connection (``await aiosqlite.connect(...)``).
    """

    def __init__(self, connection: Any, table: str,
                 record_time: bool = False, mode: str = "atomic",
                 auto_create_table: bool = True,
                 *args: Any, **kwargs: Any) -> None:
        if mode not in ("atomic", "batch"):
            raise ValueError("mode must be 'atomic' or 'batch'")
        if not self._valid_table_name(table):
            raise ValueError(f"invalid table name: {table!r}")
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

    async def create_table(self) -> None:
        if await self._exists_table_async():
            self._table_ready = True
            return
        sql = self._create_table_sql()
        try:
            async with self.connection.cursor() as cur:
                await cur.execute(sql)
            await self.connection.commit()
        except Exception:
            await self.connection.rollback()
            raise

    async def _exists_table_async(self) -> bool:
        async with self.connection.cursor() as cur:
            await cur.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND name=?", [self.table])
            return await cur.fetchone() is not None

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
        async with self.connection.cursor() as cur:
            await cur.execute(sql, params)
            raw = await cur.fetchall()
        result = {row[0] for row in raw}
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
        sql, rows = self._insert_sql(values)
        try:
            async with self.connection.cursor() as cur:
                await cur.executemany(sql, rows)
            await self.connection.commit()
        except Exception:
            await self.connection.rollback()
            raise
        return [True for _ in rows]

    async def count(self) -> Any:
        await self._ensure_table()
        async with self.connection.cursor() as cur:
            await cur.execute(f"SELECT COUNT(*) FROM {self.table}", [])
            row = await cur.fetchone()
        return int(row[0]) if row else 0

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
        try:
            for value in values:
                sql, params = self._insert_sql([value])
                new_value = self._value_hash_and_compress(value)
                param = params[0]
                if not isinstance(param, (tuple, list)):
                    param = (param,)
                async with self.connection.cursor() as cur:
                    await cur.execute(sql, param)
                    stat = cur.rowcount == 0
                self._log_exists([value], [new_value], [stat])
                stats.append(stat)
            await self.connection.commit()
        except Exception:
            await self.connection.rollback()
            raise
        return stats

    @decorate_warning
    async def release(self, value: Any) -> bool:
        result = await self.release_many([value])
        return result[0]

    @decorate_warning
    async def release_many(self, values: Iterable[Any]) -> list[bool]:
        new_values = [self._value_hash_and_compress(value)
                      for value in values]
        if not new_values:
            return []
        placeholders = self._placeholder_many(len(new_values))
        sql = f"DELETE FROM {self.table} WHERE id IN ({placeholders})"
        try:
            async with self.connection.cursor() as cur:
                await cur.execute(sql, new_values)
            await self.connection.commit()
        except Exception:
            await self.connection.rollback()
            raise
        return [True for _ in values]

    # Two-phase claim/confirm aliases, mirroring the sync backends.
    exists_and_lock = exists_and_insert
    exists_and_lock_many = exists_and_insert_many
    confirm = insert
    confirm_many = insert_many

    async def close(self) -> None:
        await self.connection.close()
