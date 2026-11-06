# -*- coding:utf-8 -*-
# author: kusen
# email: 1194542196@qq.com
# date: 2024/3/12

from __future__ import annotations

import asyncio
import time
from collections.abc import Iterable
from typing import Any

from dupfilter.filters import Filter, decorate_warning
from dupfilter.filters.sql import SQLFilter


class OracleSQLFilter(SQLFilter):

    def _create_table_sql(self) -> str:
        if not self.record_time:
            return f"""CREATE TABLE {self.table} (
                id VARCHAR(32) NOT NULL,
                PRIMARY KEY (id)
            )"""
        return f"""CREATE TABLE {self.table} (
            id VARCHAR(32) NOT NULL,
            insert_time INT,
            PRIMARY KEY (id)
        )"""

    def _exists_table(self) -> bool:
        sql = f"SELECT TABLE_NAME FROM USER_TABLES WHERE TABLE_NAME = '{self.table.upper()}'"
        try:
            self.cursor.execute(sql)
            res = self.cursor.fetchone()
            return bool(res)
        except Exception:
            return False

    def _placeholder_many(self, count: int) -> str:
        return ", ".join(f":{i + 1}" for i in range(count))

    def _insert_sql(self, values: Iterable[Any]) -> tuple[str, list[Any]]:
        values = [self._value_hash_and_compress(value) for value in values]
        if self.record_time:
            sql = f"""INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX(
            {self.table}(id)) */ INTO {self.table}
                     (id, insert_time) VALUES (:1,:2)"""
            values = [(value, int(time.time())) for value in values]
        else:
            sql = f"""INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX(
            {self.table}(id)) */ INTO {self.table}
                     (id) VALUES (:1)"""
            values = [(value, ) for value in values]
        return sql, values


class AsyncOracleSQLFilter(OracleSQLFilter):
    """Async Oracle backend built on python-oracledb's asyncio mode.

    Takes an ``oracledb`` AsyncConnection (``await oracledb.connect_async(...)``),
    thin or thick mode both work.
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
        cursor = self.connection.cursor()
        try:
            await cursor.execute(sql)
            await self.connection.commit()
        except Exception:
            await self.connection.rollback()
            raise
        finally:
            cursor.close()

    async def _exists_table_async(self) -> bool:
        cursor = self.connection.cursor()
        try:
            await cursor.execute(
                "SELECT TABLE_NAME FROM USER_TABLES "
                "WHERE TABLE_NAME = :1", [self.table.upper()])
            return await cursor.fetchone() is not None
        finally:
            cursor.close()

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
        cursor = self.connection.cursor()
        try:
            await cursor.execute(sql, params)
            raw = await cursor.fetchall()
        finally:
            cursor.close()
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
        cursor = self.connection.cursor()
        try:
            await cursor.executemany(sql, rows)
            await self.connection.commit()
        except Exception:
            await self.connection.rollback()
            raise
        finally:
            cursor.close()
        return [True for _ in rows]

    async def count(self) -> Any:
        await self._ensure_table()
        cursor = self.connection.cursor()
        try:
            await cursor.execute(f"SELECT COUNT(*) FROM {self.table}", [])
            row = await cursor.fetchone()
        finally:
            cursor.close()
        return int(row[0]) if row else 0

    @decorate_warning
    async def exists_and_insert(self, value: Any) -> bool:
        result = await self.exists_and_insert_many([value])
        return result[0]

    @decorate_warning
    async def exists_and_insert_many(self, values: Iterable[Any]) -> list[bool]:
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
                cursor = self.connection.cursor()
                try:
                    await cursor.execute(sql, param)
                    stat = cursor.rowcount == 0
                finally:
                    cursor.close()
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
        cursor = self.connection.cursor()
        try:
            await cursor.execute(sql, new_values)
            await self.connection.commit()
        except Exception:
            await self.connection.rollback()
            raise
        finally:
            cursor.close()
        return [True for _ in values]

    # Two-phase claim/confirm aliases, mirroring the sync backends.
    exists_and_lock = exists_and_insert
    exists_and_lock_many = exists_and_insert_many
    confirm = insert
    confirm_many = insert_many

    async def close(self) -> None:
        await self.connection.close()


