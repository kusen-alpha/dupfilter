# -*- coding:utf-8 -*-
from __future__ import annotations

from typing import Any

from dupfilter.filters.sql.sqlite import (
    AsyncSQLiteFilter,
    SQLiteFilter,
)


class DiskKVFilter(SQLiteFilter):
    """Self-contained on-disk key-value dedup filter.

    Unlike :class:`~dupfilter.filters.file.FileFilter`, entries are not
    loaded into memory: every check hits a SQLite index, so it scales to
    much larger sets. The database is WAL-mode and created on first use.
    A single instance is bound to its creating thread; share it across
    threads through ``ConnectionPool`` like other SQL backends.
    """

    def __init__(self, path: str, table: str = "kv", timeout: float = 30,
                 *args: Any, **kwargs: Any) -> None:
        import sqlite3

        self.path = path
        connection = sqlite3.connect(path, timeout=timeout)
        try:
            connection.execute("PRAGMA journal_mode=WAL")
            super().__init__(connection, table, *args, **kwargs)
        except BaseException:
            connection.close()
            raise


class AsyncDiskKVFilter(AsyncSQLiteFilter):
    """Async on-disk key-value dedup filter over ``aiosqlite``."""

    def __init__(self, path: str, table: str = "kv",
                 *args: Any, **kwargs: Any) -> None:
        import aiosqlite

        self.path = path
        self._started = False
        super().__init__(aiosqlite.connect(path), table, *args, **kwargs)

    async def _ensure_table(self) -> None:
        if not self._started:
            await self.connection
            self._started = True
        await super()._ensure_table()
