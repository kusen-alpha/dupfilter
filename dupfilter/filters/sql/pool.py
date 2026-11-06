from __future__ import annotations

import contextlib
import queue
import threading
from typing import Any, Callable, Iterator

from dupfilter.filters.sql import SQLFilter


class ConnectionPool:
    """A tiny thread-safe pool of database connections.

    Connections are created lazily on demand and returned to the pool
    after each acquire/release cycle. Call :meth:`close` to close all
    idle connections. When pooling SQLite, create connections with
    ``check_same_thread=False`` so they can be shared across threads.
    """

    def __init__(self, connect: Callable[[], Any], size: int = 5) -> None:
        if size <= 0:
            raise ValueError("size must be > 0")
        self._connect = connect
        self._max_size = size
        self._available: queue.Queue[Any] = queue.Queue()
        self._lock = threading.Lock()
        self._closed = False

    @contextlib.contextmanager
    def acquire(self) -> Iterator[Any]:
        with self._lock:
            if self._closed:
                raise RuntimeError("pool is closed")
            if not self._available.empty():
                conn = self._available.get_nowait()
            else:
                conn = self._connect()
        try:
            yield conn
        finally:
            self._available.put(conn)

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
        while not self._available.empty():
            conn = self._available.get_nowait()
            try:
                conn.close()
            except Exception:
                pass


class PooledSQLFilter:
    """SQL filter that checks a connection out of a pool per operation.

    The dedup table is created once on construction; every public call
    then runs on a fresh connection from the pool, which makes the sync
    backend safe to share across threads.
    """

    def __init__(self, pool: ConnectionPool, table: str,
                 filter_cls: type[SQLFilter] = SQLFilter,
                 **init_kwargs: Any) -> None:
        self._pool = pool
        self._table = table
        self._filter_cls = filter_cls
        self._init_kwargs = init_kwargs
        with self._pool.acquire() as conn:
            flt = self._filter_cls(conn, self._table, **self._init_kwargs)
            flt.cursor.close()

    def _run(self, method: str, args: tuple[Any, ...],
             kwargs: dict[str, Any]) -> Any:
        with self._pool.acquire() as conn:
            flt = self._filter_cls(
                conn, self._table, auto_create_table=False,
                **self._init_kwargs)
            try:
                return getattr(flt, method)(*args, **kwargs)
            finally:
                try:
                    flt.cursor.close()
                except Exception:
                    pass

    def exists(self, value: Any) -> Any:
        return self._run("exists", (value,), {})

    def exists_many(self, values: Any) -> Any:
        return self._run("exists_many", (values,), {})

    def insert(self, value: Any) -> Any:
        return self._run("insert", (value,), {})

    def insert_many(self, values: Any) -> Any:
        return self._run("insert_many", (values,), {})

    def count(self) -> Any:
        return self._run("count", (), {})

    def exists_and_insert(self, value: Any) -> Any:
        return self._run("exists_and_insert", (value,), {})

    def exists_and_insert_many(self, values: Any) -> Any:
        return self._run("exists_and_insert_many", (values,), {})

    def exists_and_lock(self, value: Any) -> Any:
        return self._run("exists_and_lock", (value,), {})

    def confirm(self, value: Any) -> Any:
        return self._run("confirm", (value,), {})

    def release(self, value: Any) -> Any:
        return self._run("release", (value,), {})

    def release_many(self, values: Any) -> Any:
        return self._run("release_many", (values,), {})

    def close(self) -> None:
        self._pool.close()
