# -*- coding:utf-8 -*-
# author: kusen
# email: 1194542196@qq.com
# date: 2023/9/7

from __future__ import annotations

import os
import threading
from typing import Any

from dupfilter.filters import decorate_reset, decorate_warning, to_thread
from dupfilter.filters.memory import MemoryFilter, MemoryReset


class FileReset(MemoryReset):

    def reset(self) -> None:
        with self.flt._lock:
            super().reset()
            self.flt.file.seek(0)
            self.flt.file.truncate()
            for dup in self.flt.dups:
                self.flt.file.write(dup + '\n')
            self.flt.file.flush()


class FileFilter(MemoryFilter):
    def __init__(self,
                 path: str = './',
                 *args: Any, **kwargs: Any) -> None:
        self.path = path
        os.makedirs(self.path, exist_ok=True)
        self.file = open(os.path.join(self.path, 'dup'), 'a+')
        self.file.seek(0)
        self._lock = threading.RLock()
        super().__init__(*args, **kwargs)
        self.dups.update(x.rstrip() for x in self.file)

    @decorate_reset
    def _insert(self, value: Any) -> None:
        with self._lock:
            self.dups.add(value)
            self.file.write(value + '\n')

    @decorate_warning
    def insert(self, value: Any) -> bool:
        new_value = self._value_hash_and_compress(value)
        with self._lock:
            if new_value not in self.dups:
                self._insert(new_value)
        return True

    @decorate_warning
    def exists_and_insert(self, value: Any) -> bool:
        new_value = self._value_hash_and_compress(value)
        with self._lock:
            stat = new_value in self.dups
            self._log_exists([value], [new_value], [stat])
            if not stat:
                self._insert(new_value)
        return stat

    def close(self) -> None:
        with self._lock:
            if getattr(self, "_closed", False):
                return
            try:
                self.file.flush()
                self.file.close()
                self._closed = True
            except Exception as e:
                self.logger.warning(f"文件去重句柄关闭失败：{str(e)}")


class AsyncFileFilter:
    """Async API over :class:`FileFilter`.

    File I/O is blocking, so each operation runs in a worker thread via
    :func:`to_thread` and never blocks the event loop; the underlying
    :class:`FileFilter` keeps the shared file handle thread-safe.
    """

    def __init__(self, path: str = "./",
                 *args: Any, **kwargs: Any) -> None:
        kwargs["path"] = path
        self._flt: Any = FileFilter(*args, **kwargs)

    @property
    def dups(self) -> Any:
        return self._flt.dups

    async def exists(self, value: Any, **kwargs: Any) -> Any:
        return await to_thread(self._flt.exists, value, **kwargs)

    async def exists_many(self, values: Any, **kwargs: Any) -> Any:
        return await to_thread(self._flt.exists_many, values, **kwargs)

    async def insert(self, value: Any, **kwargs: Any) -> Any:
        return await to_thread(self._flt.insert, value, **kwargs)

    async def insert_many(self, values: Any, **kwargs: Any) -> Any:
        return await to_thread(self._flt.insert_many, values, **kwargs)

    async def exists_and_insert(self, value: Any, **kwargs: Any) -> Any:
        return await to_thread(self._flt.exists_and_insert, value, **kwargs)

    async def exists_and_insert_many(self, values: Any,
                                     **kwargs: Any) -> Any:
        return await to_thread(
            self._flt.exists_and_insert_many, values, **kwargs)

    async def close(self) -> None:
        await to_thread(self._flt.close)
