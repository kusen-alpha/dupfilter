# -*- coding:utf-8 -*-
# author: kusen
# email: 1194542196@qq.com
# date: 2025/6/17

from __future__ import annotations

import os
from collections.abc import Iterable
from typing import Any, Callable

from dupfilter.filters import Filter, decorate_warning, to_thread


def get_files(base_path: str, allow_empty: bool = False,
              deep: bool = False) -> list[str]:
    files = []
    with os.scandir(base_path) as iterator:
        for entry in iterator:
            if entry.is_file(follow_symlinks=False):
                if not allow_empty and entry.stat(
                        follow_symlinks=False).st_size <= 0:
                    continue
                files.append(entry.path)
            elif entry.is_dir(follow_symlinks=False) and deep:
                files.extend(get_files(entry.path, allow_empty, deep))
    return files


class FilePathFilter(Filter):
    def __init__(self, base_path: str, allow_empty: bool = False,
                 deep: bool = False,
                 value_hash_func: Callable[[Any], Any] | None = None,
                 *args: Any, **kwargs: Any) -> None:
        self.base_path = base_path
        self.allow_empty = allow_empty
        self.deep = deep
        kwargs["value_hash_func"] = value_hash_func
        super().__init__(*args, **kwargs)

    @decorate_warning
    def exists(self, value: Any) -> bool:
        files = get_files(self.base_path, self.allow_empty, self.deep)
        names = [os.path.basename(file) for file in files]
        return value in names

    @decorate_warning
    def exists_many(self, values: Iterable[Any]) -> list[bool]:
        files = get_files(self.base_path, self.allow_empty, self.deep)
        names = [os.path.basename(file) for file in files]
        return [value in names for value in values]


class AsyncFilePathFilter:
    """Async API over :class:`FilePathFilter`.

    Directory scanning is blocking, so each call runs in a worker thread
    via :func:`to_thread` and keeps the event loop responsive.
    """

    def __init__(self, base_path: str, allow_empty: bool = False,
                 deep: bool = False, *args: Any, **kwargs: Any) -> None:
        kwargs["allow_empty"] = allow_empty
        kwargs["deep"] = deep
        self._flt: Any = FilePathFilter(base_path, *args, **kwargs)

    async def exists(self, value: Any, **kwargs: Any) -> Any:
        return await to_thread(self._flt.exists, value, **kwargs)

    async def exists_many(self, values: Iterable[Any],
                          **kwargs: Any) -> Any:
        return await to_thread(self._flt.exists_many, values, **kwargs)


if __name__ == '__main__':
    pass
