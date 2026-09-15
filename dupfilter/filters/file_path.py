# -*- coding:utf-8 -*-
# author: kusen
# email: 1194542196@qq.com
# date: 2025/6/17


import os

from dupfilter.filters import Filter, decorate_warning


def get_files(base_path, allow_empty=False, deep=False):
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
    def __init__(self, base_path, allow_empty=False,
                 deep=False, value_hash_func=None,
                 *args, **kwargs):
        self.base_path = base_path
        self.allow_empty = allow_empty
        self.deep = deep
        super(FilePathFilter, self).__init__(
            value_hash_func=value_hash_func,
            *args, **kwargs)

    @decorate_warning
    def exists(self, value):
        files = get_files(self.base_path, self.allow_empty, self.deep)
        names = [os.path.basename(file) for file in files]
        return value in names

    @decorate_warning
    def exists_many(self, values):
        files = get_files(self.base_path, self.allow_empty, self.deep)
        names = [os.path.basename(file) for file in files]
        return [value in names for value in values]


if __name__ == '__main__':
    pass
