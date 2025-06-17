# -*- coding:utf-8 -*-
# author: kusen
# email: 1194542196@qq.com
# date: 2025/6/17


import os

from dupfilter.filters import Filter, decorate_warning


def get_files(base_path, allow_empty=False, deep=False):
    files = []
    for name in os.listdir(base_path):
        path = os.path.join(base_path, name)
        if os.path.isfile(path):
            if not allow_empty and os.path.getsize(path) <= 0:
                continue
            files.append(path)
        elif os.path.isdir(path) and deep:
            files.extend(get_files(path, allow_empty, deep))
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
