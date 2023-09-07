# -*- coding:utf-8 -*-
# author: kusen
# email: 1194542196@qq.com
# date: 2023/9/7
import os

from dupfilter.filters import Filter


class FileFilter(Filter):
    def __init__(self, path='./', *args, **kwargs):
        self.file = open(os.path.join(path, 'dup'), 'a+')
        self.file.seek(0)
        self.dups = set()
        self.dups.update(x.rstrip() for x in self.file)
        super(FileFilter, self).__init__(*args, **kwargs)

    def exists(self, value):
        value = self._value_hash_and_compress(value)
        return value in self.dups

    def insert(self, value):
        value = self._value_hash_and_compress(value)
        if value not in self.dups:
            self.dups.add(value)
            self.file.write(value + '\n')
        return True

    def exists_and_insert(self, value):
        value = self._value_hash_and_compress(value)
        stats = value in self.dups
        if not stats:
            self.dups.add(value)
            self.file.write(value + '\n')
        return stats

    def exists_many(self, values):
        return [self.exists(value) for value in values]

    def insert_many(self, values):
        return [self.insert(value) for value in values]

    def exists_and_insert_many(self, values):
        return [self.exists_and_insert(value) for value in values]
