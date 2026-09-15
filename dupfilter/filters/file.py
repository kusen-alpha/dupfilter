# -*- coding:utf-8 -*-
# author: kusen
# email: 1194542196@qq.com
# date: 2023/9/7


import os
import random

from dupfilter.filters import decorate_warning, decorate_reset
from dupfilter.filters.memory import MemoryFilter
from dupfilter.filters.memory import MemoryReset


class FileReset(MemoryReset):

    def reset(self):
        super(FileReset, self).reset()
        self.flt.file.seek(0)
        self.flt.file.truncate()
        for dup in self.flt.dups:
            self.flt.file.write(dup + '\n')
        self.flt.file.flush()


class FileFilter(MemoryFilter):
    def __init__(self,
                 path='./',
                 *args, **kwargs):
        self.path = path
        os.makedirs(self.path, exist_ok=True)
        self.file = open(os.path.join(self.path, 'dup'), 'a+')
        self.file.seek(0)
        super(FileFilter, self).__init__(*args, **kwargs)
        self.dups.update(x.rstrip() for x in self.file)

    @decorate_reset
    def _insert(self, value):
        self.dups.add(value)
        self.file.write(value + '\n')

    @decorate_warning
    def insert(self, value):
        new_value = self._value_hash_and_compress(value)
        if new_value not in self.dups:
            self._insert(new_value)
        return True

    @decorate_warning
    def exists_and_insert(self, value):
        new_value = self._value_hash_and_compress(value)
        stat = new_value in self.dups
        self._log_exists([value], [new_value], [stat])
        if not stat:
            self._insert(new_value)
        return stat

    def close(self):
        try:
            self.file.flush()
            self.file.close()
        except Exception as e:
            self.logger.warning("文件去重句柄关闭失败：%s" % str(e))
