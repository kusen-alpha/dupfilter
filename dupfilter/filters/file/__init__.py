# -*- coding:utf-8 -*-
# author: kusen
# email: 1194542196@qq.com
# date: 2023/9/7


import os
import time

from dupfilter.filters import decorate_warning
from dupfilter.filters.memory import MemoryFilter


class FileFilter(MemoryFilter):
    def __init__(self,
                 path='./',
                 *args, **kwargs):
        self.path = path
        self.file = open(os.path.join(self.path, 'dup'), 'a+')
        self.file.seek(0)
        super(FileFilter, self).__init__(*args, **kwargs)
        self.dups.update(x.rstrip() for x in self.file)

    def _insert(self, value):
        self.dups.add(value)
        self.file.write(value + '\n')

    @decorate_warning
    def insert(self, value):
        value = self._value_hash_and_compress(value)
        if value not in self.dups:
            self._insert(value)
        return True

    @decorate_warning
    def exists_and_insert(self, value):
        value = self._value_hash_and_compress(value)
        stat = value in self.dups
        if not stat:
            self._insert(value)
        return stat

# class FileFilter(MemoryFilter):
#     def __init__(self,
#                  path='./',
#                  reset_to_proportion=0.6,
#                  *args, **kwargs):
#         self.path = path
#         self.reset_to_proportion = reset_to_proportion
#         self.file = open(os.path.join(self.path, 'dup'), 'a+')
#         self.file.seek(0)
#         super(FileFilter, self).__init__(*args, **kwargs)
#         self.dups.update(x.rstrip() for x in self.file)
#
#     def _reset(self):
#         self._resetting = True
#         if self.cache.get('proportion') is None:
#             self.cache['proportion'] = self.proportion
#         proportion = self.cache.get('proportion')
#         if not proportion or (proportion < self.reset_proportion):
#             self._resetting = False
#             return
#         size = len(self.dups) - int(self.maxsize * self.reset_to_proportion)
#         self.file.seek(0)
#         for i in range(size):
#             self.file.readline()
#         self.dups.clear()
#         self.dups.update(x.rstrip() for x in self.file)
#         self.file.truncate(0)
#         for item in self.dups:
#             self.file.write(item + '\n')
#         self.file.flush()
#         self.cache['proportion'] = self.proportion
#         self._resetting = False
#
#     def _add(self, value):
#         if self._resetting:
#             while True:
#                 time.sleep(0.1)
#                 if not self._resetting:
#                     break
#         self.dups.add(value)
#         self.file.write(value + '\n')
#         if self.reset:
#             self._reset()
#
#     def insert(self, value):
#         value = self._value_hash_and_compress(value)
#         if value not in self.dups:
#             self._add(value)
#         return True
#
#     def exists_and_insert(self, value):
#         value = self._value_hash_and_compress(value)
#         stat = value in self.dups
#         if not stat:
#             self._add(value)
#         return stat
