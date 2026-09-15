# -*- coding:utf-8 -*-
import os
import tempfile
import unittest

from dupfilter.filters.file import FileFilter
from dupfilter.filters.memory import MemoryReset

from .helpers import quiet_logger


class FileFilterTests(unittest.TestCase):
    def test_persist_and_close(self):
        with tempfile.TemporaryDirectory() as directory:
            flt = FileFilter(path=directory, logger=quiet_logger())
            flt.insert("a")
            flt.insert("b")
            flt.close()

            flt2 = FileFilter(path=directory, logger=quiet_logger())
            self.assertTrue(flt2.exists("a"))
            self.assertTrue(flt2.exists("b"))
            flt2.close()

    def test_reset_rewrites_file_without_gaps(self):
        with tempfile.TemporaryDirectory() as directory:
            reset = MemoryReset(
                max_count=6, max_rate=0.5, reset_to_rate=0.25,
                monitor_time=3600)
            flt = FileFilter(
                path=directory, reset=reset, logger=quiet_logger())
            for i in range(6):
                flt.insert(str(i))

            with open(
                    os.path.join(directory, "dup"),
                    encoding="utf-8") as handle:
                content = handle.read()
            self.assertNotIn("\x00", content)
            lines = content.splitlines()
            self.assertLessEqual(len(lines), 6)
            for line in lines:
                self.assertIn(line, flt.dups)
            flt.close()

    def test_creates_parent_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            target = os.path.join(directory, "nested", "path")
            flt = FileFilter(path=target, logger=quiet_logger())
            self.assertTrue(flt.insert("x"))
            flt.close()
            self.assertTrue(os.path.isfile(os.path.join(target, "dup")))
