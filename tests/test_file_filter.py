# -*- coding:utf-8 -*-
import asyncio
import os
import tempfile
import threading
import unittest

from dupfilter.filters.file import AsyncFileFilter, FileFilter
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

    def test_concurrent_exists_and_insert_writes_once(self):
        with tempfile.TemporaryDirectory() as directory:
            flt = FileFilter(path=directory, logger=quiet_logger())

            def worker():
                for _ in range(50):
                    flt.exists_and_insert("shared")

            threads = [threading.Thread(target=worker) for _ in range(4)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            self.assertEqual(len(flt.dups), 1)  # lock prevented duplicates
            flt.close()
            with open(
                    os.path.join(directory, "dup"),
                    encoding="utf-8") as handle:
                lines = [line.rstrip() for line in handle if line.strip()]
            self.assertEqual(len(lines), 1)

    def test_close_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            flt = FileFilter(path=directory, logger=quiet_logger())
            flt.insert("a")
            flt.close()
            flt.close()


class AsyncFileFilterTests(unittest.TestCase):
    def test_awaitable_roundtrip(self):
        async def case():
            with tempfile.TemporaryDirectory() as directory:
                flt = AsyncFileFilter(path=directory, logger=quiet_logger())
                self.assertFalse(await flt.exists_and_insert("a"))
                self.assertTrue(await flt.exists_and_insert("a"))
                self.assertTrue(await flt.insert("b"))
                self.assertEqual(
                    await flt.exists_many(["a", "b", "c"]),
                    [True, True, False])
                await flt.close()

        asyncio.run(case())
