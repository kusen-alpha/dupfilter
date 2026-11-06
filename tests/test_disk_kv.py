# -*-coding:utf-8 -*-
import asyncio
import os
import tempfile
import unittest

try:
    import aiosqlite
except ImportError:
    aiosqlite = None

from dupfilter.filters.disk_kv import AsyncDiskKVFilter, DiskKVFilter

from .helpers import quiet_logger


class DiskKVFilterTests(unittest.TestCase):
    def setUp(self):
        self.path = os.path.join(tempfile.mkdtemp(), "dup.db")
        self.flt = DiskKVFilter(self.path, logger=quiet_logger())

    def tearDown(self):
        self.flt.close()
        if os.path.exists(self.path):
            os.remove(self.path)

    def test_exists_and_insert_many(self):
        self.assertEqual(
            self.flt.exists_and_insert_many(["a", "a", "b"]),
            [False, True, False])
        self.assertEqual(self.flt.count(), 2)
        self.assertTrue(self.flt.exists("a"))

    def test_release_and_count(self):
        self.flt.insert("a")
        self.assertEqual(self.flt.count(), 1)
        self.assertTrue(self.flt.release("a"))
        self.assertEqual(self.flt.count(), 0)

    def test_two_phase(self):
        self.assertFalse(self.flt.exists_and_lock("x"))
        self.assertTrue(self.flt.confirm("x"))
        self.assertTrue(self.flt.exists("x"))
        self.assertTrue(self.flt.release("x"))
        self.assertFalse(self.flt.exists("x"))

    def test_persists_across_reopen(self):
        self.flt.insert("persist-me")
        self.flt.close()
        flt = DiskKVFilter(self.path, logger=quiet_logger())
        try:
            self.assertTrue(flt.exists("persist-me"))
            self.assertEqual(flt.count(), 1)
        finally:
            flt.close()


@unittest.skipUnless(aiosqlite, "aiosqlite not installed")
class AsyncDiskKVFilterTests(unittest.TestCase):
    def test_async_roundtrip(self):
        path = os.path.join(tempfile.mkdtemp(), "dup_async.db")

        async def run():
            flt = AsyncDiskKVFilter(path, logger=quiet_logger())
            self.assertFalse(await flt.exists_and_insert("a"))
            self.assertTrue(await flt.exists("a"))
            self.assertEqual(await flt.count(), 1)
            await flt.close()

        try:
            asyncio.run(run())
        finally:
            if os.path.exists(path):
                os.remove(path)


if __name__ == "__main__":
    unittest.main()
