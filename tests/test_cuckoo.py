# -*-coding:utf-8 -*-
import asyncio
import unittest

from dupfilter.filters.cuckoo import AsyncCuckooFilter, CuckooFilter

from .helpers import quiet_logger


class CuckooFilterTests(unittest.TestCase):
    def setUp(self):
        self.flt = CuckooFilter(
            expected_items=1000, logger=quiet_logger())

    def test_insert_exists(self):
        self.flt.insert("a")
        self.assertTrue(self.flt.exists("a"))
        self.assertFalse(self.flt.exists("b"))

    def test_exists_and_insert(self):
        self.assertFalse(self.flt.exists_and_insert("a"))
        self.assertTrue(self.flt.exists_and_insert("a"))

    def test_exists_and_insert_many_dedups(self):
        self.assertEqual(
            self.flt.exists_and_insert_many(["a", "a", "b"]),
            [False, True, False])

    def test_release_removes_value(self):
        self.flt.insert("a")
        self.assertTrue(self.flt.release("a"))
        self.assertFalse(self.flt.exists("a"))
        self.assertFalse(self.flt.release("a"))

    def test_release_many(self):
        self.flt.insert_many(["a", "b"])
        self.assertEqual(self.flt.release_many(["a", "b"]), [True, True])
        self.assertEqual(self.flt.count(), 0)

    def test_count_and_clear(self):
        self.flt.insert_many(["1", "2", "3"])
        self.assertEqual(self.flt.count(), 3)
        self.flt.clear()
        self.assertEqual(self.flt.count(), 0)
        self.assertFalse(self.flt.exists("1"))


class AsyncCuckooFilterTests(unittest.TestCase):
    def test_async_roundtrip(self):
        flt = AsyncCuckooFilter(
            expected_items=100, logger=quiet_logger())
        self.assertFalse(asyncio.run(flt.exists_and_insert("a")))
        self.assertTrue(asyncio.run(flt.exists("a")))
        self.assertTrue(asyncio.run(flt.release("a")))
        self.assertFalse(asyncio.run(flt.exists("a")))


if __name__ == "__main__":
    unittest.main()
