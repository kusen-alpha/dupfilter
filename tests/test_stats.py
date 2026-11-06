# -*-coding:utf-8 -*-
import asyncio
import sqlite3
import unittest

from dupfilter.filters.memory import AsyncMemoryFilter, MemoryFilter
from dupfilter.filters.sql.sqlite import SQLiteFilter
from dupfilter.filters.stats import AsyncStatsFilter, StatsFilter

from .helpers import quiet_logger


class StatsFilterTests(unittest.TestCase):
    def setUp(self):
        self.base = MemoryFilter(logger=quiet_logger())
        self.flt = StatsFilter(self.base)

    def test_hit_miss_counting(self):
        self.assertFalse(self.flt.exists_and_insert("a"))
        self.assertTrue(self.flt.exists_and_insert("a"))
        self.assertEqual(self.flt.total, 2)
        self.assertEqual(self.flt.hits, 1)
        self.assertEqual(self.flt.misses, 1)
        self.assertEqual(self.flt.hit_rate, 0.5)
        self.assertEqual(self.flt.miss_rate, 0.5)

    def test_many_counts_each_item(self):
        self.flt.exists_and_insert_many(["a", "a", "b"])
        self.assertEqual(self.flt.total, 3)
        self.assertEqual(self.flt.hits, 1)

    def test_exists_records(self):
        self.base.insert("a")
        self.assertTrue(self.flt.exists("a"))
        self.assertEqual(self.flt.total, 1)
        self.assertEqual(self.flt.hits, 1)

    def test_count_delegates_to_dups(self):
        self.base.insert_many(["1", "2", "3"])
        self.assertEqual(self.flt.count(), 3)

    def test_count_delegates_to_sql(self):
        connection = sqlite3.connect(":memory:")
        try:
            base = SQLiteFilter(
                connection, "dup", logger=quiet_logger())
            flt = StatsFilter(base)
            flt.insert("a")
            self.assertEqual(flt.count(), 1)
        finally:
            connection.close()

    def test_reset_stats(self):
        self.flt.exists_and_insert("a")
        self.flt.reset_stats()
        self.assertEqual(self.flt.total, 0)
        self.assertEqual(self.flt.hit_rate, 0.0)


class AsyncStatsFilterTests(unittest.TestCase):
    def test_async_hit_miss_counting(self):
        base = AsyncMemoryFilter(logger=quiet_logger())
        flt = AsyncStatsFilter(base)

        async def run():
            self.assertFalse(await flt.exists_and_insert("a"))
            self.assertTrue(await flt.exists_and_insert("a"))
            self.assertEqual(flt.total, 2)
            self.assertEqual(flt.hit_rate, 0.5)

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
