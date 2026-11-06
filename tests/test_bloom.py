# -*- coding:utf-8 -*-
import asyncio
import unittest

from dupfilter.filters.bloom import (
    AsyncMemoryBloomFilter,
    AsyncScalableBloomFilter,
    MemoryBloomFilter,
    ScalableBloomFilter,
)

from .helpers import quiet_logger


class MemoryBloomFilterTests(unittest.TestCase):
    def test_exists_and_insert(self):
        flt = MemoryBloomFilter(
            expected_items=1000, false_positive_rate=0.01,
            logger=quiet_logger())
        self.assertFalse(flt.exists("a"))
        self.assertFalse(flt.exists_and_insert("a"))
        self.assertTrue(flt.exists_and_insert("a"))
        self.assertTrue(flt.exists("a"))

    def test_exists_and_insert_many_dedups(self):
        flt = MemoryBloomFilter(
            expected_items=100, false_positive_rate=0.01,
            logger=quiet_logger())
        self.assertEqual(
            flt.exists_and_insert_many(["a", "a", "b"]), [False, True, False])
        self.assertEqual(
            flt.exists_and_insert_many(["a", "b", "c"]), [True, True, False])

    def test_false_positive_stays_below_target(self):
        flt = MemoryBloomFilter(
            expected_items=1000, false_positive_rate=0.01,
            logger=quiet_logger())
        for i in range(999):
            flt.insert(f"in{i}")
        false_positives = sum(
            flt.exists(f"out{i}") for i in range(1000))
        self.assertLess(false_positives, 50)

    def test_invalid_args_rejected(self):
        with self.assertRaises(ValueError):
            MemoryBloomFilter(expected_items=0)
        with self.assertRaises(ValueError):
            MemoryBloomFilter(
                expected_items=100, false_positive_rate=1.0)

    def test_clear_and_count(self):
        flt = MemoryBloomFilter(
            expected_items=100, false_positive_rate=0.01,
            logger=quiet_logger())
        flt.insert_many(["1", "2", "3"])
        self.assertEqual(flt.count(), 3)
        flt.clear()
        self.assertEqual(flt.count(), 0)
        self.assertFalse(flt.exists("1"))

    def test_manual_bit_and_hash_num(self):
        flt = MemoryBloomFilter(
            bit=64, hash_num=4, logger=quiet_logger())
        flt.insert("x")
        self.assertTrue(flt.exists("x"))


class ScalableBloomFilterTests(unittest.TestCase):
    def test_grows_beyond_initial_capacity(self):
        flt = ScalableBloomFilter(
            initial_capacity=50, false_positive_rate=0.01,
            logger=quiet_logger())
        for i in range(300):
            flt.insert(f"k{i}")
        self.assertTrue(flt.exists("k299"))
        self.assertEqual(flt.count(), 300)
        # Cross-layer aggregate false positives stay bounded.
        false_positives = sum(
            flt.exists(f"missing{i}") for i in range(100))
        self.assertLess(false_positives, 40)

    def test_exists_and_insert_many(self):
        flt = ScalableBloomFilter(
            initial_capacity=10, false_positive_rate=0.01,
            logger=quiet_logger())
        self.assertEqual(
            flt.exists_and_insert_many(["a", "a", "b"]), [False, True, False])

    def test_clear_and_count(self):
        flt = ScalableBloomFilter(
            initial_capacity=10, false_positive_rate=0.01,
            logger=quiet_logger())
        flt.insert_many(["1", "2", "3"])
        self.assertGreaterEqual(flt.count(), 3)
        flt.clear()
        self.assertEqual(flt.count(), 0)
        self.assertFalse(flt.exists("1"))


class AsyncBloomFilterTests(unittest.TestCase):
    def test_async_memory_bloom(self):
        flt = AsyncMemoryBloomFilter(
            expected_items=100, false_positive_rate=0.01,
            logger=quiet_logger())
        self.assertFalse(asyncio.run(flt.exists_and_insert("a")))
        self.assertTrue(asyncio.run(flt.exists("a")))

    def test_async_scalable_bloom(self):
        flt = AsyncScalableBloomFilter(
            initial_capacity=10, false_positive_rate=0.01,
            logger=quiet_logger())
        asyncio.run(flt.insert_many(["1", "2", "3"]))
        self.assertTrue(asyncio.run(flt.exists("2")))


if __name__ == "__main__":
    unittest.main()
