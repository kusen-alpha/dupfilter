# -*-coding:utf-8 -*-
import asyncio
import unittest

from dupfilter.filters.sketch import (
    AsyncCountMinSketch,
    AsyncHyperLogLog,
    CountMinSketch,
    HyperLogLog,
)


class HyperLogLogTests(unittest.TestCase):
    def test_cardinality_within_tolerance(self):
        hll = HyperLogLog(precision=14)
        for i in range(5000):
            hll.add(str(i))
        estimate = hll.count()
        self.assertAlmostEqual(estimate, 5000, delta=400)

    def test_duplicates_do_not_inflate(self):
        hll = HyperLogLog(precision=14)
        values = [str(i) for i in range(5000)]
        for value in values:
            hll.add(value)
        first = hll.count()
        for value in values:
            hll.add(value)
        self.assertLess(abs(hll.count() - first), 1)

    def test_clear(self):
        hll = HyperLogLog(precision=14)
        hll.add_many(["1", "2", "3"])
        hll.clear()
        self.assertLess(hll.count(), 10)

    def test_invalid_precision_rejected(self):
        with self.assertRaises(ValueError):
            HyperLogLog(precision=3)


class CountMinSketchTests(unittest.TestCase):
    def test_estimate_never_underestimates(self):
        cms = CountMinSketch(width=100, depth=3)
        for _ in range(100):
            cms.add("hot")
        for i in range(1000):
            cms.add(f"noise{i}")
        self.assertGreaterEqual(cms.estimate("hot"), 100)

    def test_estimate_without_collisions(self):
        cms = CountMinSketch(width=10000, depth=5)
        for _ in range(1000):
            cms.add("hot")
        self.assertEqual(cms.estimate("hot"), 1000)
        self.assertEqual(cms.estimate("cold"), 0)

    def test_from_error_rate(self):
        cms = CountMinSketch.from_error_rate(
            expected_items=10000, error_rate=0.001, confidence=0.99)
        self.assertGreaterEqual(cms.width, 10000)
        self.assertGreaterEqual(cms.depth, 4)

    def test_clear(self):
        cms = CountMinSketch(width=100, depth=3)
        cms.add("a")
        cms.clear()
        self.assertEqual(cms.estimate("a"), 0)


class AsyncSketchTests(unittest.TestCase):
    def test_async_hll(self):
        hll = AsyncHyperLogLog(precision=10)
        asyncio.run(hll.add_many([str(i) for i in range(1000)]))
        self.assertAlmostEqual(asyncio.run(hll.count()), 1000, delta=100)

    def test_async_cms(self):
        cms = AsyncCountMinSketch(width=1000, depth=3)
        asyncio.run(cms.add("hot", 50))
        self.assertEqual(asyncio.run(cms.estimate("hot")), 50)


if __name__ == "__main__":
    unittest.main()
