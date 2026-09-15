# -*- coding:utf-8 -*-
import unittest

from dupfilter.filters.memory import MemoryFilter

from .helpers import quiet_logger


def boom(value):
    raise RuntimeError("boom")


class MemoryFilterTests(unittest.TestCase):
    def setUp(self):
        self.flt = MemoryFilter(logger=quiet_logger())

    def test_keyword_arguments(self):
        self.assertFalse(self.flt.exists(value="1"))
        self.assertEqual(
            self.flt.exists_many(values=["1", "2"]), [False, False])
        self.assertTrue(self.flt.insert(value="1"))
        self.assertTrue(self.flt.exists("1"))

    def test_fallback_shape_for_tuple(self):
        flt = MemoryFilter(value_hash_func=boom, logger=quiet_logger())
        self.assertEqual(flt.exists_many(("a", "b")), [False, False])

    def test_fallback_shape_for_generator(self):
        flt = MemoryFilter(value_hash_func=boom, logger=quiet_logger())
        self.assertEqual(flt.exists_many(iter(["a", "b"])), [False, False])

    def test_default_stat_on_failure(self):
        flt = MemoryFilter(
            value_hash_func=boom, default_stat=True, logger=quiet_logger())
        self.assertTrue(flt.exists("a"))
        self.assertEqual(flt.exists_many(["a", "b"]), [True, True])

    def test_raise_on_error(self):
        flt = MemoryFilter(
            value_hash_func=boom, raise_on_error=True, logger=quiet_logger())
        with self.assertRaises(RuntimeError):
            flt.exists("a")

    def test_basic_deduplication(self):
        self.assertFalse(self.flt.exists_and_insert("1"))
        self.assertTrue(self.flt.exists_and_insert("1"))
        self.assertTrue(self.flt.insert("2"))
        self.assertEqual(
            self.flt.exists_and_insert_many(["1", "2", "3"]),
            [True, True, False])
