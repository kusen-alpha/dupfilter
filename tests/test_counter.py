# -*- coding:utf-8 -*-
import unittest

from dupfilter.filters import FilterCounter


class FilterCounterTests(unittest.TestCase):
    def test_appeared_uses_given_index(self):
        counter = FilterCounter([False, True, False])
        self.assertFalse(counter.appeared(1))
        self.assertTrue(counter.appeared(2))
        self.assertFalse(counter.appeared(3))
        self.assertFalse(counter.appeared(0))
        self.assertFalse(counter.appeared(4))

    def test_count_helpers(self):
        counter = FilterCounter([False, True, False, True])
        self.assertEqual(counter.count(True), 2)
        self.assertEqual(counter.count(False), 2)
        self.assertTrue(counter.any(True))
        self.assertFalse(counter.all(True))
        self.assertTrue(counter.reach(2, True))
