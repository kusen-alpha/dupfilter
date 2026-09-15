# -*- coding:utf-8 -*-
import asyncio
import unittest
from unittest import mock

from dupfilter.filters import Filter, Reset, decorate_reset
from dupfilter.filters.memory import MemoryFilter, MemoryReset

from .helpers import quiet_logger


class CountingMemoryReset(MemoryReset):
    def __init__(self, *args, **kwargs):
        self.reset_calls = 0
        super(CountingMemoryReset, self).__init__(*args, **kwargs)

    def reset(self):
        self.reset_calls += 1
        super(CountingMemoryReset, self).reset()


class FakeReset(Reset):
    def __init__(self, *args, **kwargs):
        self.reset_calls = 0
        super(FakeReset, self).__init__(*args, **kwargs)

    @property
    def current_count(self):
        return 10

    def reset(self):
        self.reset_calls += 1


class ResetTests(unittest.TestCase):
    def test_parameter_validation(self):
        with self.assertRaises(ValueError):
            MemoryReset(max_count=0)
        with self.assertRaises(ValueError):
            MemoryReset(max_count=10, max_rate=2)
        with self.assertRaises(ValueError):
            MemoryReset(max_count=10, max_rate=0.4, reset_to_rate=0.5)
        with self.assertRaises(ValueError):
            MemoryReset(max_count=10, monitor_time=0)

    def test_timer_with_monotonic(self):
        with mock.patch(
                "dupfilter.filters.time.monotonic", return_value=100.0):
            reset = MemoryReset(max_count=10, monitor_time=60)
            self.assertTrue(reset.get_timer())
        with mock.patch(
                "dupfilter.filters.time.monotonic", return_value=161.0):
            self.assertFalse(reset.get_timer())

    def test_reset_runs_once_then_timer_blocks(self):
        reset = CountingMemoryReset(
            max_count=6, max_rate=0.5, reset_to_rate=0.25,
            monitor_time=3600)
        reset._next_reset_at = 0.0
        flt = MemoryFilter(reset=reset, logger=quiet_logger())
        for i in range(8):
            flt.insert(str(i))
        self.assertEqual(reset.reset_calls, 1)
        self.assertLessEqual(len(flt.dups), 6)

    def test_async_reset_calls_sync_reset(self):
        reset = FakeReset(
            max_count=6, max_rate=0.5, reset_to_rate=0.25,
            monitor_time=3600)
        reset._next_reset_at = 0.0

        class AsyncResettingFilter(Filter):
            def __init__(self):
                super(AsyncResettingFilter, self).__init__(
                    logger=quiet_logger())
                self.reset = reset
                reset.flt = self

            @decorate_reset
            async def insert(self, value):
                return True

        flt = AsyncResettingFilter()
        self.assertTrue(asyncio.run(flt.insert("x")))
        self.assertEqual(reset.reset_calls, 1)
