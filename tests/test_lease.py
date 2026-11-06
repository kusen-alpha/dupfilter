# -*-coding:utf-8 -*-
import asyncio
import sqlite3
import time
import unittest

try:
    import aiosqlite
except ImportError:
    aiosqlite = None

from dupfilter.filters.lease import AsyncLeaseFilter, LeaseFilter
from dupfilter.filters.sql.sqlite import AsyncSQLiteFilter, SQLiteFilter

from .helpers import quiet_logger


class LeaseFilterTests(unittest.TestCase):
    def setUp(self):
        self.connection = sqlite3.connect(":memory:")
        self.base = SQLiteFilter(
            self.connection, "dup", logger=quiet_logger())
        self.flt = LeaseFilter(self.base, default_lease=0.05)

    def tearDown(self):
        self.connection.close()

    def test_claim_hold_release(self):
        self.assertFalse(self.flt.exists_and_lock("a"))
        self.assertTrue(self.flt.exists_and_lock("a"))
        self.assertTrue(self.flt.release("a"))
        self.assertFalse(self.flt.exists_and_lock("a"))

    def test_lease_expiry_reclaims(self):
        self.assertFalse(self.flt.exists_and_lock("a"))
        self.assertTrue(self.flt.exists_and_lock("a"))
        time.sleep(0.08)
        self.assertFalse(self.flt.exists_and_lock("a"))

    def test_confirm_clears_lease(self):
        self.assertFalse(self.flt.exists_and_lock("a"))
        self.assertTrue(self.flt.confirm("a"))
        self.assertTrue(self.flt.exists("a"))

    def test_foreign_claim_never_records_lease(self):
        self.base.insert("a")
        self.assertTrue(self.flt.exists_and_lock("a"))
        self.assertEqual(self.flt._leases, {})

    def test_clock_injection(self):
        now = [0.0]
        flt = LeaseFilter(self.base, default_lease=10,
                          clock=lambda: now[0])
        self.assertFalse(flt.exists_and_lock("a"))
        now[0] = 5.0
        self.assertTrue(flt.exists_and_lock("a"))
        now[0] = 11.0
        self.assertFalse(flt.exists_and_lock("a"))


@unittest.skipUnless(aiosqlite, "aiosqlite not installed")
class AsyncLeaseFilterTests(unittest.TestCase):
    def test_async_claim_hold_release(self):
        async def run():
            connection = await aiosqlite.connect(":memory:")
            try:
                base = AsyncSQLiteFilter(
                    connection, "dup", logger=quiet_logger())
                flt = AsyncLeaseFilter(base, default_lease=0.05)
                self.assertFalse(await flt.exists_and_lock("a"))
                self.assertTrue(await flt.exists_and_lock("a"))
                await asyncio.sleep(0.08)
                self.assertFalse(await flt.exists_and_lock("a"))
            finally:
                await connection.close()

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
