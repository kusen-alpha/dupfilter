import os
import sqlite3
import tempfile
import unittest

from dupfilter.filters.sql.pool import ConnectionPool, PooledSQLFilter
from dupfilter.filters.sql.sqlite import SQLiteFilter

from .helpers import quiet_logger


class ConnectionPoolTests(unittest.TestCase):
    def test_rejects_zero_size(self):
        with self.assertRaises(ValueError):
            ConnectionPool(connect=sqlite3.connect, size=0)

    def test_pooled_filter_roundtrip(self):
        with tempfile.TemporaryDirectory() as directory:
            db = os.path.join(directory, "dup.db")
            pool = ConnectionPool(
                connect=lambda: sqlite3.connect(db), size=2)
            flt = PooledSQLFilter(
                pool, "dup", filter_cls=SQLiteFilter, logger=quiet_logger())
            self.assertEqual(
                flt.exists_and_insert_many(["a", "b"]),
                [False, False])
            self.assertEqual(
                flt.exists_and_insert_many(["a", "b", "c"]),
                [True, True, False])
            self.assertTrue(flt.confirm("a"))
            self.assertTrue(flt.release("a"))
            self.assertFalse(flt.exists("a"))
            flt.close()

    def test_pool_reuses_connections(self):
        created = []

        with tempfile.TemporaryDirectory() as directory:
            db = os.path.join(directory, "dup.db")

            def connect():
                conn = sqlite3.connect(db)
                created.append(conn)
                return conn

            pool = ConnectionPool(connect=connect, size=1)
            flt = PooledSQLFilter(
                pool, "dup", filter_cls=SQLiteFilter, logger=quiet_logger())
            flt.insert("x")
            flt.exists("x")
            self.assertEqual(len(created), 1)  # one connection reused
            flt.close()
