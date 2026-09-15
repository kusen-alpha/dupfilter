# -*- coding:utf-8 -*-
import sqlite3
import unittest

from dupfilter.filters.sql import SQLFilter
from dupfilter.filters.sql.oracle import OracleSQLFilter
from dupfilter.filters.sql.sqlite import SQLiteFilter

from .helpers import quiet_logger


class SQLiteFilterTests(unittest.TestCase):
    def setUp(self):
        self.connection = sqlite3.connect(":memory:")
        self.flt = SQLiteFilter(
            self.connection, "dup", logger=quiet_logger())

    def tearDown(self):
        self.connection.close()

    def test_duplicate_insert_returns_true(self):
        self.assertTrue(self.flt.insert("a"))
        self.assertTrue(self.flt.insert("a"))
        self.assertTrue(self.flt.exists("a"))

    def test_duplicate_insert_many_returns_true(self):
        self.assertEqual(self.flt.insert_many(["a", "a", "b"]),
                         [True, True, True])

    def test_exists_and_insert_many_with_duplicates(self):
        self.assertEqual(
            self.flt.exists_and_insert_many(["a", "a", "b"]),
            [False, False, False])
        self.assertEqual(
            self.flt.exists_and_insert_many(["a", "b", "c"]),
            [True, True, False])

    def test_empty_batch(self):
        self.assertEqual(self.flt.exists_many([]), [])
        self.assertEqual(self.flt.insert_many([]), [])

    def test_exists_sql_uses_placeholders(self):
        sql, params, new_values = self.flt._exists_sql(["a", "b", "c"])
        self.assertIn("(?, ?, ?)", sql)
        self.assertEqual(len(params), len(new_values))

    def test_identity_hash_value_is_not_injected(self):
        flt = SQLiteFilter(
            self.connection, "dup2", value_hash_func=None,
            logger=quiet_logger())
        evil = "x'); DROP TABLE dup;--"
        self.assertTrue(flt.insert(evil))
        self.assertTrue(flt.exists(evil))
        cursor = self.connection.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name='dup'")
        self.assertIsNotNone(cursor.fetchone())

    def test_placeholder_helpers(self):
        self.assertEqual(SQLFilter._placeholder_many(None, 2), "%s, %s")
        self.assertEqual(SQLiteFilter._placeholder_many(None, 2), "?, ?")
        self.assertEqual(
            OracleSQLFilter._placeholder_many(None, 3), ":1, :2, :3")


class OracleSQLFilterTests(unittest.TestCase):
    class FakeOracle(object):
        table = "dup"
        record_time = False

        def _value_hash_and_compress(self, value):
            return "h" + value

    def test_insert_sql_without_record_time_is_well_formed(self):
        fake = OracleSQLFilterTests.FakeOracle()
        sql, values = OracleSQLFilter._insert_sql(fake, ["a"])
        self.assertIn("IGNORE_ROW_ON_DUPKEY_INDEX", sql)
        self.assertIn("(id)) */ INTO dup", sql)
        self.assertNotIn(" dup INTO", sql)
        self.assertIn("VALUES (:1)", sql)
        self.assertEqual(values, ["ha"])

    def test_insert_sql_with_record_time(self):
        fake = OracleSQLFilterTests.FakeOracle()
        fake.record_time = True
        sql, values = OracleSQLFilter._insert_sql(fake, ["a"])
        self.assertIn("(id, insert_time) VALUES (:1,:2)", sql)
        self.assertIn("(id)) */ INTO dup", sql)
        self.assertEqual(len(values), 1)
        self.assertEqual(values[0][0], "ha")

    def test_create_table_sql_avoids_if_not_exists(self):
        fake = OracleSQLFilterTests.FakeOracle()
        sql = OracleSQLFilter._create_table_sql(fake)
        self.assertNotIn("IF NOT EXISTS", sql)
        self.assertIn("PRIMARY KEY (id)", sql)
        fake.record_time = True
        sql = OracleSQLFilter._create_table_sql(fake)
        self.assertNotIn("IF NOT EXISTS", sql)
        self.assertIn("insert_time", sql)
