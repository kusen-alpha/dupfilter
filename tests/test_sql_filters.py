# -*- coding:utf-8 -*-
import asyncio
import sqlite3
import unittest

try:
    import aiosqlite
except ImportError:
    aiosqlite = None

from dupfilter import utils
from dupfilter.filters.sql import SQLFilter
from dupfilter.filters.sql.mysql import AsyncMySQLFilter, MySQLFilter
from dupfilter.filters.sql.oracle import AsyncOracleSQLFilter, OracleSQLFilter
from dupfilter.filters.sql.sqlite import AsyncSQLiteFilter, SQLiteFilter

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
            [False, True, False])
        self.assertEqual(
            self.flt.exists_and_insert_many(["a", "b", "c"]),
            [True, True, False])

    def test_batch_mode_uses_snapshot_semantics(self):
        flt = SQLiteFilter(
            self.connection, "dup", mode="batch", logger=quiet_logger())
        self.assertEqual(
            flt.exists_and_insert_many(["a", "a", "b"]),
            [False, False, False])
        self.assertEqual(
            flt.exists_and_insert_many(["a", "b", "c"]),
            [True, True, False])

    def test_insert_sql_only_skips_primary_key_conflict(self):
        sql, _ = self.flt._insert_sql(["a"])
        self.assertIn("ON CONFLICT(id) DO NOTHING", sql)
        self.assertNotIn("OR IGNORE", sql)

    def test_base_insert_sql_uses_on_duplicate_key_update(self):
        sql, _ = SQLFilter._insert_sql(self.flt, ["a"])
        self.assertIn("ON DUPLICATE KEY UPDATE id=id", sql)
        self.assertNotIn("INSERT IGNORE", sql)

    def test_empty_batch(self):
        self.assertEqual(self.flt.exists_many([]), [])
        self.assertEqual(self.flt.insert_many([]), [])

    def test_table_name_is_validated(self):
        with self.assertRaises(ValueError):
            SQLiteFilter(
                self.connection, "dup; DROP TABLE x", logger=quiet_logger())
        with self.assertRaises(ValueError):
            SQLiteFilter(
                self.connection, "select 1", logger=quiet_logger())
        with self.assertRaises(ValueError):
            SQLiteFilter(
                self.connection, "dup", mode="fast", logger=quiet_logger())

    def test_exists_sql_uses_placeholders(self):
        sql, params, new_values = self.flt._exists_sql(["a", "b", "c"])
        self.assertIn("(?, ?, ?)", sql)
        self.assertEqual(len(params), len(new_values))

    def test_exists_table_reflects_created_table(self):
        self.assertTrue(self.flt._exists_table())
        self.connection.execute("DROP TABLE dup")
        self.assertFalse(self.flt._exists_table())

    def test_atomic_mode_rolls_back_on_error(self):
        flt = SQLiteFilter(
            self.connection, "dup", logger=quiet_logger())
        real_cursor = flt.cursor

        class FlakyCursor:
            def __init__(self):
                self.n = 0

            def execute(self, sql, params=None):
                self.n += 1
                if self.n == 2:
                    raise RuntimeError("boom")
                return real_cursor.execute(sql, params)

        flt.cursor = FlakyCursor()
        stats = flt.exists_and_insert_many(["a", "b", "c"])
        self.assertEqual(stats, [False, False, False])  # fail-soft default
        flt.cursor = real_cursor
        self.assertFalse(flt.exists("a"))  # rolled back, nothing persisted

    def test_two_phase_claim_confirm_release(self):
        self.assertFalse(self.flt.exists_and_lock("a"))
        self.assertTrue(self.flt.exists_and_lock("a"))  # already claimed
        self.assertTrue(self.flt.confirm("a"))
        self.assertTrue(self.flt.release("a"))
        self.assertFalse(self.flt.exists("a"))  # released, row gone

    def test_release_many_deletes_rows(self):
        self.flt.insert_many(["a", "b", "c"])
        self.assertEqual(self.flt.release_many(["a", "c"]), [True, True])
        self.assertEqual(self.flt.exists_many(["a", "b", "c"]),
                         [False, True, False])

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


class FakeSyncCursor:
    def __init__(self, exists_row=None):
        self.executed = []
        self.fetchone_result = exists_row
        self.rowcount = 1

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def executemany(self, sql, values):
        self.executed.append((sql, values))
        self.rowcount = len(values)

    def fetchone(self):
        return self.fetchone_result

    def fetchall(self):
        return []

    def close(self):
        pass


class FakeSyncConnection:
    def __init__(self, exists_row=None):
        self.cursors = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = False
        self.exists_row = exists_row

    def cursor(self):
        cursor = FakeSyncCursor(self.exists_row)
        self.cursors.append(cursor)
        return cursor

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


class MySQLFilterTests(unittest.TestCase):
    def test_exists_table_queries_information_schema(self):
        conn = FakeSyncConnection(exists_row=("dup",))
        flt = MySQLFilter(conn, "dup", logger=quiet_logger())
        self.assertTrue(flt._exists_table())
        sql = conn.cursors[0].executed[0][0]
        self.assertIn("information_schema.TABLES", sql)

    def test_create_table_runs_when_missing(self):
        conn = FakeSyncConnection(exists_row=None)
        MySQLFilter(conn, "dup", logger=quiet_logger())
        create_sqls = [sql for sql, _ in conn.cursors[0].executed
                       if "CREATE TABLE" in sql]
        self.assertEqual(len(create_sqls), 1)
        self.assertIn("CREATE TABLE IF NOT EXISTS", create_sqls[0])

    def test_exists_many_with_fake_cursor(self):
        conn = FakeSyncConnection()
        flt = MySQLFilter(conn, "dup", logger=quiet_logger())
        self.assertEqual(flt.exists_many(["a", "b"]), [False, False])


class FakeOracleAsyncCursor:
    def __init__(self, rowcounts=None):
        self.rowcounts = list(rowcounts or [])
        self.rowcount = 1

    async def execute(self, sql, params=None):
        self.rowcount = self.rowcounts.pop(0) if self.rowcounts else 1

    async def executemany(self, sql, values):
        self.rowcount = len(values)

    async def fetchone(self):
        return None

    async def fetchall(self):
        return []

    def close(self):
        pass


class FakeOracleAsyncConnection:
    def __init__(self, cursor=None):
        self.cursor_obj = cursor or FakeOracleAsyncCursor()
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    async def commit(self):
        self.commits += 1

    async def rollback(self):
        self.rollbacks += 1

    async def close(self):
        self.closed = True


class AsyncOracleSQLFilterTests(unittest.TestCase):
    def test_atomic_maps_rowcount_to_stats(self):
        cursor = FakeOracleAsyncCursor([1, 0, 1])
        conn = FakeOracleAsyncConnection(cursor)
        flt = AsyncOracleSQLFilter(conn, "dup", logger=quiet_logger())
        flt._table_ready = True
        stats = asyncio.run(flt.exists_and_insert_many(["a", "a", "b"]))
        self.assertEqual(stats, [False, True, False])
        self.assertEqual(conn.commits, 1)

    def test_two_phase_awaitable(self):
        flt = AsyncOracleSQLFilter(
            FakeOracleAsyncConnection(), "dup", logger=quiet_logger())
        flt._table_ready = True
        self.assertFalse(asyncio.run(flt.exists_and_lock("x")))
        self.assertTrue(asyncio.run(flt.confirm("x")))

    def test_release_many(self):
        conn = FakeOracleAsyncConnection()
        flt = AsyncOracleSQLFilter(conn, "dup", logger=quiet_logger())
        flt._table_ready = True
        result = asyncio.run(flt.release_many(["a", "b"]))
        self.assertEqual(result, [True, True])

    def test_validation(self):
        with self.assertRaises(ValueError):
            AsyncOracleSQLFilter(
                FakeOracleAsyncConnection(), "dup", mode="fast",
                logger=quiet_logger())
        with self.assertRaises(ValueError):
            AsyncOracleSQLFilter(
                FakeOracleAsyncConnection(), "bad name",
                logger=quiet_logger())


class OracleSQLFilterTests(unittest.TestCase):
    class FakeOracle:
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
        self.assertEqual(values, [("ha", )])

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


class FakeAsyncCursor:
    def __init__(self, rowcounts):
        self.rowcounts = list(rowcounts)
        self.rowcount = 1
        self.executed = []
        self.create_calls = 0
        self.executemany_calls = 0
        self.executemany_args = None
        self.fail_on = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def execute(self, sql, params=None):
        if sql.lstrip().upper().startswith("CREATE"):
            self.create_calls += 1
            self.rowcount = 0
            return
        if self.fail_on is not None and len(self.executed) + 1 == self.fail_on:
            raise RuntimeError("boom")
        self.executed.append(params)
        self.rowcount = self.rowcounts.pop(0) if self.rowcounts else 1

    async def fetchall(self):
        return []

    async def executemany(self, sql, values):
        self.executemany_calls += 1
        self.executemany_args = (sql, list(values))
        self.rowcount = len(values)


class FakeAsyncConnection:
    def __init__(self, cursor):
        self.cursor_instance = cursor
        self.committed = False
        self.rolled_back = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def cursor(self):
        return self.cursor_instance

    async def commit(self):
        self.committed = True

    async def rollback(self):
        self.rolled_back = True


class FakeAsyncPool:
    def __init__(self, cursor):
        self.connection = FakeAsyncConnection(cursor)

    def acquire(self):
        return self.connection


class AsyncMySQLFilterTests(unittest.TestCase):
    def test_exists_and_insert_many_maps_rowcount_to_stats(self):
        cursor = FakeAsyncCursor([1, 0, 1])
        pool = FakeAsyncPool(cursor)
        flt = AsyncMySQLFilter(pool=pool, table="dup", logger=quiet_logger())
        stats = asyncio.run(flt.exists_and_insert_many(["a", "a", "b"]))
        self.assertEqual(stats, [False, True, False])
        self.assertTrue(pool.connection.committed)
        self.assertEqual(len(cursor.executed), 3)
        self.assertEqual(len(cursor.rowcounts), 0)
        self.assertEqual(cursor.create_calls, 1)

    def test_table_is_created_lazily_once(self):
        cursor = FakeAsyncCursor([1, 1])
        pool = FakeAsyncPool(cursor)
        flt = AsyncMySQLFilter(pool=pool, table="dup", logger=quiet_logger())
        self.assertEqual(cursor.create_calls, 0)
        asyncio.run(flt.exists_and_insert_many(["a", "b"]))
        self.assertEqual(cursor.create_calls, 1)
        asyncio.run(flt.exists_many(["a"]))
        self.assertEqual(cursor.create_calls, 1)

    def test_table_name_is_validated(self):
        pool = FakeAsyncPool(FakeAsyncCursor([]))
        with self.assertRaises(ValueError):
            AsyncMySQLFilter(
                pool=pool, table="dup; DROP TABLE x", logger=quiet_logger())

    def test_mode_is_validated(self):
        pool = FakeAsyncPool(FakeAsyncCursor([]))
        with self.assertRaises(ValueError):
            AsyncMySQLFilter(
                pool=pool, table="dup", mode="fast", logger=quiet_logger())

    def test_batch_mode_uses_snapshot_semantics(self):
        cursor = FakeAsyncCursor([1])
        pool = FakeAsyncPool(cursor)
        flt = AsyncMySQLFilter(
            pool=pool, table="dup", mode="batch", logger=quiet_logger())
        stats = asyncio.run(flt.exists_and_insert_many(["a", "a", "b"]))
        self.assertEqual(stats, [False, False, False])
        self.assertEqual(cursor.create_calls, 1)
        self.assertEqual(len(cursor.executed), 1)  # one SELECT
        self.assertEqual(cursor.executemany_calls, 1)
        rows = cursor.executemany_args[1]
        self.assertEqual(len(rows), 2)  # "a" deduped, so "a" and "b"
        self.assertEqual(
            set(rows), {(utils.md5("a"), ), (utils.md5("b"), )})
        self.assertTrue(pool.connection.committed)

    def test_atomic_mode_rolls_back_on_error(self):
        cursor = FakeAsyncCursor([1, 1, 1])
        cursor.fail_on = 2
        pool = FakeAsyncPool(cursor)
        flt = AsyncMySQLFilter(
            pool=pool, table="dup", logger=quiet_logger())
        stats = asyncio.run(flt.exists_and_insert_many(["a", "b", "c"]))
        self.assertEqual(stats, [False, False, False])
        self.assertTrue(pool.connection.rolled_back)

    def test_async_release_many_deletes_rows(self):
        cursor = FakeAsyncCursor([1])
        pool = FakeAsyncPool(cursor)
        flt = AsyncMySQLFilter(
            pool=pool, table="dup", logger=quiet_logger())
        result = asyncio.run(flt.release_many(["a", "b"]))
        self.assertEqual(result, [True, True])
        self.assertEqual(len(cursor.executed[-1]), 2)

    def test_async_two_phase_aliases_are_awaitable(self):
        cursor = FakeAsyncCursor([1, 1])
        pool = FakeAsyncPool(cursor)
        flt = AsyncMySQLFilter(
            pool=pool, table="dup", logger=quiet_logger())
        self.assertFalse(asyncio.run(flt.exists_and_lock("a")))
        self.assertTrue(asyncio.run(flt.confirm("a")))


@unittest.skipUnless(aiosqlite is not None, "aiosqlite not installed")
class AsyncSQLiteFilterTests(unittest.TestCase):
    def _run(self, coro):
        return asyncio.run(coro)

    def test_atomic_roundtrip(self):
        async def case():
            conn = await aiosqlite.connect(":memory:")
            flt = None
            try:
                flt = AsyncSQLiteFilter(conn, "dup", logger=quiet_logger())
                self.assertEqual(
                    await flt.exists_and_insert_many(["a", "a", "b"]),
                    [False, True, False])
                self.assertEqual(
                    await flt.exists_and_insert_many(["a", "b", "c"]),
                    [True, True, False])
            finally:
                if flt is not None:
                    await flt.close()

        self._run(case())

    def test_batch_snapshot_semantics(self):
        async def case():
            conn = await aiosqlite.connect(":memory:")
            flt = None
            try:
                flt = AsyncSQLiteFilter(
                    conn, "dup", mode="batch", logger=quiet_logger())
                self.assertEqual(
                    await flt.exists_and_insert_many(["a", "a", "b"]),
                    [False, False, False])
            finally:
                if flt is not None:
                    await flt.close()

        self._run(case())

    def test_two_phase(self):
        async def case():
            conn = await aiosqlite.connect(":memory:")
            flt = None
            try:
                flt = AsyncSQLiteFilter(conn, "dup", logger=quiet_logger())
                self.assertFalse(await flt.exists_and_lock("a"))
                self.assertTrue(await flt.exists_and_lock("a"))
                self.assertTrue(await flt.confirm("a"))
                self.assertTrue(await flt.release("a"))
                self.assertFalse(await flt.exists("a"))
            finally:
                if flt is not None:
                    await flt.close()

        self._run(case())
