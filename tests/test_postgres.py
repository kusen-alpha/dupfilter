# -*-coding:utf-8 -*-
import asyncio
import unittest

from dupfilter import utils
from dupfilter.filters.sql.postgres import (
    AsyncPostgresFilter,
    PostgresFilter,
)

from .helpers import quiet_logger


class FakeSyncCursor:
    def __init__(self, exists_row=None):
        self.executed = []
        self.fetchone_result = exists_row

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def executemany(self, sql, values):
        self.executed.append((sql, values))

    def fetchone(self):
        return self.fetchone_result

    def fetchall(self):
        return []

    def close(self):
        pass


class FakeSyncConnection:
    def __init__(self, exists_row=None):
        self.cursors = []
        self.exists_row = exists_row

    def cursor(self):
        cursor = FakeSyncCursor(self.exists_row)
        self.cursors.append(cursor)
        return cursor

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass


class PostgresFilterTests(unittest.TestCase):
    def test_insert_sql_uses_on_conflict(self):
        conn = FakeSyncConnection()
        flt = PostgresFilter(conn, "dup", logger=quiet_logger())
        sql, _ = flt._insert_sql(["a"])
        self.assertIn("ON CONFLICT (id) DO NOTHING", sql)
        self.assertNotIn("OR IGNORE", sql)

    def test_record_time_insert_sql(self):
        conn = FakeSyncConnection()
        flt = PostgresFilter(
            conn, "dup", record_time=True, logger=quiet_logger())
        sql, values = flt._insert_sql(["a"])
        self.assertIn("insert_time", sql)
        self.assertEqual(len(values[0]), 2)

    def test_placeholder_many(self):
        self.assertEqual(
            PostgresFilter._placeholder_many(None, 3), "%s, %s, %s")

    def test_exists_table_queries_information_schema(self):
        conn = FakeSyncConnection(exists_row=("dup",))
        flt = PostgresFilter(conn, "dup", logger=quiet_logger())
        self.assertTrue(flt._exists_table())
        sql = conn.cursors[0].executed[0][0]
        self.assertIn("information_schema.tables", sql)

    def test_create_table_runs_when_missing(self):
        conn = FakeSyncConnection(exists_row=None)
        PostgresFilter(conn, "dup", logger=quiet_logger())
        create_sqls = [sql for sql, _ in conn.cursors[0].executed
                       if "CREATE TABLE" in sql]
        self.assertEqual(len(create_sqls), 1)
        self.assertIn("CREATE TABLE IF NOT EXISTS", create_sqls[0])

    def test_table_name_is_validated(self):
        conn = FakeSyncConnection()
        with self.assertRaises(ValueError):
            PostgresFilter(conn, "dup; DROP TABLE x", logger=quiet_logger())


class FakeAsyncPGConnection:
    """Behavioral asyncpg stand-in: keeps a store of inserted ids."""

    def __init__(self):
        self.store = set()
        self.created = False
        self.queries = []

    async def fetchval(self, sql, *args):
        self.queries.append(("fetchval", sql, args))
        if "information_schema" in sql:
            return 1 if self.created else 0
        if "COUNT" in sql:
            return len(self.store)
        return None

    async def execute(self, sql, *args):
        self.queries.append(("execute", sql, args))
        if "CREATE TABLE" in sql:
            self.created = True
        elif "DELETE" in sql and args and isinstance(args[0], list):
            for value in args[0]:
                self.store.discard(value)
        return "OK"

    async def fetch(self, sql, *args):
        self.queries.append(("fetch", sql, args))
        ids = args[0] if args else []
        return [{"id": value} for value in ids if value in self.store]

    async def fetchrow(self, sql, *args):
        self.queries.append(("fetchrow", sql, args))
        value = args[0]
        if value in self.store:
            return None
        self.store.add(value)
        return {"id": value}

    async def executemany(self, sql, rows):
        self.queries.append(("executemany", sql, rows))
        for row in rows:
            self.store.add(row[0])

    async def close(self):
        pass


class AsyncPostgresFilterTests(unittest.TestCase):
    def test_atomic_semantics(self):
        conn = FakeAsyncPGConnection()
        flt = AsyncPostgresFilter(conn, "dup", logger=quiet_logger())
        flt._table_ready = True
        stats = asyncio.run(flt.exists_and_insert_many(["a", "a", "b"]))
        self.assertEqual(stats, [False, True, False])

    def test_batch_snapshot_semantics(self):
        conn = FakeAsyncPGConnection()
        flt = AsyncPostgresFilter(
            conn, "dup", mode="batch", logger=quiet_logger())
        stats = asyncio.run(flt.exists_and_insert_many(["a", "a", "b"]))
        self.assertEqual(stats, [False, False, False])
        self.assertEqual(conn.store, {utils.md5("a"), utils.md5("b")})

    def test_roundtrip(self):
        conn = FakeAsyncPGConnection()
        flt = AsyncPostgresFilter(conn, "dup", logger=quiet_logger())

        async def run():
            self.assertFalse(await flt.exists_and_insert("a"))
            self.assertTrue(await flt.exists("a"))
            self.assertEqual(await flt.count(), 1)
            self.assertTrue(await flt.release("a"))
            self.assertFalse(await flt.exists("a"))
            self.assertEqual(await flt.count(), 0)

        asyncio.run(run())

    def test_two_phase(self):
        conn = FakeAsyncPGConnection()
        flt = AsyncPostgresFilter(conn, "dup", logger=quiet_logger())

        async def run():
            self.assertFalse(await flt.exists_and_lock("x"))
            self.assertTrue(await flt.exists_and_lock("x"))
            self.assertTrue(await flt.confirm("x"))
            self.assertTrue(await flt.exists("x"))
            self.assertTrue(await flt.release("x"))
            self.assertFalse(await flt.exists("x"))

        asyncio.run(run())

    def test_validation(self):
        conn = FakeAsyncPGConnection()
        with self.assertRaises(ValueError):
            AsyncPostgresFilter(conn, "dup", mode="fast", logger=quiet_logger())
        with self.assertRaises(ValueError):
            AsyncPostgresFilter(conn, "bad name", logger=quiet_logger())


if __name__ == "__main__":
    unittest.main()
