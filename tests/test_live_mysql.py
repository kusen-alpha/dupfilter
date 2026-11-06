import asyncio
import os
import unittest

from dupfilter.filters.sql.mysql import AsyncMySQLFilter, MySQLFilter

from .helpers import quiet_logger

MYSQL_HOST = os.environ.get("DUPFILTER_MYSQL_HOST", "")


@unittest.skipUnless(MYSQL_HOST, "set DUPFILTER_MYSQL_HOST to run live MySQL tests")
class LiveMySQLTests(unittest.TestCase):
    def setUp(self):
        import pymysql

        self.connection = pymysql.connect(
            host=MYSQL_HOST,
            port=int(os.environ.get("DUPFILTER_MYSQL_PORT", "3306")),
            user=os.environ.get("DUPFILTER_MYSQL_USER", "root"),
            password=os.environ.get("DUPFILTER_MYSQL_PASSWORD", ""),
            database=os.environ.get("DUPFILTER_MYSQL_DB", "dupfilter"),
            autocommit=False,
        )
        cursor = self.connection.cursor()
        cursor.execute("DROP TABLE IF EXISTS dup_live_test")
        self.connection.commit()
        cursor.close()
        self.flt = MySQLFilter(
            self.connection, "dup_live_test", logger=quiet_logger())

    def tearDown(self):
        cursor = self.connection.cursor()
        cursor.execute("DROP TABLE IF EXISTS dup_live_test")
        self.connection.commit()
        cursor.close()
        self.connection.close()

    def test_atomic_mode_roundtrip(self):
        self.assertEqual(
            self.flt.exists_and_insert_many(["1", "2", "3"]),
            [False, False, False])
        self.assertEqual(
            self.flt.exists_and_insert_many(["1", "2", "4"]),
            [True, True, False])

    def test_batch_mode_snapshot_semantics(self):
        flt = MySQLFilter(
            self.connection, "dup_live_test",
            mode="batch", logger=quiet_logger())
        self.assertEqual(
            flt.exists_and_insert_many(["a", "a", "b"]),
            [False, False, False])
        self.assertEqual(
            flt.exists_and_insert_many(["a", "b", "c"]),
            [True, True, False])


@unittest.skipUnless(MYSQL_HOST, "set DUPFILTER_MYSQL_HOST to run live MySQL tests")
class LiveAsyncMySQLTests(unittest.TestCase):
    def setUp(self):
        import aiomysql

        self._aiomysql = aiomysql
        self.pool = None

    def tearDown(self):
        if self.pool is not None:
            self.pool.close()

    async def _make(self, mode="atomic"):
        pool = await self._aiomysql.create_pool(
            host=MYSQL_HOST,
            port=int(os.environ.get("DUPFILTER_MYSQL_PORT", "3306")),
            user=os.environ.get("DUPFILTER_MYSQL_USER", "root"),
            password=os.environ.get("DUPFILTER_MYSQL_PASSWORD", ""),
            db=os.environ.get("DUPFILTER_MYSQL_DB", "dupfilter"),
            minsize=1,
            maxsize=4,
        )
        self.pool = pool
        flt = AsyncMySQLFilter(
            pool=pool, table="dup_live_test_async", mode=mode,
            logger=quiet_logger())
        await flt.create_table()
        return flt

    async def _drop(self):
        import pymysql

        conn = pymysql.connect(
            host=MYSQL_HOST,
            port=int(os.environ.get("DUPFILTER_MYSQL_PORT", "3306")),
            user=os.environ.get("DUPFILTER_MYSQL_USER", "root"),
            password=os.environ.get("DUPFILTER_MYSQL_PASSWORD", ""),
            database=os.environ.get("DUPFILTER_MYSQL_DB", "dupfilter"),
        )
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS dup_live_test_async")
        conn.commit()
        cursor.close()
        conn.close()

    def test_async_atomic_roundtrip(self):
        async def case():
            flt = await self._make()
            self.assertEqual(
                await flt.exists_and_insert_many(["1", "2", "3"]),
                [False, False, False])
            self.assertEqual(
                await flt.exists_and_insert_many(["1", "2", "4"]),
                [True, True, False])

        asyncio.run(case())

    def test_async_batch_snapshot_semantics(self):
        async def case():
            flt = await self._make(mode="batch")
            self.assertEqual(
                await flt.exists_and_insert_many(["a", "a", "b"]),
                [False, False, False])

        asyncio.run(case())
        asyncio.run(self._drop())
