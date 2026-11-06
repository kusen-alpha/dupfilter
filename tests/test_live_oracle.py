import asyncio
import os
import unittest

from dupfilter.filters.sql.oracle import AsyncOracleSQLFilter, OracleSQLFilter

from .helpers import quiet_logger

ORACLE_USER = os.environ.get("DUPFILTER_ORACLE_USER", "")
TABLE = "dup_live_test"
TABLE_RT = "dup_live_test_rt"


def _init_client():
    import cx_Oracle

    lib_dir = os.environ.get("DUPFILTER_ORACLE_LIB_DIR")
    if lib_dir:
        try:
            cx_Oracle.init_oracle_client(lib_dir=lib_dir)
        except Exception:
            pass
    return cx_Oracle


@unittest.skipUnless(ORACLE_USER, "set DUPFILTER_ORACLE_USER to run live Oracle tests")
class LiveOracleTests(unittest.TestCase):
    def setUp(self):
        self._cx = _init_client()
        self.connection = self._cx.connect(
            user=ORACLE_USER,
            password=os.environ.get("DUPFILTER_ORACLE_PASSWORD", ""),
            dsn=os.environ.get("DUPFILTER_ORACLE_DSN", ""),
        )
        self._drop_table()
        self._drop_named(TABLE_RT)
        self.flt = OracleSQLFilter(
            self.connection, TABLE, logger=quiet_logger())

    def tearDown(self):
        self._drop_table()
        self._drop_named(TABLE_RT)
        self.connection.close()

    def _drop_named(self, table):
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"DROP TABLE {table}")
            self.connection.commit()
            cursor.close()
        except Exception:
            self.connection.rollback()

    def _drop_table(self):
        self._drop_named(TABLE)

    def test_atomic_roundtrip(self):
        self.assertEqual(
            self.flt.exists_and_insert_many(["1", "2", "3"]),
            [False, False, False])
        self.assertEqual(
            self.flt.exists_and_insert_many(["1", "2", "4"]),
            [True, True, False])

    def test_batch_snapshot_semantics(self):
        flt = OracleSQLFilter(
            self.connection, TABLE, mode="batch", logger=quiet_logger())
        self.assertEqual(
            flt.exists_and_insert_many(["a", "a", "b"]),
            [False, False, False])
        self.assertEqual(
            flt.exists_and_insert_many(["a", "b", "c"]),
            [True, True, False])

    def test_two_phase_claim_release(self):
        self.assertFalse(self.flt.exists_and_lock("a"))
        self.assertTrue(self.flt.exists_and_lock("a"))
        self.assertTrue(self.flt.confirm("a"))
        self.assertTrue(self.flt.release("a"))
        self.assertFalse(self.flt.exists("a"))

    def test_record_time_variant(self):
        flt = OracleSQLFilter(
            self.connection, TABLE_RT, record_time=True,
            logger=quiet_logger())
        self.assertEqual(
            flt.exists_and_insert_many(["x", "y"]),
            [False, False])
        self.assertEqual(
            flt.exists_and_insert_many(["x", "z"]),
            [True, False])


@unittest.skipUnless(ORACLE_USER, "set DUPFILTER_ORACLE_USER to run live Oracle tests")
class LiveAsyncOracleTests(unittest.TestCase):
    def setUp(self):
        import oracledb

        self._oracledb = oracledb
        self._dsn = {
            "user": ORACLE_USER,
            "password": os.environ.get("DUPFILTER_ORACLE_PASSWORD", ""),
            "dsn": os.environ.get("DUPFILTER_ORACLE_DSN", ""),
        }

    def _run(self, coro):
        return asyncio.run(coro)

    async def _drop(self, conn, table):
        cursor = conn.cursor()
        await cursor.execute(
            f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {table}'; "
            "EXCEPTION WHEN OTHERS THEN NULL; END;")
        await conn.commit()
        cursor.close()

    def test_async_atomic_roundtrip(self):
        async def case():
            conn = await self._oracledb.connect_async(**self._dsn)
            await self._drop(conn, "dup_async_live")
            flt = AsyncOracleSQLFilter(
                conn, "dup_async_live", logger=quiet_logger())
            self.assertEqual(
                await flt.exists_and_insert_many(["1", "2", "3"]),
                [False, False, False])
            self.assertEqual(
                await flt.exists_and_insert_many(["1", "2", "4"]),
                [True, True, False])
            await self._drop(conn, "dup_async_live")
            await flt.close()

        self._run(case())

    def test_async_batch_snapshot_semantics(self):
        async def case():
            conn = await self._oracledb.connect_async(**self._dsn)
            await self._drop(conn, "dup_async_live")
            flt = AsyncOracleSQLFilter(
                conn, "dup_async_live", mode="batch", logger=quiet_logger())
            self.assertEqual(
                await flt.exists_and_insert_many(["a", "a", "b"]),
                [False, False, False])
            self.assertEqual(
                await flt.exists_and_insert_many(["a", "b", "c"]),
                [True, True, False])
            await self._drop(conn, "dup_async_live")
            await flt.close()

        self._run(case())

    def test_async_two_phase(self):
        async def case():
            conn = await self._oracledb.connect_async(**self._dsn)
            await self._drop(conn, "dup_async_live")
            flt = AsyncOracleSQLFilter(
                conn, "dup_async_live", logger=quiet_logger())
            self.assertFalse(await flt.exists_and_lock("a"))
            self.assertTrue(await flt.exists_and_lock("a"))
            self.assertTrue(await flt.confirm("a"))
            self.assertTrue(await flt.release("a"))
            self.assertFalse(await flt.exists("a"))
            await self._drop(conn, "dup_async_live")
            await flt.close()

        self._run(case())
