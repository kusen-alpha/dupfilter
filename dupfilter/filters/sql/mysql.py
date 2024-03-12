# -*- coding:utf-8 -*-
# author: kusen
# email: 1194542196@qq.com
# date: 2024/3/12

from dupfilter.filters import Filter
from dupfilter.filters.sql import SQLFilter


class AsyncMySQLFilter(SQLFilter, Filter):
    def __init__(self, pool, table,
                 record_time=False, *args, **kwargs):
        self.pool = pool
        self.table = table
        self.record_time = record_time
        Filter.__init__(self, *args, **kwargs)

    async def create_table(self):
        sql = self._create_table_sql()
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql)
                await conn.commit()

    async def exists(self, value):
        result = await self.exists_many([value])
        return result[0]

    async def exists_many(self, values):
        sql, values = self._exists_sql(values)
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql)
                result = await cur.fetchall()
                result = [res[0] for res in result]
                return [value in result for value in values]

    async def insert(self, value):
        result = await self.insert_many([value])
        return result[0]

    async def insert_many(self, values):
        sql, values = self._insert_sql(values)
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(sql, values)
                await conn.commit()
                return [True for _ in values]

    async def exists_and_insert(self, value):
        result = await self.exists_and_insert_many([value])
        return result[0]

    async def exists_and_insert_many(self, values):
        stats = await self.exists_many(values)
        values = [value for stat, value in zip(stats, values) if not stat]
        await self.insert_many(values)
        return stats


if __name__ == '__main__':
    import pymysql

    # conn = pymysql.connect(host='192.168.1.10', user='root', password='123456',
    #                        database='test')
    # cursor = conn.cursor()
    #
    # f = SQLFilter(conn, cursor, 'dup', record_time=False)
    # r = f.exists_and_insert_many(['1', '2', '5'])
    # # r = f.insert_many(['1', '2', '6'])
    # print(r)

    import aiomysql
    import asyncio


    async def test():
        configs = {
            'minsize': 1,
            'maxsize': 10,
            'host': '192.168.1.10',
            'user': 'root',
            'password': '123456',
            'port': 3306,
            'db': 'test'
        }
        pool = await aiomysql.create_pool(**configs)
        f = AsyncSQLFilter(pool=pool, table="dup", record_time=True)
        await f.create_table()
        res = await f.exists_and_insert_many(['1', '2', '3'])
        print(res)


    loop = asyncio.get_event_loop()
    loop.run_until_complete(test())
