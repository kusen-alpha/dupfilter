# -*- coding:utf-8 -*-
# author: kusen
# email: 1194542196@qq.com
# date: 2024/3/12


from dupfilter.filters import Filter, decorate_warning
from dupfilter.filters.sql import SQLFilter


class MySQLFilter(SQLFilter):
    pass


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
                try:
                    await cur.execute(sql)
                    await conn.commit()
                    self.logger.info("创建去重表%s成功" % self.table)
                except Exception as e:
                    self.logger.info("创建去重表%s已经创建或创建失败" % self.table)
                    return

    @decorate_warning
    async def exists(self, value):
        result = await self.exists_many([value])
        return result[0]

    @decorate_warning
    async def exists_many(self, values):
        sql, new_values = self._exists_sql(values)
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql)
                result = await cur.fetchall()
                result = [res[0] for res in result]
                stats = [value in result for value in new_values]
                self._log_exists(values, new_values, stats)
                return stats

    @decorate_warning
    async def insert(self, value):
        result = await self.insert_many([value])
        return result[0]

    @decorate_warning
    async def insert_many(self, values):
        sql, values = self._insert_sql(values)
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(sql, values)
                await conn.commit()
                return [True for _ in values]

    @decorate_warning
    async def exists_and_insert(self, value):
        result = await self.exists_and_insert_many([value])
        return result[0]

    @decorate_warning
    async def exists_and_insert_many(self, values):
        stats = await self.exists_many(values)
        values = [value for stat, value in zip(stats, values) if not stat]
        await self.insert_many(values)
        return stats


if __name__ == '__main__':
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
        f = AsyncMySQLFilter(pool=pool, table="dup", record_time=True)
        await f.create_table()
        res = await f.exists_and_insert_many(['1', '2', '3'])
        print(res)


    loop = asyncio.get_event_loop()
    loop.run_until_complete(test())
