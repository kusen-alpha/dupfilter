# -*- coding:utf-8 -*-
# author: kusen
# email: 1194542196@qq.com
# date: 2024/3/12


import time
from dupfilter.filters.sql import SQLFilter
from dupfilter.filters.sql.mysql import AsyncMySQLFilter


class OracleSQLFilter(SQLFilter):

    def _exists_table(self):
        sql = f"SELECT TABLE_NAME FROM USER_TABLES WHERE TABLE_NAME = '{self.table.upper()}'"
        try:
            self.cursor.execute(sql)
            res = self.cursor.fetchone()
            return bool(res)
        except:
            return False

    def _insert_sql(self, values):
        values = [self._value_hash_and_compress(value) for value in values]
        if self.record_time:
            sql = f"""INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX(
            {self.table}(id)) */ INTO {self.table}
                     (id, insert_time) VALUES (:1,:2)"""
            values = [(value, int(time.time())) for value in values]
        else:
            sql = f"""INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX(
            {self.table} INTO {self.table} (id) VALUES (:1)"""
        return sql, values


class AsyncOracleSQLFilter(OracleSQLFilter, AsyncMySQLFilter):
    def __init__(self, *args, **kwargs):
        AsyncMySQLFilter.__init__(self, *args, **kwargs)


