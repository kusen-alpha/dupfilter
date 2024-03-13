# -*- coding:utf-8 -*-
# author: kusen
# email: 1194542196@qq.com
# date: 2024/3/13


import time

from dupfilter.filters.sql import SQLFilter


class SQLiteFilter(SQLFilter):
    def _insert_sql(self, values):
        values = [self._value_hash_and_compress(value) for value in values]
        if self.record_time:
            sql = f"""INSERT INTO {self.table}
                             (id, insert_time) VALUES (?, ?)"""
            values = [(value, int(time.time())) for value in values]
        else:
            sql = f"INSERT INTO {self.table} (id) VALUES (?)"
            values = [(value, ) for value in values]
        return sql, values
