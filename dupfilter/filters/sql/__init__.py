# -*- coding:utf-8 -*-
# author: kusen
# email: 1194542196@qq.com
# date: 2023/12/8

import time

from dupfilter.filters import Filter, ResetFilter


class SQLFilter(Filter):

    def __init__(self, connection, cursor, table,
                 record_time=False, *args, **kwargs):
        """
        需建立一张包含id、insert_time（可选，结合record_time逻辑）的表，id为主键
        :param connection:
        :param cursor:
        :param table:
        :param record_time: 是否记录插入时间
        :param args:
        :param kwargs:
        """
        self.cursor = cursor
        self.table = table
        self.connection = connection
        self.record_time = record_time
        super(SQLFilter, self).__init__(*args, **kwargs)

    def exists(self, value):
        return self.exists_many([value])[0]

    def exists_many(self, values):
        values = [self._value_hash_and_compress(value) for value in values]
        s_values = str(tuple(values)) if len(values) > 1 else f"('{values[0]}')"
        sql = f"select id from {self.table} where id in {s_values}"
        self.cursor.execute(sql)
        result = [res[0] for res in cursor.fetchall()]
        return [value in result for value in values]

    def insert(self, value):
        return self.insert_many([value])[0]

    def insert_many(self, values):
        values = [self._value_hash_and_compress(value) for value in values]
        if self.record_time:
            sql = f"INSERT IGNORE INTO {self.table} (id, insert_time) VALUES (%s, %s)"
            values = [(value, int(time.time())) for value in values]
        else:
            sql = f"INSERT IGNORE INTO {self.table} (id) VALUES (%s)"
        self.cursor.executemany(sql, values)
        self.connection.commit()
        return [True for _ in values]

    def exists_and_insert(self, value):
        return self.exists_and_insert_many([value])[0]

    def exists_and_insert_many(self, values):
        stats = self.exists_many(values)
        self.insert_many(values)
        return stats


# class SQLResetFilter(ResetFilter):
#     def __init__(self, cursor, *args, **kwargs):
#         self.cursor = cursor
#         super(SQLResetFilter, self).__init__(*args, **kwargs)
#

if __name__ == '__main__':
    import pymysql

    conn = pymysql.connect(host='192.168.1.10', user='root', password='123456',
                           database='test')
    cursor = conn.cursor()

    f = SQLFilter(conn, cursor, 'dup', record_time=False)
    r = f.exists_and_insert_many(['1', '2'])
    print(r)
