# -*- coding:utf-8 -*-
# author: kusen
# email: 1194542196@qq.com
# date: 2023/12/8


import time
from dupfilter.filters import Filter, decorate_warning


class SQLFilter(Filter):

    def __init__(self, connection, table,
                 record_time=False, *args, **kwargs):
        """
        需建立一张包含id、insert_time（可选，结合record_time逻辑）的表，id为主键
        :param connection:
        :param table:
        :param record_time: 是否记录插入时间
        :param args:
        :param kwargs:
        """
        self.table = table
        self.connection = connection
        self.cursor = self.connection.cursor()
        self.record_time = record_time
        super(SQLFilter, self).__init__(*args, **kwargs)
        self._create_table()

    def _create_table_sql(self):
        if not self.record_time:
            sql = """  
            CREATE TABLE IF NOT EXISTS %s (  
                id VARCHAR(32) NOT NULL,
                PRIMARY KEY (id)  
            )
            """ % self.table
        else:
            sql = """  
            CREATE TABLE IF NOT EXISTS %s (  
                id VARCHAR(32) NOT NULL,  
                insert_time INT ,
                PRIMARY KEY (id)  
            )
                """ % self.table
        return sql

    def _exists_table(self):
        return False

    def _create_table(self):
        if self._exists_table():
            return
        sql = self._create_table_sql()
        try:
            self.cursor.execute(sql)
            self.connection.commit()
            # self.logger.info("创建去重表%s成功" % self.table)
        except Exception as e:
            self.logger.info("创建去重表%s失败" % self.table)

    @decorate_warning
    def exists(self, value):
        return self.exists_many([value])[0]

    def _exists_sql(self, values):
        values = [self._value_hash_and_compress(value) for value in values]
        s_values = str(tuple(values)) if len(
            values) > 1 else f"('{values[0]}')"
        sql = f"select id from {self.table} where id in {s_values}"
        return sql, values

    @decorate_warning
    def exists_many(self, values):
        sql, new_values = self._exists_sql(values)
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        try:
            result = [res[0] for res in result]
        except KeyError:
            result = [res['id'] for res in result]
        stats = [value in result for value in new_values]
        self._log_exists(values, new_values, stats)
        return stats

    @decorate_warning
    def insert(self, value):
        return self.insert_many([value])[0]

    def _insert_sql(self, values):
        values = [self._value_hash_and_compress(value) for value in values]
        if self.record_time:
            sql = f"""INSERT IGNORE INTO {self.table}
                     (id, insert_time) VALUES (%s, %s)"""
            values = [(value, int(time.time())) for value in values]
        else:
            sql = f"INSERT IGNORE INTO {self.table} (id) VALUES (%s)"
        return sql, values

    @decorate_warning
    def insert_many(self, values):
        sql, values = self._insert_sql(values)
        self.cursor.executemany(sql, values)
        self.connection.commit()
        return [True for _ in values]

    @decorate_warning
    def exists_and_insert(self, value):
        return self.exists_and_insert_many([value])[0]

    @decorate_warning
    def exists_and_insert_many(self, values):
        stats = self.exists_many(values)
        values = [value for stat, value in zip(stats, values) if not stat]
        self.insert_many(values)
        return stats

    def close(self):
        try:
            self.cursor.close()
            self.connection.close()
            self.logger.info("去重数据库连接关闭成功")
        except Exception as e:
            self.logger.warning("去重数据库连接关闭失败：%s" % str(e))


if __name__ == '__main__':
    import pymysql

    conn = pymysql.connect(host='192.168.1.10', user='root', password='123456',
                           database='test')
    cursor = conn.cursor()

    f = SQLFilter(conn, cursor, 'dup', record_time=False)
    r = f.exists_and_insert_many(['1', '2', '5'])
    # r = f.insert_many(['1', '2', '6'])
    print(r)
