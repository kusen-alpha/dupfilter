# -*- coding:utf-8 -*-
# author: kusen
# email: 1194542196@qq.com
# date: 2023/12/8

from __future__ import annotations

import re
import time
from collections.abc import Iterable
from typing import Any

from dupfilter.filters import Filter, decorate_warning

_TABLE_NAME_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')


class SQLFilter(Filter):

    def __init__(self, connection: Any, table: str,
                 record_time: bool = False, mode: str = "atomic",
                 auto_create_table: bool = True,
                 *args: Any, **kwargs: Any) -> None:
        """
        需建立一张包含id、insert_time（可选，结合record_time逻辑）的表，id为主键
        :param connection:
        :param table:
        :param record_time: 是否记录插入时间
        :param args:
        :param kwargs:
        """
        if mode not in ("atomic", "batch"):
            raise ValueError("mode must be 'atomic' or 'batch'")
        if not self._valid_table_name(table):
            raise ValueError(f'invalid table name: {table!r}')
        self.table = table
        self.connection = connection
        self.cursor = self.connection.cursor()
        self.record_time = record_time
        # "batch" trades atomicity for speed: one SELECT plus one
        # batched INSERT (2 round trips). It only reports the state
        # before the call, so a value repeated inside the same batch
        # is not reported as a duplicate, and concurrent writers can
        # both see "new". Use it only from a single worker with
        # unique values per call. "atomic" (default) inserts row by
        # row and reports per-position duplicates safely under
        # concurrency.
        self.mode = mode
        super().__init__(*args, **kwargs)
        if auto_create_table:
            self._create_table()

    @staticmethod
    def _valid_table_name(name: str) -> bool:
        return bool(_TABLE_NAME_RE.match(name))

    def _create_table_sql(self) -> str:
        if not self.record_time:
            sql = f"""
            CREATE TABLE IF NOT EXISTS {self.table} (
                id VARCHAR(32) NOT NULL,
                PRIMARY KEY (id)
            )
            """
        else:
            sql = f"""
            CREATE TABLE IF NOT EXISTS {self.table} (
                id VARCHAR(32) NOT NULL,
                insert_time INT ,
                PRIMARY KEY (id)
            )
                """
        return sql

    def _exists_table(self) -> bool:
        return False

    def _placeholder_many(self, count: int) -> str:
        return ", ".join(["%s"] * count)

    def _create_table(self) -> None:
        if self._exists_table():
            return
        sql = self._create_table_sql()
        try:
            self.cursor.execute(sql)
            self.connection.commit()
            # self.logger.info("创建去重表%s成功" % self.table)
        except Exception:
            self.connection.rollback()
            self.logger.info(f"创建去重表{self.table}失败")

    @decorate_warning
    def exists(self, value: Any) -> Any:
        return self.exists_many([value])[0]

    def _exists_sql(self, values: list[Any]) -> tuple[str, Any, list[Any]]:
        values = [self._value_hash_and_compress(value) for value in values]
        if not values:
            return "", (), values
        placeholders = self._placeholder_many(len(values))
        sql = f"select id from {self.table} where id in ({placeholders})"
        return sql, values, values

    @decorate_warning
    def exists_many(self, values: Iterable[Any]) -> Any:
        values = list(values)
        sql, params, new_values = self._exists_sql(values)
        if not new_values:
            return []
        self.cursor.execute(sql, params)
        result = self.cursor.fetchall()
        try:
            result = set(res[0] for res in result)
        except KeyError:
            result = set(res['id'] for res in result)
        stats = [value in result for value in new_values]
        self._log_exists(values, new_values, stats)
        return stats

    @decorate_warning
    def insert(self, value: Any) -> Any:
        return self.insert_many([value])[0]

    def _insert_sql(self, values: Iterable[Any]) -> tuple[str, list[Any]]:
        values = [self._value_hash_and_compress(value) for value in values]
        if self.record_time:
            sql = f"""INSERT INTO {self.table}
                     (id, insert_time) VALUES (%s, %s)
                     ON DUPLICATE KEY UPDATE id=id"""
            values = [(value, int(time.time())) for value in values]
        else:
            sql = f"""INSERT INTO {self.table} (id) VALUES (%s)
                      ON DUPLICATE KEY UPDATE id=id"""
            values = [(value, ) for value in values]
        return sql, values

    @decorate_warning
    def insert_many(self, values: Iterable[Any]) -> Any:
        try:
            sql, values = self._insert_sql(values)
            self.cursor.executemany(sql, values)
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise
        return [True for _ in values]

    def count(self) -> Any:
        """Current dedup record count, useful as a capacity water level."""
        self.cursor.execute(f"SELECT COUNT(*) FROM {self.table}")
        return int(self.cursor.fetchone()[0])

    @decorate_warning
    def exists_and_insert(self, value: Any) -> Any:
        return self.exists_and_insert_many([value])[0]

    # Two-phase claim/confirm semantics: exists_and_lock claims a value
    # atomically (True means it was already claimed/present, so the
    # caller should skip it). Call confirm() after the work finished,
    # or release() when it failed so a retry can claim it again.
    exists_and_lock = exists_and_insert
    confirm = insert
    confirm_many = insert_many

    @decorate_warning
    def release(self, value: Any) -> Any:
        return self.release_many([value])[0]

    @decorate_warning
    def release_many(self, values: list[Any]) -> Any:
        new_values = [self._value_hash_and_compress(value)
                      for value in values]
        if not new_values:
            return []
        placeholders = self._placeholder_many(len(new_values))
        sql = f"DELETE FROM {self.table} WHERE id IN ({placeholders})"
        try:
            self.cursor.execute(sql, new_values)
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise
        return [True for _ in values]

    def _insert_one(self, value: Any) -> tuple[bool, Any]:
        """Insert a single value and report whether it already existed.

        The rowcount is 0 when the primary key conflicts (the row was
        already present), and 1 when the row was inserted by this call.
        """
        sql, params = self._insert_sql([value])
        new_value = self._value_hash_and_compress(value)
        param = params[0]
        if not isinstance(param, (tuple, list)):
            param = (param,)
        self.cursor.execute(sql, param)
        return self.cursor.rowcount == 0, new_value

    @decorate_warning
    def exists_and_insert_many(self, values: Iterable[Any]) -> Any:
        values = list(values)
        if self.mode == "batch":
            # Snapshot semantics: the judgment reflects the state
            # before this call, so intra-batch repeated values are
            # all reported as new. Fast (2 round trips) but only safe
            # for a single worker with unique values per batch.
            stats = self.exists_many(values)
            seen = set()
            to_insert = []
            for stat, value in zip(stats, values):
                if stat:
                    continue
                new_value = self._value_hash_and_compress(value)
                if new_value in seen:
                    continue
                seen.add(new_value)
                to_insert.append(value)
            if to_insert:
                self.insert_many(to_insert)
            return stats
        stats = []
        try:
            for value in values:
                stat, new_value = self._insert_one(value)
                self._log_exists([value], [new_value], [stat])
                stats.append(stat)
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise
        return stats

    exists_and_lock_many = exists_and_insert_many

    def close(self) -> Any:
        try:
            if not getattr(self, "_closed", False):
                self.cursor.close()
                self.connection.close()
                self._closed = True
            self.logger.info("去重数据库连接关闭成功")
        except Exception as e:
            self.logger.warning(f"去重数据库连接关闭失败：{str(e)}")


if __name__ == '__main__':
    import pymysql

    conn = pymysql.connect(host='127.0.0.1', user='root', password='change_me',
                           database='dupfilter')
    cursor = conn.cursor()

    f = SQLFilter(conn, 'dup', record_time=False)
    r = f.exists_and_insert_many(['1', '2', '5'])
    # r = f.insert_many(['1', '2', '6'])
    print(r)
