# 开发与测试指南

本文档面向 dupfilter 的开发者与维护者，包含测试、静态检查、基准与路线图。

## 测试

### 单元测试

```bash
python -m unittest discover -v
```

### Live 集成测试

Live 集成测试需要本地服务，未配置环境变量时自动跳过：

```bash
docker compose up -d
DUPFILTER_REDIS_URL=redis://127.0.0.1:6379/0 python -m unittest tests.test_live_redis -v
DUPFILTER_MYSQL_HOST=127.0.0.1 DUPFILTER_MYSQL_USER=root \
DUPFILTER_MYSQL_PASSWORD=root DUPFILTER_MYSQL_DB=dupfilter \
python -m unittest tests.test_live_mysql -v
DUPFILTER_ORACLE_USER=YOUR_ORACLE_USER DUPFILTER_ORACLE_PASSWORD=YOUR_ORACLE_PASSWORD \
DUPFILTER_ORACLE_DSN=HOST:PORT/SERVICE \
python -m unittest tests.test_live_oracle -v
```

Oracle 异步基于 python-oracledb（thin 模式可免装客户端）：

```python
import asyncio
import oracledb
from dupfilter import AsyncOracleSQLFilter


async def main():
    conn = await oracledb.connect_async(
        user="YOUR_ORACLE_USER", password="YOUR_ORACLE_PASSWORD",
        dsn="HOST:PORT/SERVICE")
    flt = AsyncOracleSQLFilter(conn, "dup")
    stats = await flt.exists_and_insert_many(["1", "2", "3"])
    print(stats)
    await flt.close()


asyncio.run(main())
```

## 静态检查

```bash
pip install ruff mypy
ruff check dupfilter tests
mypy dupfilter
```

## 基准

```bash
python -m benchmarks.bench_sqlite
python -m benchmarks.bench_redis   # 需要 DUPFILTER_REDIS_URL
```

实测（本机，Python 3.8）：

| 后端 | 1 万条耗时 |
| --- | --- |
| memory | 0.09s |
| sqlite atomic / batch | 0.19s |
| redis set（Lua 批量） | 0.22s |
| redis bloom | 1.68s |

布隆误判率实测（离线按哈希偏移统计；位宽取 2 的幂使数组比理论最优最大约 2 倍，
误判率低于目标，属安全方向的保守）：

| expected_items | 目标误判率 | 实测误判率 |
| --- | --- | --- |
| 1000 | 0.01 | 0.0006 |
| 10000 | 0.01 | 0.0018 |
| 100000 | 0.001 | 0.0001 |

多 worker 并发去重校验（8 线程 × 200 次，`dups` 为去重表中唯一记录数，应为 1）：

| 后端 | 耗时 | dups |
| --- | --- | --- |
| file | 0.016s | 1 |
| pooled sqlite | 6.9s | 1 |

## 路线图

- [x] SQL `exists_and_insert_many` 原子化与 `batch`/`atomic` 双模式
- [x] 表名注入防护、真实表存在性检查、异步惰性建表
- [x] SQL 事务回滚与幂等 close
- [x] FileFilter 线程安全、布隆自动参数
- [x] 类型标注、`mypy --strict`、ruff、CI 与 Redis/MySQL live 测试
- [x] 按调用覆盖的降级策略、`on_error` 观察钩子
- [x] SQL 两阶段 `exists_and_lock` / `confirm` / `release`
- [x] 同步 SQL 连接池（`ConnectionPool` / `PooledSQLFilter`）
- [x] Redis Set / SortedSet 两阶段，布隆支持占用/确认
- [x] `RetryingFilter` / `CircuitBreaker` 重试与熔断
- [x] 布隆误判率实测、多 worker 并发基准、异步 MySQL live、覆盖率
- [x] Oracle 真机验证（atomic / batch / 两阶段 / record_time live 测试）
- [x] 本地内存布隆与可扩容布隆（`MemoryBloomFilter` / `ScalableBloomFilter`）
- [x] 支持删除的布谷鸟过滤器（`CuckooFilter`）
- [x] 自管理落盘 KV 去重（`DiskKVFilter`，容量不受内存限制）
- [x] `HyperLogLog` / `CountMinSketch` 基数与频次统计
- [x] `SimHashFilter` 相似文本去重（8 分块 LSH 索引 + 汉明距离校验）
- [x] 带租约的两阶段锁 `LeaseFilter`（过期自动回收）
- [x] 命中率与水位监控（`StatsFilter`、各后端 `count()`）
- [x] PostgreSQL 后端（`PostgresFilter` / `AsyncPostgresFilter`）
- [x] RedisBloom 模块原生指令（`RedisBloomNativeFilter`，BF.RESERVE/ADD/MADD/INFO）
- [x] 图片感知哈希去重（`ImageHashFilter`，dHash/aHash + 分块索引）
- [x] 滑动窗口限频（`SlidingWindowLimiter` / `RedisSlidingWindowLimiter`）
