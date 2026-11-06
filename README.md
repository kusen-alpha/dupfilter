# 简介

去重过滤器,提供常见的去重方案，开发便捷、性能极高。

# 安装

```bash
pip install dupfilter
```

# 去重方案

<table style="text-align: center">
    <tr>
        <th>种类</th>
        <th>去重方案</th>
        <th>说明</th>
        <th>特点</th>
        <th>缺点</th>
        <th>置出方案</th>
    </tr>
    <tr >
        <td >Memory</td>
        <td>MemoryFilter</td>
        <td>基于内存集合类型实现</td>
        <td>准确性高</td>
        <td>不能持久化 </td>
        <td>随机删除 </td>
    </tr>
    <tr>
        <td>Memory</td>
        <td>MemoryBloomFilter<br>AsyncMemoryBloomFilter</td>
        <td>本地内存布隆过滤器，无第三方依赖</td>
        <td>占用内存极小，可按期望容量与误判率自动推算参数</td>
        <td>有误判且不能删除元素</td>
        <td>全量清空</td>
    </tr>
    <tr>
        <td>Memory</td>
        <td>ScalableBloomFilter<br>AsyncScalableBloomFilter</td>
        <td>多层可扩容布隆过滤器</td>
        <td>容量不够时自动追加新层，无需重建</td>
        <td>误判率随层数累积</td>
        <td>全量清空</td>
    </tr>
    <tr>
        <td>Memory</td>
        <td>CuckooFilter<br>AsyncCuckooFilter</td>
        <td>布谷鸟过滤器，按指纹存储</td>
        <td>支持删除/释放元素</td>
        <td>有误判，容量满时需清空或增大容量</td>
        <td>按指纹删除</td>
    </tr>
    <tr>
        <td>File</td>
        <td>FileFiler</td>
        <td>基于文件+集合类型实现</td>
        <td>准确性高</td>
        <td>本地内存和存储占用大</td>
        <td>利用文件指针区间删除</td>
    </tr>
    <tr>
        <td>File</td>
        <td>DiskKVFilter<br>AsyncDiskKVFilter</td>
        <td>基于 SQLite 落盘的键值去重，自管理连接</td>
        <td>数据量可远超内存，重启不丢</td>
        <td>依赖本机 SQLite 文件</td>
        <td>按记录删除</td>
    </tr>
    <tr>
        <td rowspan="5">Redis</td>
        <td>RedisBloomFilter<br>AsyncRedisBloomFilter</td>
        <td>基于Redis Bitmap和布隆过滤器算法实现</td>
        <td>占用内存极小</td>
        <td>有误判的情况且不容易删除元素</td>
        <td>随机删除</td>
    </tr>
    <tr>
        <td>RedisBloomNativeFilter<br>AsyncRedisBloomNativeFilter</td>
        <td>基于 RedisBloom 服务端模块的原生 BF.* 指令实现</td>
        <td>服务端自动扩容，可用 BF.INFO 查容量与水位</td>
        <td>需要 Redis 加载 RedisBloom 模块，有误判且不能删除</td>
        <td>服务端自动扩容</td>
    </tr>
    <tr>
        <td>RedisStringFilter<br>AsyncRedisStringFilter</td>
        <td>基于Redis String数据结构实现</td>
        <td>不会误判，能基于过期时间实现查询去重和确认机制</td>
        <td>占用资源很大，需尽可能压缩和设置过期时间</td>
        <td>设置过期时间</td>
    </tr>
    <tr>
        <td>RedisSetFilter<br>AsyncRedisSetFilter</td>
        <td>基于Redis Set数据结构实现</td>
        <td>准确性高</td>
        <td>占用资源较大</td>
        <td>随机删除</td>
    </tr>
    <tr>
        <td>RedisSortedSetFilter<br>AsyncRedisSortedSetFilter</td>
        <td>基于Redis SortedSet数据结构实现</td>
        <td>准确性高</td>
        <td>占用资源较大</td>
        <td>根据分值删除</td>
    </tr>
    <tr >
        <td >SQL</td>
        <td>SQLFilter</td>
        <td>基于SQL关系数据库表主键来实现</td>
        <td>准确性高</td>
        <td>在大规模去重场景性能差 </td>
        <td>按时间删除 </td>
    </tr>
    <tr>
        <td>SQL</td>
        <td>PostgresFilter<br>AsyncPostgresFilter</td>
        <td>基于 PostgreSQL 唯一主键 + ON CONFLICT DO NOTHING</td>
        <td>准确性高，原子模式用 RETURNING 判定冲突</td>
        <td>在大规模去重场景性能差</td>
        <td>按记录删除</td>
    </tr>
</table>

# 项目特点

1. 多种方案提供不同场景需求。
2. 基于Lua脚本支持批量操作，速度快。
3. 支持异步，可快速集成到异步代码和异步框架中。
4. Reset定时逻辑基于time.monotonic实现，无运行时三方依赖。
5. SQL 去重支持 `atomic` / `batch` 双模式，兼顾并发安全与批量速度。
6. 布隆过滤器可按期望容量和误判率自动推算参数。
7. 公共 API 带类型标注，CI 覆盖 ruff / mypy 与 Redis、MySQL 真后端测试。

# 去重示例

## RedisBloomFilter

```python
import redis
from dupfilter import RedisBloomFilter

server = redis.Redis(host="127.0.0.1", port=6379)
rbf = RedisBloomFilter(server=server, key="bf", block_num=2)
print(rbf.exists_many(["1", "2", "3"]))
rbf.insert_many(["1", "2", "3"])
print(rbf.exists_many(["1", "2", "3"]))
```

## AsyncRedisBloomFilter

```python
import asyncio
import aioredis
from dupfilter import AsyncRedisBloomFilter


async def test():
    server = aioredis.from_url('redis://127.0.0.1:6379/0')
    arbf = AsyncRedisBloomFilter(server, key='bf')
    stats = await arbf.exists_many(["1", "2", "3"])
    print(stats)
    await arbf.insert_many(["1", "2", "3"])
    stats = await arbf.exists_many(["1", "2", "3"])
    print(stats)


loop = asyncio.get_event_loop()
loop.run_until_complete(test())
```

## DefaultFilter

在项目中，可能在外层参数确认是否走去重逻辑，这时为了方法的逻辑一致性，预留默认去重类。

```python
from dupfilter import MemoryFilter
from dupfilter import DefaultFilter

is_dup = True  # 全局设置是否去重
if is_dup:
    flr = MemoryFilter()
else:
    flr = DefaultFilter(default_stat=False)

print(flr.exists("1"))
```

## FilterCounter

对去重结果进行统计判断：

```python
from dupfilter import MemoryFilter
from dupfilter import FilterCounter
flt = MemoryFilter()
flt_counter = FilterCounter()
values = ['1', '2', '3']
for value in values:
    flt_counter.insert_stat(flt.exists(value))

# 进行判断和统计
print(flt_counter.any(), flt_counter.all(), flt_counter.count())
```

## SQL mode 参数

SQL 去重的 `exists_and_insert_many` 支持两种模式：

- `mode="atomic"`（默认）：逐条先插后判，批内重复正确判为已存在，多 worker 并发下原子成立。
- `mode="batch"`：一次查询 + 一次批量插入（2 次往返），速度快，但只反映调用前的状态，批内重复会全部判为新值；仅适合单 worker 且每批值唯一的场景。

```python
from dupfilter import SQLiteFilter

flt = SQLiteFilter(conn, "dup", mode="batch")
print(flt.exists_and_insert_many(["1", "2", "3"]))
```

## 布隆自动参数

```python
from dupfilter import RedisBloomFilter

rbf = RedisBloomFilter(
    server, "bf",
    expected_items=1000000, false_positive_rate=0.01)
```

按期望容量和误判率自动推算位宽与哈希函数数量，也可继续手动传 `bit` / `hash_num`。

## SQL 两阶段（失败可重试）

处理失败时先 `release()`，重试即可重新占用：

```python
from dupfilter import SQLiteFilter

flt = SQLiteFilter(conn, "dup")
if not flt.exists_and_lock("order_123"):
    try:
        handle_order("order_123")
        flt.confirm("order_123")
    except Exception:
        flt.release("order_123")  # 释放占用，允许重试
```

## 连接池

同步 SQL 后端默认单连接；多线程共享时可使用连接池：

```python
import sqlite3
from dupfilter import ConnectionPool, PooledSQLFilter
from dupfilter.filters.sql.sqlite import SQLiteFilter

# SQLite 连接默认绑定创建线程，池化跨线程使用需关闭该检查
pool = ConnectionPool(
    connect=lambda: sqlite3.connect("dup.db", check_same_thread=False),
    size=5)
flt = PooledSQLFilter(pool, "dup", filter_cls=SQLiteFilter)
flt.exists_and_insert_many(["1", "2", "3"])
flt.close()
```

## 重试与熔断

```python
from dupfilter import CircuitBreaker, MemoryFilter, RetryingFilter

flt = MemoryFilter()
wrapped = RetryingFilter(
    flt,
    attempts=3,
    backoff=0.05,          # 初始退避，指数增长
    breaker=CircuitBreaker(failure_threshold=5, reset_timeout=30),
)
wrapped.exists_and_insert("1")   # 瞬时失败自动重试，持续失败触发熔断
```

## 错误策略与观察钩子

默认失败返回 `default_stat` 并记录日志；可全局 `raise_on_error=True`，或按调用临时覆盖；`on_error` 钩子在降级时回调：

```python
from dupfilter import MemoryFilter

flt = MemoryFilter(on_error=lambda exc, fallback: metrics.incr("dup_error"))
flt.exists("a", raise_on_error=True)   # 本次调用直接抛异常
```

## 本地布隆与可扩容布隆

无需 Redis，纯内存布隆过滤器按期望容量与误判率自动推算参数；容量不够时 `ScalableBloomFilter` 自动追加新层：

```python
from dupfilter import MemoryBloomFilter, ScalableBloomFilter

bf = MemoryBloomFilter(expected_items=100000, false_positive_rate=0.01)
print(bf.exists_and_insert("order_123"))     # False，首次出现
print(bf.exists("order_123"))                # True

scalable = ScalableBloomFilter(initial_capacity=10000)
scalable.insert_many([f"item_{i}" for i in range(50000)])  # 自动扩容
```

## Cuckoo 过滤器（可删除）

布隆不能删除元素，`CuckooFilter` 用指纹替代位图，支持 `release` 释放后再次占用：

```python
from dupfilter import CuckooFilter

cf = CuckooFilter(expected_items=100000)
print(cf.exists_and_insert("a"))   # False
print(cf.release("a"))             # True，删除后 a 重新可用
```

## DiskKV 落盘去重

给一个文件路径即可，数据通过 SQLite 索引落盘，容量不受内存限制，重启不丢：

```python
from dupfilter import DiskKVFilter

flt = DiskKVFilter("dup.db")
print(flt.exists_and_insert_many(["1", "2", "3"]))
print(flt.count())                 # 3，可作水位监控
flt.close()
```

## 相似内容去重

`SimHashFilter` 对文本生成 64 位指纹，用 8 分块 LSH 索引近重复候选，再按汉明距离精确校验，结果无误报；`threshold >= 0.89` 时近重复无漏报：

```python
from dupfilter import SimHashFilter, similarity, simhash

flt = SimHashFilter(threshold=0.9)
flt.add("Python 去重库性能极高，开发便捷")
print(flt.exists_similar("Python 去重库性能极高 开发便捷"))   # 相似判重
print(flt.find_similar("Python 去重库性能极高 开发便捷"))

fp1, fp2 = simhash("text a"), simhash("text b")
print(similarity(fp1, fp2))
```

## 基数与频次统计

`HyperLogLog` 估算去重后的基数，`CountMinSketch` 估算元素出现频次，常与去重配套做水位与热点统计：

```python
from dupfilter import CountMinSketch, HyperLogLog

hll = HyperLogLog(precision=14)
for i in range(100000):
    hll.add(f"user_{i}")
print(round(hll.count()))                     # 约 100000

cms = CountMinSketch.from_error_rate(100000)
for _ in range(1000):
    cms.add("hot")
print(cms.estimate("hot"))                    # 约 1000
```

## 带租约的两阶段锁

包装任意支持释放的后端（SQL / Redis Set / SortedSet / String），`exists_and_lock` 自带租约；租约过期未 `confirm` 会自动释放回收，任务失败可重试：

```python
from dupfilter import LeaseFilter, SQLiteFilter
import sqlite3

base = SQLiteFilter(sqlite3.connect("dup.db"), "dup")
flt = LeaseFilter(base, default_lease=300)
if not flt.exists_and_lock("order_123"):
    try:
        handle_order("order_123")
        flt.confirm("order_123")
    except Exception:
        flt.release("order_123")              # 立即释放，或等租约过期
```

## 命中率与水位监控

`StatsFilter` 统计去重判定命中率（重复占比），`count()` 返回各后端当前去重记录数：

```python
from dupfilter import MemoryFilter, StatsFilter

flt = StatsFilter(MemoryFilter())
flt.exists_and_insert_many(["1", "1", "2"])
print(flt.hit_rate, flt.total)                # 0.33, 3
print(flt.count())                            # 2，去重集合大小
```

## 图片相似去重

`ImageHashFilter` 用感知哈希（默认 dHash，也可 aHash）把图片转成 64 位指纹，按 8 分块 LSH 索引、汉明距离精确校验，适合图片库去重：

```python
from dupfilter import ImageHashFilter

flt = ImageHashFilter(method="dhash", threshold=0.9)
flt.add("images/photo_a.png")
print(flt.exists_similar("images/photo_a_copy.png"))   # 轻微修改仍判重
```

需要 Pillow（`pip install Pillow`），未安装时相关类可导入但调用会报错。

## 滑动窗口限频

`SlidingWindowLimiter` 进程内限频，`RedisSlidingWindowLimiter` 用 SortedSet + Lua 实现共享限频，过期记录每次调用自动清理：

```python
from dupfilter import RedisSlidingWindowLimiter

limiter = RedisSlidingWindowLimiter(server, limit=100, window=60)
print(limiter.allow("user_1"))               # 60 秒窗口内最多 100 次
print(limiter.remaining("user_1"))
```

## Others

所有去重类别现在都有同步与异步双形态：`MemoryFilter`/`AsyncMemoryFilter`、
`FileFilter`/`AsyncFileFilter`、`FilePathFilter`/`AsyncFilePathFilter`、
`SQLiteFilter`/`AsyncSQLiteFilter`，加上已有的 Redis 四类与 MySQL/Oracle/Postgres。
异步实现方式按后端特点选择：内存为浅包装，文件/目录用 `asyncio.to_thread`，
SQLite 用 `aiosqlite`，Redis/SQL 用原生异步驱动。

## 异常处理

去重操作失败时默认返回 `default_stat`，避免异常中断业务流程；
如需快速暴露后端故障，可传入 `raise_on_error=True`，异常会直接向上抛出。

```python
from dupfilter import MemoryFilter

flt = MemoryFilter(raise_on_error=True)
```

# 相关库

1. redis：redis/aioredis
2. mysql：pymysql/aiomysql
3. sqlite：sqlite3
4. oracle：cx_Oracle/cx_Oracle_async
5. sqlite 异步：aiosqlite
6. postgres：psycopg2/asyncpg
7. 图片相似去重：Pillow（可选）

# 关于作者

1. 邮箱：1194542196@qq.com
