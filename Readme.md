# 简介

去重过滤器,提供常见的去重方案，开发便捷、性能极高。

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
        <td>File</td>
        <td>FileFiler</td>
        <td>基于文件+集合类型实现</td>
        <td>准确性高</td>
        <td>本地内存和存储占用大</td>
        <td>利用文件指针区间删除</td>
    </tr>
    <tr>
        <td rowspan="4">Redis</td>
        <td>RedisBloomFilter<br>AsyncRedisBloomFilter</td>
        <td>基于Redis Bitmap和布隆过滤器算法实现</td>
        <td>占用内存极小</td>
        <td>有误判的情况且不容易删除元素</td>
        <td>随机删除</td>
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
</table>

# 项目特点

1. 多种方案提供不同场景需求。
2. 基于Lua脚本支持批量操作，速度快。
3. 支持异步，可快速集成到异步代码和异步框架中。

# 代码示例

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

## Others

和上述示例类似

# 关于作者

1. 邮箱：1194542196@qq.com
2. 微信：hu1194542196