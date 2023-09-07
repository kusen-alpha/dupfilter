# 去重过滤器

实现常见的去重方案

# 去重方案

| 去重方案             | 说明                       | 特点                     | 缺点                       |
|------------------|--------------------------|------------------------|--------------------------|  
| RedisBloomFilter | 基于redis bitmap和布隆过滤器算法实现 | 占用内存小                  | 有误判的情况且不容易删除元素，若要删除可随机删除 |
| RedisStringFilter     | 基于redis string类型实现       | 不会误判，能基于过期时间实现查询去重和确认机制 | 占用资源很大，需尽可能压缩和设置过期时间     |
| RedisSetFilter        | 基于redis set类型实现          | 不会误判，占用内存相对较少          | 不易删除元素，若要删除可随机删除         |
| FileFiler | 基于文件+集合实现 | 准确性高                   | 本地内存和存储占用大               |
| MemoryFilter | 基于内存集合实现 | 准确性高                   | 不能持久化                    |

# 项目特点

1. 多种方案提供不同场景需求。
2. 支持批量，速度快。
3. 支持异步，可快速集成到异步代码和异步框架中。

# 示例

## RedisBloomFilter

```python
import redis
from dupfilter import RedisBloomFilter

server = redis.Redis(host="127.0.0.1", port=6379)
rbf = RedisBloomFilter(server=server, key="bf", block_num=2)
print(rbf.insert_many(["1", "2", 3]))
rbf.insert_many(["1", "2", "3"])
print(rbf.insert_many(["1", "2", 3]))
```

## Others

    和上述示例类似