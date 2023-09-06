# 去重过滤器
    常见的去重方案

# 去重方案
| 去重方案 | 说明 | 特点 |
| --- | --- | --- | 
| BloomFilter | 基于布隆过滤器实现 | 占用内存小 | 
| StringFilter | 基于redis string类型实现 | 能基于过期时间实现查询去重和确认机制 |
| SetFilter | 基于redis set类型实现 | 占用内存相对较少 | 