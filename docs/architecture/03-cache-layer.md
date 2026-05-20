# 本地缓存层实现细节

## 分层关系

GeeCache 的本地缓存不是一层，而是两层：

### 1. 业务缓存包装层：`Cache.go`

负责：

- shard 路由
- TTL 检查
- negative Cache
- 显式删除 / 清空
- 后台清理过期项

### 2. 通用缓存容器层：`algo/base.go`

负责：

- `map[string]Value`
- 字节统计
- 淘汰触发
- 条件删除
- 与具体 evictor 交互

## 为什么要 shard

单个全局 Cache 在并发访问下容易形成锁热点。

当前实现会把一个 Cache 分成多个 shard：

- 每个 shard 独立维护一个 `algo.Cache`
- key 通过 `hash(key) % N` 路由到对应 shard
- 这样能降低并发下的锁竞争

默认 shard 数目前是 1，只有显式 `WithShards(N)` 才会分片。

## cacheEntry 结构

本地缓存存的不是裸 value，而是 `cacheEntry`：

- `value ByteView`
- `expiresAt time.Time`
- `negative bool`

这样一个 entry 同时能表达：

- 正常缓存值
- 带 TTL 的缓存值
- 不存在结果的短期缓存

## TTL 如何实现

### 正常值 TTL

如果配置了 `WithCacheTTL(ttl, jitter)`：

- `populateCache` 写入时会生成 `expiresAt`
- 支持随机抖动，避免大批 key 同时失效

### 空值缓存 TTL

如果 `Getter` 或 peer 返回 `ErrNotFound`，且开启 `WithEmptyCache(ttl)`：

- 会写入一个 `negative=true` 的 entry
- 在 TTL 内再次访问，直接返回 `ErrNotFound`

## 过期删除为什么要原子化

这个项目后期专门修过一次：

旧实现的问题是：

1. 先 `Get`
2. 判断过期
3. 再 `Remove`

这在并发下会误删新值。

现在底层 `algo.Cache` 提供了：

- `GetOrRemoveIf`
- `RemoveIf`

从而把“判断是否过期 + 删除”放到同一把锁里完成。

## Delete / Clear / cleanup 的区别

### Delete(keys...)

按 key 删除本地缓存项。

适合：

- 精准失效
- 数据更新后定点删除

### Clear()

清空当前 group 的本地缓存。

适合：

- 管理操作
- 全量失效

注意：它当前更偏“管理语义”，在高并发写入中不是严格屏障。

### cleanupExpired

后台定时扫描过期项。

适合：

- 长时间不访问的脏过期数据清理

## 底层 algo.Cache 的职责

`algo.Cache` 不理解分布式，也不理解 peer。

它只负责：

- `Add`
- `Get`
- `Remove`
- `Keys`
- `GetOrRemoveIf`
- `RemoveIf`

淘汰逻辑通过 `Evictor` 接口解耦：

```go
type Evictor interface {
    OnAdd(key string)
    OnAccess(key string)
    OnRemove(key string)
    OnEvict(key string)
    Evict() string
}
```

这让上层可以切换不同算法，而不用改缓存主流程。
