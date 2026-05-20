# 测试、压测与项目亮点

## 测试目录

项目的测试现在统一放在 `./test` 下：

- `test/algo`
- `test/consistenthash`
- `test/geecache`
- `test/singleflight`

## 测试覆盖内容

### 功能测试

- 本地缓存命中 / miss
- TTL 过期
- 空值缓存
- `Delete(keys...)`
- `Clear()`
- 后台 cleanup
- `Close()` / `DeleteGroup()`

### 分布式测试

- gRPC peer 拉取
- peer `ErrNotFound`
- peer 失败后的本地 fallback
- peer 重试
- 熔断行为
- 动态更新 peer

### 并发相关测试

- `singleflight` 合并同 key 并发请求
- 替换同名 group 时旧 cleanup 停止

## 如何运行测试

运行全部测试：

```bash
go test ./...
```

建议定期补跑竞态检测：

```bash
go test -race ./...
```

## benchmark 设计

当前 benchmark 主要看三类：

### 1. CacheHit

衡量纯命中路径开销。

### 2. ParallelSameKey

衡量高并发命中同一个 key 的开销。

### 3. MixedWorkload

衡量热点 key 和冷 key 混合切换时的表现。

### 4. WideKeyspace

衡量缓存远小于 key 空间时的行为。

## benchmark 命令

基础命令：

```bash
go test ./test/geecache -run '^$' -bench . -benchmem
```

指定算法：

```bash
BENCH_EVICTOR=lru   go test ./test/geecache -run '^$' -bench . -benchmem
BENCH_EVICTOR=lfu   go test ./test/geecache -run '^$' -bench . -benchmem
BENCH_EVICTOR=lru-k go test ./test/geecache -run '^$' -bench . -benchmem
BENCH_EVICTOR=arc   go test ./test/geecache -run '^$' -bench . -benchmem
```

放大 key 空间：

```bash
BENCH_EVICTOR=lru-k \
BENCH_HOT_KEYS=32 \
BENCH_COLD_KEYS=50000 \
BENCH_BLOCK_SIZE=10 \
BENCH_COLD_PER_BLOCK=9 \
BENCH_CACHE_ENTRIES=64 \
go test ./test/geecache -run '^$' -bench 'BenchmarkGroupGetMixedWorkload$' -benchmem -benchtime=50000x
```

极大 key 空间：

```bash
BENCH_EVICTOR=arc BENCH_UNIQUE_KEYS=50000 \
go test ./test/geecache -run '^$' -bench 'BenchmarkGroupGetWideKeyspace$' -benchmem -benchtime=50000x
```

## 项目亮点提炼

如果你要用这个项目做面试或简历，可以提炼成下面几类亮点：

### 1. 分布式缓存主链路

- 一致性哈希
- gRPC 节点通信
- 远端 owner 拉取

### 2. 并发与性能

- `singleflight` 合并回源
- shard Cache 降低锁竞争
- 多淘汰算法 benchmark 对比

### 3. 缓存治理

- TTL
- 空值缓存
- 显式失效
- 后台清理
- 生命周期管理

### 4. 稳定性

- peer 重试
- 退避
- 熔断
- 本地降级

## 面试时怎么讲

建议按“问题 -> 方案 -> 权衡”来讲：

1. 为什么要 `singleflight`
2. 为什么要 shard
3. TTL 和后台清理怎么协同
4. 为什么要做 negative Cache
5. 不同淘汰算法在 mixed workload 下的差异是什么
