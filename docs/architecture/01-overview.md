# GeeCache 总览

## 项目定位

GeeCache 是一个基于 Go 实现的分布式缓存项目。它不是只有最基础的单机缓存能力，而是已经补上了比较完整的工程能力，包括：

- 本地缓存与多种淘汰算法：`LRU` / `LFU` / `LRU-K` / `ARC`
- shard 化本地缓存
- 一致性哈希路由
- gRPC + Protobuf 节点通信
- `singleflight` 合并同 key 并发请求
- TTL、空值缓存、防穿透
- peer 重试、退避、熔断、本地 fallback
- 显式失效：`Delete(keys...)`、`Clear()`
- group 生命周期管理：`Close()`、`DeleteGroup()`
- 后台清理过期项
- benchmark 与多 key 压测

## 技术栈

语言与基础设施：

- Go 1.24+
- gRPC
- Protobuf
- `context.Context`
- `sync` / `atomic`

核心能力对应：

- 分布式路由：一致性哈希
- 节点通信：gRPC
- 并发去重：`singleflight`
- 本地缓存策略：LRU / LFU / LRU-K / ARC
- 失效管理：TTL、显式删除、后台清理
- 容错：重试、退避、熔断、降级

## 核心模块

### 1. Group 主入口

文件：`geecache.go`

职责：

- 定义缓存命名空间 `Group`
- 提供 `Get` / `GetContext`
- 串起本地缓存、peer 拉取、本地回源
- 管理生命周期与后台清理
- 维护核心统计信息

### 2. 本地缓存层

文件：`Cache.go`

职责：

- 将一个本地 Cache 拆成多个 shard
- 通过 `hash(key) % N` 路由
- 存储带 TTL 和 negative 信息的 `cacheEntry`
- 提供 `delete` / `clear` / `cleanupExpired`git 

### 3. 底层缓存容器

目录：`algo/`

职责：

- 管理 `map[key]value`
- 维护字节占用
- 与淘汰算法交互
- 提供原子条件删除能力

### 4. 分布式通信层

文件：`grpc.go`

职责：

- 维护 peer 列表
- 一致性哈希选择 owner
- 负责 gRPC client/server

### 5. 并发合并层

目录：`singleflight/`

职责：

- 对同一个 key 的并发 miss 做请求合并
- 避免热点 key 同时回源

## 阅读建议

建议按下面顺序读源码：

1. `geecache.go`
2. `Cache.go`
3. `algo/base.go`
4. `algo/lru.go` / `algo/lfu.go` / `algo/lru-k.go` / `algo/arc.go`
5. `grpc.go`
6. `consistenthash/consistenthash.go`
7. `singleflight/singleflight.go`
8. `options.go` / `stats.go`
9. `cmd/server/main.go`
10. `test/geecache/geecache_test.go`
