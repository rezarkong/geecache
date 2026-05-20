# GeeCache

`GeeCache` 是一个用 Go 实现的分布式缓存项目，补上了比较完整的工程能力：

- 本地缓存，支持 `LRU` / `LFU` / `LRU-K` / `ARC`
- 一致性哈希路由 peer
- `singleflight` 合并同 key 并发回源
- gRPC + Protobuf 节点通信
- peer 重试、退避、熔断和本地降级
- 正常缓存 TTL、空值缓存 TTL
- 本地缓存分片
- 显式失效：`Delete(keys...)`、`Clear()`
- 可选后台清理过期项：`WithCleanupInterval`
- 运行时核心指标导出

## 文档

更详细的实现说明已经整理到 `./docs`：

- `docs/README.md`：文档总索引
- `docs/architecture/`：架构与源码阅读文档
- `docs/benchmarks/`：压测记录模板和性能材料
- `docs/career/`：简历、面试表述材料

## 项目结构

核心目录：

- `algo/`：本地淘汰算法实现
- `cmd/server/`：demo 节点启动入口和 MySQL 回源逻辑
- `consistenthash/`：一致性哈希环
- `registry/`：etcd 注册与服务记录
- `singleflight/`：同 key 并发合并
- `test/`：按主题拆分的集成测试与 benchmark

核心文件：

- `groups.go`：`Group` 生命周期、缓存读取主链路、peer fallback、TTL 回填、显式失效
- `cache.go`：本地 shard cache 封装、TTL 检查、后台清理入口
- `grpc.go`：peer 选择、gRPC 服务端和客户端
- `mutations.go`：集群写入、删除和失效同步
- `server.go`：进程级 server、HTTP 管理接口和 peer 管理接入
- `options.go`：缓存 TTL、空值缓存、shard、peer 重试、后台清理等配置项
- `stats.go`：核心指标快照
- `scripts/`：启动脚本、MySQL 初始化和压测脚本

## 当前能力

库级接口：

- `NewGroup` / `NewGroupWithOptions`
- `Get` / `GetContext`
- `RegisterPeers`
- `Stats`
- `Delete(keys...)`
- `Clear()`

常用配置：

- `WithEvictor`
- `WithShards`
- `WithCacheTTL`
- `WithEmptyCache`
- `WithCleanupInterval`
- `WithPeerRetries`
- `WithPeerRetryBackoff`
- `WithPeerCircuitBreaker`

## 缓存失效与过期

现在支持三种方式：

1. 惰性过期  
读缓存时检查 `expiresAt`，发现过期就删除，并继续走 miss -> 回源。

2. 显式失效  
可以手动删除部分 key 或清空整个 group：

```go
group.Delete("Tom", "Jack")
group.Clear()
```

3. 后台清理  
如果配置了 `WithCleanupInterval`，会按固定周期扫描 shard 并删除过期项：

```go
group := geecache.NewGroupWithOptions(
    "scores",
    2<<10,
    getter,
    geecache.WithCacheTTL(30*time.Second, 0),
    geecache.WithCleanupInterval(5*time.Second),
)
```

## Demo 运行

环境要求：

- Go 1.24+
- 本机可监听 `localhost:8001`、`localhost:8002`、`localhost:8003`、`localhost:9999`
- 本机可访问 MySQL，且已初始化 `geecache.scores`

MySQL 初始化：

```bash
mysql -u your_user -p < ./scripts/mysql_init.sql
cp .env.example .env
# 然后按你的实际账号密码修改 .env
```

默认表结构和示例数据：

- 数据库：`geecache`
- 表：`scores`
- 主键列：`name`
- 值列：`score`
- 示例 key：`Tom`、`Jack`、`Sam`

启动 3 个纯 gRPC Cache 节点：

```bash
make demo-node1
make demo-node2
make demo-node3
```

等价命令：

```bash
go run ./cmd/server -addr=localhost:8001 -peers=localhost:8001,localhost:8002,localhost:8003
go run ./cmd/server -addr=localhost:8002 -peers=localhost:8001,localhost:8002,localhost:8003
go run ./cmd/server -addr=localhost:8003 -peers=localhost:8001,localhost:8002,localhost:8003
```

如果要让 `8001` 同时暴露 HTTP API：

```bash
make demo-node2
make demo-node3
make demo-api
```

如果你想用一个脚本按端口启动 `etcd + gRPC + 布隆过滤器 + 空值缓存` 节点：

```bash
bash ./scripts/start_etcd_node.sh 8001
bash ./scripts/start_etcd_node.sh 8002
bash ./scripts/start_etcd_node.sh 8003
```

这个脚本会固定：

- 打开 `-use-etcd=true`
- 打开 HTTP API
- API 端口 = 输入端口 `+1000`
- 开启布隆过滤器和 `bloom-reject-on-miss`
- 开启空值缓存

默认值可以通过环境变量覆盖：

```bash
ETCD_ENDPOINTS=127.0.0.1:2379 \
SERVICE_NAME=geecache \
BLOOM_ITEMS=100000 \
EMPTY_TTL=30s \
bash ./scripts/start_etcd_node.sh 8001
```

请求示例：

```bash
curl "http://localhost:9999/api?key=Tom"
curl "http://localhost:9999/api?key=unknown"
curl "http://localhost:9999/metrics"
curl "http://localhost:9999/admin/peers"
curl "http://localhost:9999/admin/hash/ring"
curl "http://localhost:9999/admin/hash/key?key=Tom"
```

运行时更新 peer：

```bash
curl -X POST "http://localhost:9999/admin/peers" \
  -H "Content-Type: application/json" \
  -d '{"peers":["localhost:8001","localhost:8003"]}'
```

## 测试

运行全部测试：

```bash
go test ./...
go test -race ./...
```

当前测试覆盖重点：

- 本地缓存命中 / miss
- `singleflight` 合并
- TTL 过期
- 空值缓存
- `Delete(keys...)` / `Clear()`
- 后台清理过期项
- gRPC peer 拉取
- peer 重试、熔断、本地 fallback
- 动态更新 peer 列表

## Benchmark 与压力测试

benchmark 位于 `./test/geecache`。根目录直接执行 `go test -bench .` 不会跑到这些用例，请显式指定包路径。

### 1. 基础 benchmark

```bash
go test ./test/geecache -run '^$' -bench . -benchmem
```

### 2. 指定淘汰算法

```bash
BENCH_EVICTOR=lru   go test ./test/geecache -run '^$' -bench . -benchmem
BENCH_EVICTOR=lfu   go test ./test/geecache -run '^$' -bench . -benchmem
BENCH_EVICTOR=lru-k go test ./test/geecache -run '^$' -bench . -benchmem
BENCH_EVICTOR=arc   go test ./test/geecache -run '^$' -bench . -benchmem
```

### 3. 多 key 切换的固定次数压力测试

这里用 `BenchmarkGroupGetMixedWorkload` 做混合 key 访问，热 key 和冷 key 交替出现，固定 50000 次操作：

```bash
BENCH_EVICTOR=lru   go test ./test/geecache -run '^$' -bench 'BenchmarkGroupGetMixedWorkload$' -benchmem -benchtime=50000x
BENCH_EVICTOR=lfu   go test ./test/geecache -run '^$' -bench 'BenchmarkGroupGetMixedWorkload$' -benchmem -benchtime=50000x
BENCH_EVICTOR=lru-k go test ./test/geecache -run '^$' -bench 'BenchmarkGroupGetMixedWorkload$' -benchmem -benchtime=50000x
BENCH_EVICTOR=arc   go test ./test/geecache -run '^$' -bench 'BenchmarkGroupGetMixedWorkload$' -benchmem -benchtime=50000x
```

如果想尽可能使用更多不同 key，可以直接把 key 空间放大：

```bash
BENCH_EVICTOR=lru-k \
BENCH_HOT_KEYS=32 \
BENCH_COLD_KEYS=50000 \
BENCH_BLOCK_SIZE=10 \
BENCH_COLD_PER_BLOCK=9 \
BENCH_CACHE_ENTRIES=64 \
go test ./test/geecache -run '^$' -bench 'BenchmarkGroupGetMixedWorkload$' -benchmem -benchtime=50000x
```

### 4. 极大 key 空间压力测试

`BenchmarkGroupGetWideKeyspace` 会尽量访问更多不同 key，适合观察“缓存远小于 key 空间”时的行为：

```bash
BENCH_EVICTOR=lru   BENCH_UNIQUE_KEYS=50000  go test ./test/geecache -run '^$' -bench 'BenchmarkGroupGetWideKeyspace$' -benchmem -benchtime=50000x
BENCH_EVICTOR=lfu   BENCH_UNIQUE_KEYS=50000  go test ./test/geecache -run '^$' -bench 'BenchmarkGroupGetWideKeyspace$' -benchmem -benchtime=50000x
BENCH_EVICTOR=lru-k BENCH_UNIQUE_KEYS=50000  go test ./test/geecache -run '^$' -bench 'BenchmarkGroupGetWideKeyspace$' -benchmem -benchtime=50000x
BENCH_EVICTOR=arc   BENCH_UNIQUE_KEYS=50000  go test ./test/geecache -run '^$' -bench 'BenchmarkGroupGetWideKeyspace$' -benchmem -benchtime=50000x
```

可调参数：

- `BENCH_HOT_KEYS`：hot key 数量
- `BENCH_COLD_KEYS`：mixed workload 里的 cold key 数量
- `BENCH_BLOCK_SIZE`：访问块大小
- `BENCH_COLD_PER_BLOCK`：每个块里 cold key 的次数
- `BENCH_CACHE_ENTRIES`：近似缓存容量
- `BENCH_WARM_HITS`：warmup 次数
- `BENCH_UNIQUE_KEYS`：wide-keyspace 场景中的不同 key 数量

### 本机结果

测试时间：`2026-03-15`  
机器：`AMD Ryzen 7 5800H with Radeon Graphics`

固定 50000 次操作下的结果：

| Evictor | CacheHit ns/op | ParallelSameKey ns/op | MixedWorkload ns/op | Mixed hit_ratio | Mixed miss_ratio | Mixed B/op | Mixed allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `lru` | `109.0` | `122.1` | `3408` | `0.0000` | `1.0000` | `488` | `14` |
| `lfu` | `867.9` | `910.5` | `3639` | `0.2000` | `0.8000` | `442` | `12` |
| `lru-k` | `114.5` | `136.3` | `2990` | `0.2000` | `0.8000` | `417` | `11` |
| `arc` | `107.9` | `136.0` | `7371` | `0.2000` | `0.8000` | `442` | `12` |

结果解读：

- `LRU` 命中路径最快，但在当前混合 key 场景下几乎保不住热点 key。
- `LFU` 能保住热点 key，但访问开销明显更高。
- `LRU-K` 在当前 workload 下是更均衡的选择，命中率和吞吐都比较稳。
- `ARC` 当前实现正确，但在这个 workload 下成本最高。

## 简单 HTTP 压测

如果要压测 demo API：

```bash
./scripts/loadtest.sh "http://localhost:9999/api?key=Tom" 1000 50
./scripts/loadtest.sh "http://localhost:9999/api?key=unknown" 1000 50
make loadtest-hot
make loadtest-miss
```

`scripts/loadtest.sh` 会优先使用 `hey`；如果本机没有安装 `hey`，会退化为串行 `curl` 循环。
