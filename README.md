# GeeCache

`GeeCache` 是一个使用 Go 实现的分布式缓存原型项目。在基础版本之上，这个仓库已经补充了更完整的工程能力，包括：

- LRU 本地缓存淘汰
- 基于一致性哈希的节点路由
- 使用 `singleflight` 抑制热点 key 并发回源
- 基于 gRPC + Protobuf 的节点间通信
- 基于 `context.Context` 的请求取消与超时透传
- peer 请求超时、重试退避和熔断
- peer 失败后的本地降级
- normal key TTL 与过期抖动
- missing key 空值缓存，降低缓存穿透
- 运行时指标、benchmark 和压测脚本
- 运行时动态更新 peer 列表

## 架构说明

请求处理流程：

1. `Group.Get(key)` 先查询本地 LRU 缓存。
2. 本地未命中时，通过 `singleflight` 合并同一个 key 的并发请求。
3. 如果一致性哈希判断该 key 属于远端节点，则通过 gRPC + Protobuf 拉取数据，并传递请求上下文。
4. 远端 peer 获取失败时，会按配置执行超时控制、重试退避和熔断判断。
5. peer 不可用或临时失败时，降级到本地 getter；如果远端 owner 明确返回 `not found`，则直接返回并可写入空值缓存，不再回本地重复查询。
6. 正常 value 支持 TTL 和抖动，missing key 支持短 TTL 空值缓存。
7. 每个缓存组会记录命中率、peer 尝试次数、成功/失败耗时、本地/远端平均加载耗时等指标。

核心文件：

- `geecache.go`：缓存组生命周期、加载链路、重试、熔断、降级、指标统计
- `grpc.go`：peer 服务端与客户端通信
- `cache.go`：本地缓存封装、TTL 和空值缓存过期逻辑
- `stats.go`：运行时指标统计与快照输出
- `options.go`：缓存 TTL、peer 重试、熔断等配置项
- `algo/`：可插拔淘汰算法实现，目前包含 LRU 和 LFU
- `cmd/server/main.go`：可运行多节点 demo，包含 `/api`、`/metrics` 和 `/admin/peers`

## 运行 Demo

分别在 3 个终端启动缓存节点：

```bash
go run ./cmd/server -addr=localhost:8001 -peers=localhost:8001,localhost:8002,localhost:8003
go run ./cmd/server -addr=localhost:8002 -peers=localhost:8001,localhost:8002,localhost:8003
go run ./cmd/server -addr=localhost:8003 -peers=localhost:8001,localhost:8002,localhost:8003
```

在其中一个节点上启动 API 和指标接口：

```bash
go run ./cmd/server \
  -addr=localhost:8001 \
  -peers=localhost:8001,localhost:8002,localhost:8003 \
  -api=true \
  -api-addr=localhost:9999 \
  -empty-ttl=30s \
  -peer-retries=1
```

切换成 LFU：

```bash
go run ./cmd/server \
  -addr=localhost:8001 \
  -peers=localhost:8001,localhost:8002,localhost:8003 \
  -api=true \
  -api-addr=localhost:9999 \
  -evictor=lfu
```

请求示例：

```bash
curl "http://localhost:9999/api?key=Tom"
curl "http://localhost:9999/api?key=unknown"
curl "http://localhost:9999/metrics"
curl "http://localhost:9999/admin/peers"
```

运行时更新 peer 列表：

```bash
curl -X POST "http://localhost:9999/admin/peers" \
  -H "Content-Type: application/json" \
  -d '{"peers":["localhost:8001","localhost:8003"]}'
```


## 测试

运行单元测试和集成测试：

```bash
go test ./...
go test -race ./...
make test
make race
```

当前覆盖的重点场景：

- 本地缓存命中与未命中
- 正常缓存项 TTL 过期
- `context` 透传后的 gRPC peer 获取
- 同 key 并发请求的 singleflight 合并
- 基于 gRPC + Protobuf 的 peer 拉取
- peer 失败后的本地降级
- peer 重试成功
- peer 明确返回 `not found` 时不再本地 fallback
- peer 熔断打开后的请求拒绝与回退
- missing key 的空值缓存行为
- peer 未初始化和动态更新时的安全处理

## 性能测试

运行本地 benchmark：

```bash
go test -bench . -benchmem
BENCH_EVICTOR=lru go test -bench . -benchmem
BENCH_EVICTOR=lfu go test -bench . -benchmem
make bench
make bench-lru
make bench-lfu
```

当前机器在 2026 年 3 月 12 日的参考结果：

| Benchmark | Evictor | ns/op | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: |
| `BenchmarkGroupGetCacheHit` | `lru` | `90.71` | `0` | `0` |
| `BenchmarkGroupGetCacheHit` | `lfu` | `268.4` | `96` | `2` |
| `BenchmarkGroupGetParallelSameKey` | `lru` | `169.0` | `0` | `0` |
| `BenchmarkGroupGetParallelSameKey` | `lfu` | `293.5` | `96` | `2` |


如果要做简单压测：

```bash
./scripts/loadtest.sh "http://localhost:9999/api?key=Tom" 1000 50
./scripts/loadtest.sh "http://localhost:9999/api?key=unknown" 1000 50
make loadtest-hot
make loadtest-miss
```