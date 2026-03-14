# GeeCache

`GeeCache` 是一个使用 Go 实现的分布式缓存原型项目。在基础版本之上，这个仓库已经补充了更完整的工程能力，包括：

- LRU/LFU/LRU-K/ARC 本地缓存淘汰
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

1. `Group.Get(key)` 先查询本地缓存。
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
- `algo/`：可插拔淘汰算法实现，目前包含 LRU、LFU、LRU-K 和 ARC
- `cmd/server/main.go`：可运行多节点 demo，包含 `/api`、`/metrics` 和 `/admin/peers`

## 已实现能力

库级接口：

- `NewGroup` / `NewGroupWithOptions`：创建缓存组
- `Get` / `GetContext`：读取缓存，支持显式传入 `context.Context`
- `RegisterPeers`：注册 peer 选择器
- `Stats`：导出运行时指标快照

可选项：

- `WithEvictor`：切换 LRU / LFU / LRU-K / ARC
- `WithCacheTTL`：设置正常缓存项 TTL 和过期抖动
- `WithEmptyCache`：开启 missing key 空值缓存
- `WithPeerRetries`：配置 peer 重试次数
- `WithPeerRetryBackoff`：配置重试退避
- `WithPeerCircuitBreaker`：配置熔断阈值和打开时长

Demo 服务提供的接口：

- `GET /api?key=<key>`：读取缓存值
- `GET /metrics`：输出 JSON 指标
- `GET /admin/peers`：查看当前 peer 列表
- `POST /admin/peers`：动态更新 peer 列表

## 运行 Demo

环境要求：

- Go 1.24+
- 本机可监听 `localhost:8001`、`localhost:8002`、`localhost:8003`、`localhost:9999`

如果只跑纯 gRPC cache 节点，可以拉起 3 个节点：

```bash
make demo-node1
make demo-node2
make demo-node3
```

等价的原始命令：

```bash
go run ./cmd/server -addr=localhost:8001 -peers=localhost:8001,localhost:8002,localhost:8003
go run ./cmd/server -addr=localhost:8002 -peers=localhost:8001,localhost:8002,localhost:8003
go run ./cmd/server -addr=localhost:8003 -peers=localhost:8001,localhost:8002,localhost:8003
```

如果希望 `8001` 同时暴露 HTTP API 和指标接口，不要再单独启动 `demo-node1`。应当启动这一组：

```bash
make demo-node2
make demo-node3
make demo-api
```

其中 `demo-api` 会占用 `localhost:8001`，它本身就是第一个 cache 节点。

等价命令：

```bash
go run ./cmd/server -addr=localhost:8002 -peers=localhost:8001,localhost:8002,localhost:8003
go run ./cmd/server -addr=localhost:8003 -peers=localhost:8001,localhost:8002,localhost:8003
go run ./cmd/server \
  -addr=localhost:8001 \
  -peers=localhost:8001,localhost:8002,localhost:8003 \
  -api=true \
  -api-addr=localhost:9999 \
  -empty-ttl=30s \
  -peer-retries=1
```

如果只想单独看 `demo-api` 的原始启动命令：

```bash
make demo-api
```

对应命令：

```bash
go run ./cmd/server \
  -addr=localhost:8001 \
  -peers=localhost:8001,localhost:8002,localhost:8003 \
  -api=true \
  -api-addr=localhost:9999 \
  -empty-ttl=30s \
  -peer-retries=1
```

如果要切换成 LFU，同样不要再启动 `demo-node1`。应当启动：

```bash
make demo-node2
make demo-node3
make demo-api-lfu
```

对应的 API 节点命令：

```bash
make demo-api-lfu
```

对应命令：

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

支持的启动参数：

| Flag | 默认值 | 说明 |
| --- | --- | --- |
| `-addr` | `localhost:8001` | 当前 gRPC cache 节点地址 |
| `-peers` | `localhost:8001,localhost:8002,localhost:8003` | 初始 peer 列表 |
| `-api` | `false` | 是否同时启动 HTTP API |
| `-api-addr` | `localhost:9999` | HTTP API 监听地址 |
| `-empty-ttl` | `30s` | 空值缓存 TTL |
| `-peer-retries` | `1` | peer 失败后的重试次数 |
| `-evictor` | `lru` | 淘汰算法，支持 `lru` / `lfu` / `lru-k` / `arc` |

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

按包执行时，当前测试主要分布在：

- `./test/algo`
- `./test/consistenthash`
- `./test/geecache`
- `./test/singleflight`

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

当前 benchmark 实现在 `./test/geecache` 包下，根目录直接执行 `go test -bench . -benchmem` 不会跑到这些用例。请使用下面这些命令：

```bash
go test ./test/geecache -run '^$' -bench . -benchmem
BENCH_EVICTOR=lru go test ./test/geecache -run '^$' -bench . -benchmem
BENCH_EVICTOR=lfu go test ./test/geecache -run '^$' -bench . -benchmem
go test ./test/geecache -run '^$' -bench BenchmarkGroupGetMixedWorkload -benchmem
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

`scripts/loadtest.sh` 会优先使用 `hey`；如果本机没有安装 `hey`，会退化为串行 `curl` 循环。
