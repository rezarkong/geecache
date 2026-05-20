# 压测记录模板

建议把这份文档当成项目的性能记录页，后续压测后直接补数字。

## 测试环境

- CPU：
- 内存：
- Go 版本：
- 操作系统：
- 测试日期：

## 测试命令

基础测试：

```bash
go test ./...
go test -race ./...
go test -bench . -benchmem
BENCH_EVICTOR=lru go test -run '^$' -bench . -benchmem
BENCH_EVICTOR=lfu go test -run '^$' -bench . -benchmem
```

启动三节点和 API 服务后，可使用：

```bash
./scripts/loadtest.sh http://localhost:9999/api?key=Tom 1000 50
./scripts/loadtest.sh http://localhost:9999/api?key=unknown 1000 50
curl "http://localhost:9999/metrics"
```

## 建议记录指标

- LRU 与 LFU 的 benchmark 对比
- 缓存命中延迟
- 同 key 并发访问时的回源次数
- peer 请求次数、失败次数、重试次数、回退次数
- missing key 开启空值缓存前后的回源变化
- 命中率、平均本地加载耗时、平均 peer 加载耗时

## 算法对比建议

建议至少记录下面两组：

- `BENCH_EVICTOR=lru go test -run '^$' -bench . -benchmem`
- `BENCH_EVICTOR=lfu go test -run '^$' -bench . -benchmem`

重点观察：

- `BenchmarkGroupGetCacheHit`
- `BenchmarkGroupGetParallelSameKey`

推荐记录成表格：

| Benchmark | Evictor | ns/op | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: |
| `BenchmarkGroupGetCacheHit` | `lru` |  |  |  |
| `BenchmarkGroupGetCacheHit` | `lfu` |  |  |  |
| `BenchmarkGroupGetParallelSameKey` | `lru` |  |  |  |
| `BenchmarkGroupGetParallelSameKey` | `lfu` |  |  |  |

## 总结示例

- 50 并发访问同一个热点 key 时，`singleflight` 将底层回源次数从 50 次压缩到 1 次。
- missing key 开启 30 秒空值缓存后，重复请求不再持续打到后端数据源。
- peer 故障场景下，请求经过超时、重试和熔断后回退到本地 getter，避免单个异常节点持续拖慢链路。
