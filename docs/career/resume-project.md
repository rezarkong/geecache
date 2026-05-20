# 简历项目描述

## 一句话版本

基于 Go 实现分布式缓存原型，支持可插拔淘汰算法（LRU/LFU）、本地缓存、一致性哈希、singleflight、gRPC + Protobuf 节点通信，并补充了 TTL、空值缓存、防穿透、peer 超时重试、熔断降级和运行时指标。

## 两到三条版本

- 基于 Go 实现分布式缓存原型，支持 LRU/LFU 可切换淘汰策略、本地缓存、一致性哈希路由、`singleflight` 热点 key 并发合并，以及 gRPC + Protobuf 节点间通信。
- 为缓存链路补充正常缓存 TTL、missing key 空值缓存、peer 超时重试、退避、熔断与本地降级，提升异常节点和重复 miss 场景下的稳定性。
- 通过运行时指标、集成测试、`go test -race` 和 benchmark 验证缓存命中路径、热点 key 并发访问和远端节点加载行为。

## 面向校招/初中级版本

- 独立实现一个 Go 分布式缓存项目，包含 LRU/LFU 可切换缓存淘汰、一致性哈希节点选择、`singleflight` 并发请求去重和 gRPC + Protobuf 节点通信。
- 针对缓存常见问题补充空值缓存、防穿透、TTL 过期控制、peer 超时重试和本地降级，提升缓存服务在失败场景下的可用性。
- 编写单元测试、gRPC 集成测试和 benchmark，对热点 key 并发场景和缓存命中性能进行验证。

## 面向社招版本

- 设计并实现 Go 分布式缓存原型，围绕本地缓存、远端 peer 拉取和一致性哈希路由构建完整读路径，支持 `context` 透传、热点 key 合并和节点间 gRPC + Protobuf 通信。
- 在异常处理上补充 peer 超时、重试退避、熔断和本地回退机制，并对远端 `not found` 语义和空值缓存策略做区分，避免无效回源和错误降级。
- 构建运行时指标体系，统计命中率、peer 尝试/成功/失败耗时、重试次数和熔断次数，并通过 race test、集成测试和 benchmark 验证实现质量。

## 可选量化表达

如果你后续做了压测，可以把下面这类数字补进去：

- 在 50 并发访问同一热点 key 的场景下，通过 `singleflight` 将底层回源次数从 50 次压缩到 1 次。
- 本地缓存命中 benchmark 达到 `82.58 ns/op`，并发同 key 访问 benchmark 达到 `147.0 ns/op`。
- 针对 missing key 引入空值缓存后，重复 miss 请求在 TTL 窗口内不再持续打到后端数据源。

## 建议放在简历里的关键词

- Go
- 分布式缓存
- LRU
- 一致性哈希
- singleflight
- gRPC
- Protobuf
- 熔断降级
- 缓存穿透
- 性能测试
