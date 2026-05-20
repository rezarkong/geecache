# 分布式与容错实现细节

## 一致性哈希

模块：`consistenthash/`

作用：

- 将 key 映射到 Cache node
- peer 数量变化时尽量减少迁移

在 `GRPCPool.Set(peers...)` 中：

- 初始化哈希环
- 添加节点
- 为每个节点维护一个 `grpcGetter`

## Peer 选择

读取 key 时，如果本地 miss：

1. `Group` 通过 `PeerPicker` 找到 owner
2. 如果 owner 不是自己，就尝试远端拉取

这里的核心接口是：

- `PeerPicker`
- `PeerGetter`

## gRPC 通信

模块：`grpc.go`

### 服务端

`groupCacheServer.Get`：

- 根据 `group` 名字取出对应 `Group`
- 调用 `group.GetContext`
- 返回 `pb.Response`

### 客户端

`grpcGetter`：

- 懒创建 gRPC client
- 将 `pb.Request` 发送到远端节点
- 把 gRPC 的 `NotFound` 转成项目内的 `ErrNotFound`

## singleflight 合并

模块：`singleflight/`

场景：

- 多个 goroutine 同时请求同一个 miss key

如果不合并：

- 会同时打 peer 或本地 getter
- 造成重复回源

现在的做法：

- 以 key 为粒度做请求合并
- 只让一个 goroutine 真的执行加载
- 其他 goroutine 等它的结果

## peer 失败处理

### 重试

通过 `WithPeerRetries` 和 `WithPeerRetryBackoff` 控制：

- 失败后按次数重试
- 支持逐步增加等待时间

### 熔断

字段：

- `peerFailureThreshold`
- `peerCircuitOpen`

机制：

- 连续失败达到阈值后打开熔断
- 在打开窗口内，直接跳过这个 peer
- 后续再尝试恢复

### 本地降级

如果 peer 失败：

- 不直接返回错误
- 回退到本地 getter

这样可用性更高。

## 生命周期与资源管理

项目后期补了 group 生命周期管理：

- `group.Close()`
- `DeleteGroup(name)`

作用：

- 将 group 从全局 registry 删除
- 停掉后台 cleanup goroutine

如果重新创建同名 group：

- 新建前会先停掉旧的 cleanup
- 避免 goroutine 泄漏
