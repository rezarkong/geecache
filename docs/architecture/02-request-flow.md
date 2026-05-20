# GeeCache 整体请求流程

## 一次 `Get(key)` 的完整路径

入口在 `Group.Get` / `Group.GetContext`。

完整流程如下：

```text
Get(key)
  -> 查本地 shard Cache
    -> 命中: 返回
    -> 过期: 删除并视为 miss
    -> miss: 进入 load
  -> singleflight 合并同 key 请求
  -> 优先尝试 peer
    -> peer 成功: 返回
    -> peer not found: 写 negative Cache 并返回
    -> peer 失败: 走本地 getter
  -> 本地 getter 成功后写回 Cache
  -> 返回 ByteView
```

## 分步骤说明

### 1. 入口校验

`GetContext` 会先检查 key 是否为空，并记录请求计数。

### 2. 查询本地缓存

调用 `mainCache.get(key)`：

- 根据 hash 找到 shard
- 从 shard 的 `algo.Cache` 中取值
- 如果值过期，则原子删除并返回 miss
- 如果值是 negative entry，则返回 `ErrNotFound`

### 3. singleflight 合并

本地 miss 后，不会直接回源，而是先进入 `loader.DoChan(key, ...)`。

目的：

- 同一个 key 并发 miss 时只真正加载一次
- 其他 goroutine 等待这次结果

### 4. 尝试从 peer 获取

如果注册了 `PeerPicker`：

- 通过一致性哈希选出 key 的 owner
- 如果 circuit breaker 允许，发起 gRPC 拉取

可能结果：

- 成功：直接返回数据
- `ErrNotFound`：写 negative Cache
- 普通失败：记录失败并 fallback 到本地 getter

### 5. 本地回源

如果 peer 不可用或没有 peer：

- 调用用户传入的 `Getter`
- 成功后构造 `ByteView`
- 根据 TTL 配置写回本地缓存

### 6. 后续访问

当同一个 key 再次访问时：

- 如果还没过期，直接本地命中
- 如果过期，先删再重新走 miss 流程

## 失效与过期路径

当前支持三种失效方式：

### 1. 惰性过期

读到 entry 时检查 `expiresAt`，过期就删除。

### 2. 显式失效

- `Delete(keys...)`
- `Clear()`

### 3. 后台清理

如果启用 `WithCleanupInterval`：

- 定时扫描各 shard
- 删除已经过期的 entry

## 生命周期相关流程

### 创建 Group

`NewGroupWithOptions`：

- 初始化 `Group`
- 应用配置项
- 启动后台 cleanup
- 注册到全局 `groups`

### 销毁 Group

- `group.Close()`
- `DeleteGroup(name)`

作用：

- 从全局 registry 删除
- 停止后台 cleanup goroutine
