# 淘汰算法实现说明

GeeCache 当前支持四种本地淘汰算法。

## 1. LRU

文件：`algo/lru.go`

特点：

- 最近最少使用淘汰
- 实现简单
- 命中路径开销低

适用场景：

- 访问局部性明显
- 更在意简单和低开销

## 2. LFU

文件：`algo/lfu.go`

特点：

- 按访问频率淘汰
- 能更好保住热点 key
- 访问维护成本更高

适用场景：

- 热点长期稳定
- 希望热点 key 尽量常驻

## 3. LRU-K

文件：`algo/lru-k.go`

特点：

- 不是看最近一次访问，而是看第 K 次访问历史
- 比 LRU 更能区分“偶然访问”和“真正热点”

适用场景：

- 有冷热混合访问
- 想避免一次偶然访问就把冷 key 当热点保留

## 4. ARC

文件：`algo/arc.go`

特点：

- 自适应平衡 recency 和 frequency
- 思路更复杂
- 在不同 workload 下表现差异更大

适用场景：

- 希望自动在 LRU / LFU 风格之间调节

## 底层交互方式

所有算法都实现统一接口：

```go
type Evictor interface {
    OnAdd(key string)
    OnAccess(key string)
    OnRemove(key string)
    OnEvict(key string)
    Evict() string
}
```

缓存容器不会直接决定该淘汰谁，而是把决策交给算法。

## benchmark 结论

结合当前 benchmark：

- `LRU`：命中路径最轻，但混合 key 场景命中率不占优
- `LFU`：热点保留效果好，但访问成本高
- `LRU-K`：目前是更均衡的实现
- `ARC`：实现正确，但当前 workload 下开销最大

## 学习建议

如果你想真正理解这些算法，建议顺序是：

1. 先看 `LRU`
2. 再看 `LFU`
3. 再看 `LRU-K`
4. 最后看 `ARC`

因为它们的复杂度是逐步上升的。
