package singleflight

import "sync"

// 同一个 key 在同一时刻只允许一次真实加载，其他并发请求不重复回源，等待并复用这次结果
type Result struct {
	Val    interface{}
	Err    error
	Shared *Shared
}

// Shared carries metadata for one shared in-flight call.
// Callers may use Do to run follow-up work exactly once across
// everyone who received the shared result.
type Shared struct {
	Dups int
	once sync.Once
}

// Do runs fn at most once for this shared call.
func (s *Shared) Do(fn func(dups int)) {
	if s == nil || fn == nil || s.Dups <= 0 {
		return
	}
	s.once.Do(func() {
		fn(s.Dups)
	})
}

// call 作为缓存 key
type call struct {
	wg     sync.WaitGroup // 维护有多少请求调用这个 key
	val    interface{}    // 维护 val
	err    error          // 维护 nil 值
	dups   int            // 重复请求数量，不包含首个执行者
	shared *Shared
}

// Group  represents a class of work and forms a namespace in which
// units of work can be executed with duplicate suppression.
// 以 key 为粒度做并发合并
type Group struct {
	mu sync.Mutex       // protects m
	m  map[string]*call // lazily initialized
	// call 作为缓存的 value
}

// Do executes and returns the results of the given function, making
// sure that only one execution is in-flight for a given key at a
// time. If a duplicate comes in, the duplicate caller waits for the
// original to complete and receives the same results.
// Do 调用 DoChan 然后返回结果
func (g *Group) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	result := <-g.DoChan(key, fn)
	return result.Val, result.Err
}

// DoChan 负责并发合并和结果分发
func (g *Group) DoChan(key string, fn func() (interface{}, error)) <-chan Result {
	g.mu.Lock()
	// m 存粒度为 key 的并发
	if g.m == nil {
		g.m = make(map[string]*call)
	}
	// 当前已经有人做访问 key 的操作了
	if c, ok := g.m[key]; ok {
		c.dups++
		g.mu.Unlock()
		// 直接开始返回 result
		return c.resultChan()
	}
	// 如果没做 key 的访问就加锁然后cache取值
	c := new(call)
	c.wg.Add(1)
	c.shared = &Shared{}
	g.m[key] = c
	g.mu.Unlock()

	go func() {
		c.val, c.err = fn()

		g.mu.Lock()
		c.shared.Dups = c.dups
		delete(g.m, key)
		g.mu.Unlock()

		c.wg.Done()
	}()

	return c.resultChan()
}

func (c *call) resultChan() <-chan Result {
	ch := make(chan Result, 1)
	go func() {
		c.wg.Wait()
		ch <- Result{Val: c.val, Err: c.err, Shared: c.shared}
	}()
	return ch
}
