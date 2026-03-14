package singleflight

import "sync"

type Result struct {
	Val interface{}
	Err error
}

// call is an in-flight or completed Do call
type call struct {
	wg  sync.WaitGroup
	val interface{}
	err error
}

// Group represents a class of work and forms a namespace in which
// units of work can be executed with duplicate suppression.
type Group struct {
	mu sync.Mutex       // protects m
	m  map[string]*call // lazily initialized
}

// Do executes and returns the results of the given function, making
// sure that only one execution is in-flight for a given key at a
// time. If a duplicate comes in, the duplicate caller waits for the
// original to complete and receives the same results.
func (g *Group) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	result := <-g.DoChan(key, fn)
	return result.Val, result.Err
}

// DoChan is like Do but returns a channel that receives the shared result.
func (g *Group) DoChan(key string, fn func() (interface{}, error)) <-chan Result {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*call)
	}
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		return c.resultChan()
	}
	c := new(call)
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	go func() {
		c.val, c.err = fn()
		c.wg.Done()

		g.mu.Lock()
		delete(g.m, key)
		g.mu.Unlock()
	}()

	return c.resultChan()
}

func (c *call) resultChan() <-chan Result {
	ch := make(chan Result, 1)
	go func() {
		c.wg.Wait()
		ch <- Result{Val: c.val, Err: c.err}
	}()
	return ch
}
