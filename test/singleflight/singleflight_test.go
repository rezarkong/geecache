package singleflight_test

import (
	"geecache/singleflight"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDo(t *testing.T) {
	var g singleflight.Group
	v, err := g.Do("key", func() (interface{}, error) {
		return "bar", nil
	})

	if v != "bar" || err != nil {
		t.Errorf("Do v = %v, error = %v", v, err)
	}
}

func TestDoSuppressesDuplicateCalls(t *testing.T) {
	var g singleflight.Group
	var calls int32
	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			v, err := g.Do("key", func() (interface{}, error) {
				atomic.AddInt32(&calls, 1)
				time.Sleep(10 * time.Millisecond)
				return "bar", nil
			})
			if err != nil {
				t.Errorf("Do error = %v", err)
			}
			if v != "bar" {
				t.Errorf("Do v = %v", v)
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("expected fn to run once, got %d", got)
	}
}
