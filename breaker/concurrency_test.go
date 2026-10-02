package breaker

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 多调用方并发 acquire：舱壁槽位不得超发，释放后总量自洽。
func TestConcurrentBulkheadNoOvercommit(t *testing.T) {
	clk := NewFakeClock(t0)
	cfg := testConfig()
	cfg.MaxConcurrent = 8
	b := New(cfg, clk)

	const callers = 200
	var acquired atomic.Int64
	var maxInflight atomic.Int64
	var cur atomic.Int64
	var wg sync.WaitGroup
	releases := make(chan func(), callers)

	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rel, err := b.acquire()
			if err == ErrBulkheadFull {
				return
			}
			if err != nil {
				t.Errorf("unexpected err: %v", err)
				return
			}
			n := cur.Add(1)
			for {
				m := maxInflight.Load()
				if n <= m || maxInflight.CompareAndSwap(m, n) {
					break
				}
			}
			acquired.Add(1)
			releases <- rel
		}()
	}
	wg.Wait()
	close(releases)
	if got := acquired.Load(); got != 8 {
		t.Fatalf("acquired = %d, want 8 (舱壁不得超发)", got)
	}
	if got := maxInflight.Load(); got != 8 {
		t.Fatalf("maxInflight = %d, want 8", got)
	}
	for rel := range releases {
		rel()
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.inflight != 0 {
		t.Fatalf("inflight = %d, want 0 (释放后自洽)", b.inflight)
	}
}

// 多调用方同时触发状态转移：最终状态确定，且打开后无重复放行。
func TestConcurrentTripToOpen(t *testing.T) {
	clk := NewFakeClock(t0)
	cfg := testConfig()
	cfg.ConsecutiveFailures = 1 // 一次失败即打开
	b := New(cfg, clk)

	const callers = 64
	var wg sync.WaitGroup
	var opens atomic.Int64
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.report(time.Millisecond, true, false)
			if b.State() == StateOpen {
				opens.Add(1)
			}
		}()
	}
	wg.Wait()
	if opens.Load() != callers {
		t.Fatalf("所有调用方应一致观察到 open, got %d/%d", opens.Load(), callers)
	}
	if got := b.State(); got != StateOpen {
		t.Fatalf("state = %v, want open", got)
	}
}

// 半开状态并发竞争：试探放行数不得超过配置上限，且不得超发。
func TestConcurrentHalfOpenProbeLimit(t *testing.T) {
	for round := 0; round < 20; round++ {
		clk := NewFakeClock(t0)
		cfg := testConfig()
		cfg.HalfOpenMaxConcurrent = 3
		b := New(cfg, clk)
		for i := 0; i < 3; i++ {
			b.report(time.Millisecond, true, false)
		}
		clk.Advance(5 * time.Second)

		const callers = 100
		var admitted atomic.Int64
		var wg sync.WaitGroup
		releases := make(chan func(), callers)
		for i := 0; i < callers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				rel, err := b.acquire()
				if err == nil {
					admitted.Add(1)
					releases <- rel
				} else if err != ErrHalfOpenLimited && err != ErrBulkheadFull {
					t.Errorf("unexpected err: %v", err)
				}
			}()
		}
		wg.Wait()
		close(releases)
		if got := admitted.Load(); got != 3 {
			t.Fatalf("round %d: admitted = %d, want 3 (试探不得超发)", round, got)
		}
		for rel := range releases {
			rel()
		}
		b.mu.Lock()
		if b.halfConcurrent != 0 || b.inflight != 0 {
			t.Fatalf("round %d: halfConcurrent=%d inflight=%d, want 0/0",
				round, b.halfConcurrent, b.inflight)
		}
		b.mu.Unlock()
	}
}

// 半开恢复竞争：并发上报成功，恰好达到阈值后关闭，不抖动。
func TestConcurrentHalfOpenRecovery(t *testing.T) {
	clk := NewFakeClock(t0)
	cfg := testConfig()
	cfg.HalfOpenSuccesses = 10
	cfg.HalfOpenMaxConcurrent = 20
	b := New(cfg, clk)
	for i := 0; i < 3; i++ {
		b.report(time.Millisecond, true, false)
	}
	clk.Advance(5 * time.Second)
	if _, err := b.acquire(); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.report(time.Millisecond, false, false)
		}()
	}
	wg.Wait()
	if got := b.State(); got != StateClosed {
		t.Fatalf("state = %v, want closed", got)
	}
	// 关闭后再上报成功不得改变状态（无抖动）
	for i := 0; i < 5; i++ {
		b.report(time.Millisecond, false, false)
	}
	if got := b.State(); got != StateClosed {
		t.Fatalf("state = %v, want closed (不应抖动)", got)
	}
}
