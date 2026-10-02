package circuitbreaker

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// ---------- helpers ----------

func newTestClock() *FakeClock {
	return NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
}

type result struct {
	v   int
	err error
}

func execute(dep *Dependency[int], ctx context.Context, fn func(context.Context) (int, error)) <-chan result {
	ch := make(chan result, 1)
	go func() {
		v, err := dep.Execute(ctx, fn)
		ch <- result{v: v, err: err}
	}()
	return ch
}

func waitResult(t *testing.T, ch <-chan result) result {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for result")
		return result{}
	}
}

func pollUntil(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		runtime.Gosched()
	}
	t.Fatal("poll condition timed out")
}

// settle lets background goroutines reach their next timer wait before the
// fake clock is advanced. The fake clock does not fire on real time, so a
// short real sleep is safe and deterministic.
func settle() { time.Sleep(5 * time.Millisecond) }

func okFn(_ context.Context) (int, error)  { return 42, nil }
func errFn(_ context.Context) (int, error) { return 0, ErrStub }

// ---------- state machine ----------

func TestHealthyCallsStayClosed(t *testing.T) {
	clk := newTestClock()
	dep := NewDependency[int]("h",
		WithClock(clk),
		WithMinimumCalls(1000),
		WithTimeout(0),
	)
	for i := 0; i < 5; i++ {
		r := waitResult(t, execute(dep, context.Background(), okFn))
		if r.err != nil {
			t.Fatalf("unexpected error: %v", r.err)
		}
	}
	if dep.State() != StateClosed {
		t.Fatalf("want closed, got %s", dep.State())
	}
	m := dep.Metrics()
	if m.TotalCalls != 5 || m.SuccessfulCalls != 5 || m.FailedCalls != 0 {
		t.Fatalf("bad metrics: %+v", m)
	}
}

func TestConsecutiveFailuresOpenAndReject(t *testing.T) {
	clk := newTestClock()
	dep := NewDependency[int]("h",
		WithClock(clk),
		WithConsecutiveFailures(3),
		WithMinimumCalls(1000),
		WithTimeout(0),
	)
	for i := 0; i < 3; i++ {
		r := waitResult(t, execute(dep, context.Background(), errFn))
		if !errors.Is(r.err, ErrStub) {
			t.Fatalf("call %d: want ErrStub, got %v", i, r.err)
		}
	}
	if dep.State() != StateOpen {
		t.Fatalf("want open, got %s", dep.State())
	}
	// Further calls are rejected without touching the dependency.
	var calls int64
	fn := func(ctx context.Context) (int, error) {
		atomic.AddInt64(&calls, 1)
		return 0, nil
	}
	r := waitResult(t, execute(dep, context.Background(), fn))
	if !errors.Is(r.err, ErrCircuitOpen) {
		t.Fatalf("want ErrCircuitOpen, got %v", r.err)
	}
	if calls != 0 {
		t.Fatalf("dependency must not be called while open, got %d", calls)
	}
	if dep.Metrics().RejectedCalls != 1 {
		t.Fatalf("want 1 rejection, got %d", dep.Metrics().RejectedCalls)
	}
}

func TestOpenRecoversViaProbes(t *testing.T) {
	clk := newTestClock()
	dep := NewDependency[int]("h",
		WithClock(clk),
		WithConsecutiveFailures(1),
		WithMinimumCalls(1000),
		WithOpenTimeout(10*time.Second),
		WithHalfOpen(3, 3, 2),
		WithTimeout(0),
	)
	waitResult(t, execute(dep, context.Background(), errFn))
	if dep.State() != StateOpen {
		t.Fatalf("want open, got %s", dep.State())
	}
	clk.Advance(10*time.Second + time.Nanosecond)
	if dep.State() != StateHalfOpen {
		t.Fatalf("want half-open, got %s", dep.State())
	}
	// Two successful probes close the breaker.
	for i := 0; i < 2; i++ {
		r := waitResult(t, execute(dep, context.Background(), okFn))
		if r.err != nil {
			t.Fatalf("probe %d: unexpected error: %v", i, r.err)
		}
	}
	if dep.State() != StateClosed {
		t.Fatalf("want closed after 2 successes, got %s", dep.State())
	}
}

func TestHalfOpenFailureReopensImmediately(t *testing.T) {
	clk := newTestClock()
	dep := NewDependency[int]("h",
		WithClock(clk),
		WithConsecutiveFailures(1),
		WithMinimumCalls(1000),
		WithOpenTimeout(10*time.Second),
		WithHalfOpen(3, 3, 2),
		WithTimeout(0),
	)
	waitResult(t, execute(dep, context.Background(), errFn))
	clk.Advance(10*time.Second + time.Nanosecond)
	if dep.State() != StateHalfOpen {
		t.Fatalf("want half-open, got %s", dep.State())
	}
	r := waitResult(t, execute(dep, context.Background(), errFn))
	if !errors.Is(r.err, ErrStub) {
		t.Fatalf("want ErrStub, got %v", r.err)
	}
	if dep.State() != StateOpen {
		t.Fatalf("want open immediately after failed probe, got %s", dep.State())
	}
}

func TestHalfOpenPermitsExhausted(t *testing.T) {
	clk := newTestClock()
	dep := NewDependency[int]("h",
		WithClock(clk),
		WithConsecutiveFailures(1),
		WithMinimumCalls(1000),
		WithOpenTimeout(10*time.Second),
		WithHalfOpen(2, 2, 2),
		WithTimeout(0),
	)
	waitResult(t, execute(dep, context.Background(), errFn))
	clk.Advance(10*time.Second + time.Nanosecond)

	block := make(chan struct{})
	fn := func(ctx context.Context) (int, error) {
		<-block
		return 42, nil
	}
	c1 := execute(dep, context.Background(), fn)
	c2 := execute(dep, context.Background(), fn)
	pollUntil(t, func() bool { return dep.Metrics().HalfOpenInFlight == 2 })
	// No more probes allowed: rejected, dependency untouched.
	r := waitResult(t, execute(dep, context.Background(), okFn))
	if !errors.Is(r.err, ErrHalfOpenBusy) {
		t.Fatalf("want ErrHalfOpenBusy, got %v", r.err)
	}
	close(block)
	if waitResult(t, c1).err != nil {
		t.Fatalf("probe 1 failed: %v", waitResult(t, c1).err)
	}
	if waitResult(t, c2).err != nil {
		t.Fatalf("probe 2 failed: %v", waitResult(t, c2).err)
	}
	if dep.State() != StateClosed {
		t.Fatalf("want closed, got %s", dep.State())
	}
}

func TestFailureRateOpens(t *testing.T) {
	clk := newTestClock()
	dep := NewDependency[int]("h",
		WithClock(clk),
		WithConsecutiveFailures(0),
		WithMinimumCalls(4),
		WithFailureRateThreshold(50),
		WithTimeout(0),
	)
	// 2 success + 2 failure => 50% failure rate.
	for i := 0; i < 2; i++ {
		waitResult(t, execute(dep, context.Background(), okFn))
	}
	for i := 0; i < 2; i++ {
		waitResult(t, execute(dep, context.Background(), errFn))
	}
	if dep.State() != StateOpen {
		t.Fatalf("want open at 50%% failure rate, got %s", dep.State())
	}
}

func TestSlowCallRateOpens(t *testing.T) {
	clk := newTestClock()
	dep := NewDependency[int]("h",
		WithClock(clk),
		WithConsecutiveFailures(0),
		WithMinimumCalls(4),
		WithFailureRateThreshold(100),
		WithSlowCallRateThreshold(50),
		WithSlowCallThreshold(100*time.Millisecond),
		WithTimeout(0),
	)
	// 2 fast + 2 slow => 50% slow rate.
	for i := 0; i < 2; i++ {
		waitResult(t, execute(dep, context.Background(), okFn))
	}
	stub := NewFaultyDependency(clk)
	stub.SetSlow(200 * time.Millisecond)
	for i := 0; i < 2; i++ {
		ch := execute(dep, context.Background(), func(ctx context.Context) (int, error) {
			return 0, stub.Call(ctx)
		})
		pollUntil(t, func() bool { return stub.InFlight() == 1 })
		settle()
		clk.Advance(200 * time.Millisecond)
		if waitResult(t, ch).err != nil {
			t.Fatalf("slow call should succeed: %v", waitResult(t, ch).err)
		}
	}
	if dep.State() != StateOpen {
		t.Fatalf("want open at 50%% slow rate, got %s", dep.State())
	}
}

func TestSlidingWindowExpiry(t *testing.T) {
	clk := newTestClock()
	dep := NewDependency[int]("h",
		WithClock(clk),
		WithConsecutiveFailures(5),
		WithMinimumCalls(4),
		WithFailureRateThreshold(50),
		WithWindow(10*time.Second, 10),
		WithTimeout(0),
	)
	// 3 success + 1 failure: below thresholds.
	for i := 0; i < 3; i++ {
		waitResult(t, execute(dep, context.Background(), okFn))
	}
	waitResult(t, execute(dep, context.Background(), errFn))
	if dep.State() != StateClosed {
		t.Fatalf("should stay closed below threshold, got %s", dep.State())
	}
	// Age the whole window out.
	clk.Advance(11 * time.Second)
	// One failure in the fresh window: below minimum calls, no open.
	waitResult(t, execute(dep, context.Background(), errFn))
	if dep.State() != StateClosed {
		t.Fatalf("expired window must not open, got %s", dep.State())
	}
	m := dep.Metrics()
	if m.TotalCalls != 1 || m.FailedCalls != 1 {
		t.Fatalf("want fresh window with 1 failure, got %+v", m)
	}
}

// ---------- bulkhead ----------

func TestBulkheadBoundsConcurrencyAndQueue(t *testing.T) {
	clk := newTestClock()
	dep := NewDependency[int]("h",
		WithClock(clk),
		WithConsecutiveFailures(100),
		WithMinimumCalls(1000),
		WithBulkhead(2, 1),
		WithTimeout(0),
	)
	block := make(chan struct{})
	fn := func(ctx context.Context) (int, error) {
		<-block
		return 42, nil
	}
	c1 := execute(dep, context.Background(), fn)
	c2 := execute(dep, context.Background(), fn)
	pollUntil(t, func() bool {
		inFlight, _ := dep.BulkheadStats()
		return inFlight == 2
	})
	// Third call queues; fourth is rejected immediately.
	c3 := execute(dep, context.Background(), fn)
	pollUntil(t, func() bool {
		_, waiters := dep.BulkheadStats()
		return waiters == 1
	})
	r4 := waitResult(t, execute(dep, context.Background(), okFn))
	if !errors.Is(r4.err, ErrBulkheadFull) {
		t.Fatalf("want ErrBulkheadFull, got %v", r4.err)
	}
	close(block)
	for _, c := range []<-chan result{c1, c2, c3} {
		r := waitResult(t, c)
		if r.err != nil {
			t.Fatalf("unexpected error: %v", r.err)
		}
	}
}

// ---------- timeout / cancel / retry ----------

func TestTimeoutCountedAsFailure(t *testing.T) {
	clk := newTestClock()
	dep := NewDependency[int]("h",
		WithClock(clk),
		WithConsecutiveFailures(100),
		WithMinimumCalls(1000),
		WithTimeout(1*time.Second),
		WithBulkhead(100, 0),
	)
	stub := NewFaultyDependency(clk)
	stub.SetSlow(2 * time.Second)
	ch := execute(dep, context.Background(), func(ctx context.Context) (int, error) {
		return 0, stub.Call(ctx)
	})
	pollUntil(t, func() bool { return stub.InFlight() == 1 })
	clk.Advance(1 * time.Second)
	r := waitResult(t, ch)
	if !errors.Is(r.err, ErrTimeout) {
		t.Fatalf("want ErrTimeout, got %v", r.err)
	}
	m := dep.Metrics()
	if m.FailedCalls != 1 {
		t.Fatalf("timeout must count as one failure, got %+v", m)
	}
}

func TestCancelNotCounted(t *testing.T) {
	clk := newTestClock()
	dep := NewDependency[int]("h",
		WithClock(clk),
		WithConsecutiveFailures(100),
		WithMinimumCalls(1000),
		WithTimeout(0),
	)
	block := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	ch := execute(dep, ctx, func(ctx context.Context) (int, error) {
		<-block
		return 42, nil
	})
	cancel()
	r := waitResult(t, ch)
	if !errors.Is(r.err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", r.err)
	}
	m := dep.Metrics()
	if m.TotalCalls != 0 || m.FailedCalls != 0 {
		t.Fatalf("cancellation must not be counted, got %+v", m)
	}
}

func TestRetryReportsSingleOutcome(t *testing.T) {
	clk := newTestClock()
	dep := NewDependency[int]("h",
		WithClock(clk),
		WithConsecutiveFailures(100),
		WithMinimumCalls(1000),
		WithTimeout(0),
		WithRetry(3, 100*time.Millisecond, 1*time.Second),
		WithRetryable(func(error) bool { return true }),
	)
	var attempts int64
	fn := func(ctx context.Context) (int, error) {
		n := atomic.AddInt64(&attempts, 1)
		if n < 3 {
			return 0, ErrStub
		}
		return 42, nil
	}
	ch := execute(dep, context.Background(), fn)
	pollUntil(t, func() bool { return atomic.LoadInt64(&attempts) == 1 })
	settle()
	clk.Advance(100 * time.Millisecond)
	pollUntil(t, func() bool { return atomic.LoadInt64(&attempts) == 2 })
	settle()
	clk.Advance(1 * time.Second)
	r := waitResult(t, ch)
	if r.err != nil {
		t.Fatalf("unexpected error: %v", r.err)
	}
	if r.v != 42 {
		t.Fatalf("want 42, got %d", r.v)
	}
	if atomic.LoadInt64(&attempts) != 3 {
		t.Fatalf("want 3 attempts, got %d", attempts)
	}
	m := dep.Metrics()
	if m.TotalCalls != 1 || m.SuccessfulCalls != 1 || m.FailedCalls != 0 {
		t.Fatalf("retries must report one logical outcome, got %+v", m)
	}
}

func TestRetryExhaustedReportsOneFailure(t *testing.T) {
	clk := newTestClock()
	dep := NewDependency[int]("h",
		WithClock(clk),
		WithConsecutiveFailures(100),
		WithMinimumCalls(1000),
		WithTimeout(0),
		WithRetry(3, 100*time.Millisecond, 1*time.Second),
		WithRetryable(func(error) bool { return true }),
	)
	var attempts int64
	fn := func(ctx context.Context) (int, error) {
		atomic.AddInt64(&attempts, 1)
		return 0, ErrStub
	}
	ch := execute(dep, context.Background(), fn)
	for i := 0; i < 2; i++ {
		pollUntil(t, func() bool { return atomic.LoadInt64(&attempts) == int64(i+1) })
		settle()
		clk.Advance(1 * time.Second)
	}
	r := waitResult(t, ch)
	if !errors.Is(r.err, ErrStub) {
		t.Fatalf("want ErrStub, got %v", r.err)
	}
	if atomic.LoadInt64(&attempts) != 3 {
		t.Fatalf("want 3 attempts, got %d", attempts)
	}
	m := dep.Metrics()
	if m.TotalCalls != 1 || m.FailedCalls != 1 {
		t.Fatalf("exhausted retries must count one failure, got %+v", m)
	}
}

func TestCancelDuringBackoffNotCounted(t *testing.T) {
	clk := newTestClock()
	dep := NewDependency[int]("h",
		WithClock(clk),
		WithConsecutiveFailures(100),
		WithMinimumCalls(1000),
		WithTimeout(0),
		WithRetry(3, 100*time.Millisecond, 1*time.Second),
		WithRetryable(func(error) bool { return true }),
	)
	var attempts int64
	fn := func(ctx context.Context) (int, error) {
		atomic.AddInt64(&attempts, 1)
		return 0, ErrStub
	}
	ctx, cancel := context.WithCancel(context.Background())
	ch := execute(dep, ctx, fn)
	pollUntil(t, func() bool { return atomic.LoadInt64(&attempts) == 1 })
	cancel()
	r := waitResult(t, ch)
	if !errors.Is(r.err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", r.err)
	}
	m := dep.Metrics()
	if m.TotalCalls != 0 {
		t.Fatalf("cancellation during backoff must not be counted, got %+v", m)
	}
}

// ---------- fallback ----------

func TestFallbackOnOpen(t *testing.T) {
	clk := newTestClock()
	dep := NewDependency[int]("h",
		WithClock(clk),
		WithConsecutiveFailures(1),
		WithMinimumCalls(1000),
		WithTimeout(0),
		WithFallback(func(_ context.Context, err error) (int, error) {
			if !errors.Is(err, ErrCircuitOpen) {
				return 0, fmt.Errorf("fallback cause want ErrCircuitOpen, got %v", err)
			}
			return 99, nil
		}),
	)
	waitResult(t, execute(dep, context.Background(), errFn))
	r := waitResult(t, execute(dep, context.Background(), okFn))
	if r.err != nil {
		t.Fatalf("fallback should succeed: %v", r.err)
	}
	if r.v != 99 {
		t.Fatalf("want fallback value 99, got %d", r.v)
	}
}

func TestFallbackTimeoutDoesNotHang(t *testing.T) {
	clk := newTestClock()
	dep := NewDependency[int]("h",
		WithClock(clk),
		WithConsecutiveFailures(1),
		WithMinimumCalls(1000),
		WithTimeout(0),
		WithFallbackTimeout(100*time.Millisecond),
		WithFallback(func(ctx context.Context, err error) (int, error) {
			if errors.Is(err, ErrCircuitOpen) {
				<-ctx.Done() // hang only when the breaker is open
			}
			return 0, nil
		}),
	)
	waitResult(t, execute(dep, context.Background(), errFn))
	ch := execute(dep, context.Background(), okFn)
	settle()
	clk.Advance(100 * time.Millisecond)
	r := waitResult(t, ch)
	if !errors.Is(r.err, ErrFallbackTimeout) {
		t.Fatalf("want ErrFallbackTimeout, got %v", r.err)
	}
}

// ---------- clock ----------

func TestClockBackwardsNoSpuriousOpen(t *testing.T) {
	clk := newTestClock()
	dep := NewDependency[int]("h",
		WithClock(clk),
		WithConsecutiveFailures(5),
		WithMinimumCalls(1000),
		WithTimeout(0),
	)
	for i := 0; i < 3; i++ {
		waitResult(t, execute(dep, context.Background(), errFn))
	}
	if dep.State() != StateClosed {
		t.Fatalf("should stay closed, got %s", dep.State())
	}
	// Wall clock rolls back: window must reset, no spurious opening.
	clk.Set(clk.Now().Add(-1 * time.Hour))
	waitResult(t, execute(dep, context.Background(), okFn))
	if dep.State() != StateClosed {
		t.Fatalf("clock rollback must not open the breaker, got %s", dep.State())
	}
	m := dep.Metrics()
	if m.TotalCalls != 1 || m.SuccessfulCalls != 1 {
		t.Fatalf("want fresh window after rollback, got %+v", m)
	}
}

func TestClockJumpRecovery(t *testing.T) {
	clk := newTestClock()
	dep := NewDependency[int]("h",
		WithClock(clk),
		WithConsecutiveFailures(1),
		WithMinimumCalls(1000),
		WithOpenTimeout(10*time.Second),
		WithHalfOpen(1, 1, 1),
		WithTimeout(0),
	)
	waitResult(t, execute(dep, context.Background(), errFn))
	if dep.State() != StateOpen {
		t.Fatalf("want open, got %s", dep.State())
	}
	// Jump backwards: breaker must not recover early.
	clk.Set(clk.Now().Add(-1 * time.Hour))
	if dep.State() != StateOpen {
		t.Fatalf("breaker must stay open after backwards jump, got %s", dep.State())
	}
	// Jump forward past the open timeout: half-open.
	clk.Set(clk.Now().Add(2 * time.Hour))
	if dep.State() != StateHalfOpen {
		t.Fatalf("want half-open after forward jump, got %s", dep.State())
	}
}

// ---------- concurrency ----------

func TestConcurrentHalfOpenNoOverIssue(t *testing.T) {
	clk := newTestClock()
	dep := NewDependency[int]("h",
		WithClock(clk),
		WithConsecutiveFailures(1),
		WithMinimumCalls(1000),
		WithOpenTimeout(10*time.Second),
		WithHalfOpen(5, 2, 3),
		WithTimeout(0),
		WithBulkhead(100, 100),
	)
	waitResult(t, execute(dep, context.Background(), errFn))
	clk.Advance(10*time.Second + time.Nanosecond)

	var active int64
	var maxActive int64
	block := make(chan struct{})
	resultCh := make(chan result, 20)
	fn := func(ctx context.Context) (int, error) {
		n := atomic.AddInt64(&active, 1)
		for {
			m := atomic.LoadInt64(&maxActive)
			if n <= m || atomic.CompareAndSwapInt64(&maxActive, m, n) {
				break
			}
		}
		<-block
		atomic.AddInt64(&active, -1)
		return 42, nil
	}

	for i := 0; i < 20; i++ {
		go func() {
			v, err := dep.Execute(context.Background(), fn)
			resultCh <- result{v: v, err: err}
		}()
	}
	// While the block is held, exactly 2 probes run and the other 18 are
	// rejected immediately.
	pollUntil(t, func() bool {
		return atomic.LoadInt64(&active) == 2
	})
	pollUntil(t, func() bool {
		return len(resultCh) == 18
	})
	close(block)

	var rejected, succeeded int
	for i := 0; i < 20; i++ {
		select {
		case r := <-resultCh:
			if r.err != nil {
				if !errors.Is(r.err, ErrHalfOpenBusy) {
					t.Fatalf("unexpected error: %v", r.err)
				}
				rejected++
			} else {
				succeeded++
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for result")
		}
	}

	if maxActive > 2 {
		t.Fatalf("half-open concurrency exceeded: max in-flight %d", maxActive)
	}
	if succeeded != 2 {
		t.Fatalf("want exactly 2 probe successes, got %d", succeeded)
	}
	if rejected != 18 {
		t.Fatalf("want 18 rejections, got %d", rejected)
	}
}

// ---------- additional edge cases ----------

func TestSlowCallCountedInMetrics(t *testing.T) {
	clk := newTestClock()
	dep := NewDependency[int]("h",
		WithClock(clk),
		WithConsecutiveFailures(100),
		WithMinimumCalls(1000),
		WithSlowCallThreshold(100*time.Millisecond),
		WithTimeout(0),
	)
	stub := NewFaultyDependency(clk)
	stub.SetSlow(200 * time.Millisecond)
	ch := execute(dep, context.Background(), func(ctx context.Context) (int, error) {
		return 0, stub.Call(ctx)
	})
	pollUntil(t, func() bool { return stub.InFlight() == 1 })
	settle()
	clk.Advance(200 * time.Millisecond)
	r := waitResult(t, ch)
	if r.err != nil {
		t.Fatalf("slow call should succeed: %v", r.err)
	}
	m := dep.Metrics()
	if m.TotalCalls != 1 || m.SuccessfulCalls != 1 || m.SlowCalls != 1 {
		t.Fatalf("want 1 slow successful call, got %+v", m)
	}
}

func TestBulkheadRejectionNotCountedAsFailure(t *testing.T) {
	clk := newTestClock()
	dep := NewDependency[int]("h",
		WithClock(clk),
		WithConsecutiveFailures(100),
		WithMinimumCalls(1000),
		WithBulkhead(1, 0),
		WithTimeout(0),
	)
	block := make(chan struct{})
	fn := func(ctx context.Context) (int, error) {
		<-block
		return 42, nil
	}
	c1 := execute(dep, context.Background(), fn)
	pollUntil(t, func() bool {
		inFlight, _ := dep.BulkheadStats()
		return inFlight == 1
	})
	// Second call rejected: must not count as failure.
	r := waitResult(t, execute(dep, context.Background(), okFn))
	if !errors.Is(r.err, ErrBulkheadFull) {
		t.Fatalf("want ErrBulkheadFull, got %v", r.err)
	}
	close(block)
	waitResult(t, c1)
	m := dep.Metrics()
	if m.FailedCalls != 0 {
		t.Fatalf("bulkhead rejection must not count as failure, got %+v", m)
	}
	if m.RejectedCalls != 1 {
		t.Fatalf("want 1 rejection, got %d", m.RejectedCalls)
	}
	if dep.State() != StateClosed {
		t.Fatalf("bulkhead rejection must not open the breaker, got %s", dep.State())
	}
}

func TestTimeoutRetry(t *testing.T) {
	clk := newTestClock()
	dep := NewDependency[int]("h",
		WithClock(clk),
		WithConsecutiveFailures(100),
		WithMinimumCalls(1000),
		WithTimeout(1*time.Second),
		WithRetry(2, 100*time.Millisecond, 1*time.Second),
		WithBulkhead(100, 0),
	)
	stub := NewFaultyDependency(clk)
	stub.SetSlow(2 * time.Second)
	ch := execute(dep, context.Background(), func(ctx context.Context) (int, error) {
		return 0, stub.Call(ctx)
	})
	pollUntil(t, func() bool { return stub.InFlight() == 1 })
	clk.Advance(1 * time.Second) // first attempt times out
	settle()
	clk.Advance(100 * time.Millisecond) // retry backoff
	settle()
	clk.Advance(1 * time.Second) // second attempt times out
	r := waitResult(t, ch)
	if !errors.Is(r.err, ErrTimeout) {
		t.Fatalf("want ErrTimeout after retries, got %v", r.err)
	}
	m := dep.Metrics()
	if m.FailedCalls != 1 {
		t.Fatalf("retried timeout must count exactly one failure, got %+v", m)
	}
}

func TestDependenciesAreIsolated(t *testing.T) {
	clk := newTestClock()
	a := NewDependency[int]("a",
		WithClock(clk),
		WithConsecutiveFailures(1),
		WithMinimumCalls(1000),
		WithTimeout(0),
	)
	b := NewDependency[int]("b",
		WithClock(clk),
		WithConsecutiveFailures(1),
		WithMinimumCalls(1000),
		WithTimeout(0),
	)
	waitResult(t, execute(a, context.Background(), errFn))
	if a.State() != StateOpen {
		t.Fatalf("dependency a should be open, got %s", a.State())
	}
	if b.State() != StateClosed {
		t.Fatalf("dependency b must stay closed, got %s", b.State())
	}
	r := waitResult(t, execute(b, context.Background(), okFn))
	if r.err != nil {
		t.Fatalf("dependency b should be unaffected: %v", r.err)
	}
}

func TestOpenTimerTransitionsWithoutTraffic(t *testing.T) {
	clk := newTestClock()
	dep := NewDependency[int]("h",
		WithClock(clk),
		WithConsecutiveFailures(1),
		WithMinimumCalls(1000),
		WithOpenTimeout(10*time.Second),
		WithTimeout(0),
	)
	waitResult(t, execute(dep, context.Background(), errFn))
	if dep.State() != StateOpen {
		t.Fatalf("want open, got %s", dep.State())
	}
	// Advance without any call: the open timer drives the transition.
	clk.Advance(10*time.Second + time.Nanosecond)
	pollUntil(t, func() bool { return dep.State() == StateHalfOpen })
}

func TestHalfOpenProbesReleasedAsSlotsFree(t *testing.T) {
	clk := newTestClock()
	dep := NewDependency[int]("h",
		WithClock(clk),
		WithConsecutiveFailures(1),
		WithMinimumCalls(1000),
		WithOpenTimeout(10*time.Second),
		WithHalfOpen(3, 1, 2), // 3 permits, 1 concurrent, 2 successes
		WithTimeout(0),
	)
	waitResult(t, execute(dep, context.Background(), errFn))
	clk.Advance(10*time.Second + time.Nanosecond)

	block1 := make(chan struct{})
	block2 := make(chan struct{})
	mkFn := func(block <-chan struct{}) func(context.Context) (int, error) {
		return func(ctx context.Context) (int, error) {
			<-block
			return 42, nil
		}
	}
	c1 := execute(dep, context.Background(), mkFn(block1))
	pollUntil(t, func() bool { return dep.Metrics().HalfOpenInFlight == 1 })
	// Second call: concurrency full -> rejected (permit not consumed).
	r := waitResult(t, execute(dep, context.Background(), okFn))
	if !errors.Is(r.err, ErrHalfOpenBusy) {
		t.Fatalf("want ErrHalfOpenBusy, got %v", r.err)
	}
	close(block1)
	if waitResult(t, c1).err != nil {
		t.Fatalf("probe 1 failed")
	}
	// Slot freed: next call uses the second permit.
	c2 := execute(dep, context.Background(), mkFn(block2))
	pollUntil(t, func() bool { return dep.Metrics().HalfOpenInFlight == 1 })
	close(block2)
	if waitResult(t, c2).err != nil {
		t.Fatalf("probe 2 failed")
	}
	if dep.State() != StateClosed {
		t.Fatalf("want closed after 2 successes, got %s", dep.State())
	}
}

func TestConfigValidation(t *testing.T) {
	clk := newTestClock()
	cases := []Option{
		WithFailureRateThreshold(0),
		WithSlowCallRateThreshold(200),
		WithMinimumCalls(0),
		WithWindow(0, 10),
		WithWindow(10*time.Second, 0),
		WithOpenTimeout(-1),
		WithHalfOpen(0, 1, 1),
		WithHalfOpen(3, 1, 5),
		WithBulkhead(0, 0),
		WithBulkhead(1, -1),
		WithTimeout(-1),
		WithRetry(0, 100*time.Millisecond, time.Second),
		WithRetry(2, 1*time.Second, 100*time.Millisecond),
		WithFallbackTimeout(0),
	}
	for i, opt := range cases {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("case %d: expected panic for invalid config", i)
				}
			}()
			NewDependency[int]("x", WithClock(clk), opt)
		}()
	}
}

func TestStateString(t *testing.T) {
	if StateClosed.String() != "closed" {
		t.Fatalf("bad string: %s", StateClosed)
	}
	if StateOpen.String() != "open" {
		t.Fatalf("bad string: %s", StateOpen)
	}
	if StateHalfOpen.String() != "half-open" {
		t.Fatalf("bad string: %s", StateHalfOpen)
	}
}
