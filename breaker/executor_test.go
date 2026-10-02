package breaker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cuixudong74/circuit-breaker-go/stub"
)

func execConfig() Config {
	return Config{
		Window:                10 * time.Second,
		Bucket:                10,
		MinRequests:           2,
		FailureRatio:          0.5,
		SlowThreshold:         500 * time.Millisecond,
		SlowRatio:             0.9,
		ConsecutiveFailures:   2,
		OpenDuration:          5 * time.Second,
		HalfOpenMaxConcurrent: 1,
		HalfOpenSuccesses:     1,
		MaxConcurrent:         4,
		Timeout:               50 * time.Millisecond,
		MaxAttempts:           1,
	}
}

// 正常调用直通，不触发降级。
func TestDoSuccess(t *testing.T) {
	dep := stub.New()
	ex := NewExecutor(New(execConfig(), nil), nil)
	r := ex.Do(context.Background(), dep.Call)
	if r.Err != nil || r.Value != "ok" || r.Reason != ReasonNone {
		t.Fatalf("got %+v, want ok/none", r)
	}
}

// 依赖挂起 -> 单次尝试超时 -> 走降级，原因可区分。
func TestDoTimeoutFallback(t *testing.T) {
	dep := stub.New()
	dep.Set(stub.Behavior{Hang: true})
	ex := NewExecutor(New(execConfig(), nil), func(ctx context.Context, reason FallbackReason, err error) (any, error) {
		return "degraded:" + reason.String(), nil
	})
	start := time.Now()
	r := ex.Do(context.Background(), dep.Call)
	if r.Err != nil || r.Value != "degraded:timeout" || r.Reason != ReasonTimeout {
		t.Fatalf("got %+v, want timeout fallback", r)
	}
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Fatalf("超时控制未生效, 耗时 %v", d)
	}
}

// 调用方取消：立即返回 ErrCanceled，不污染熔断计数，不触发降级。
func TestDoCancelNotCounted(t *testing.T) {
	dep := stub.New()
	dep.Set(stub.Behavior{Hang: true})
	br := New(execConfig(), nil)
	var fallbackCalled atomic.Bool
	ex := NewExecutor(br, func(ctx context.Context, reason FallbackReason, err error) (any, error) {
		fallbackCalled.Store(true)
		return nil, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan Result, 1)
	go func() { done <- ex.Do(ctx, dep.Call) }()
	time.Sleep(10 * time.Millisecond)
	cancel()
	r := <-done
	if !errors.Is(r.Err, ErrCanceled) {
		t.Fatalf("err = %v, want ErrCanceled", r.Err)
	}
	if fallbackCalled.Load() {
		t.Fatal("取消不应触发降级")
	}
	// 取消不计数：连续取消多次后熔断器仍关闭
	for i := 0; i < 5; i++ {
		c2, cancel2 := context.WithCancel(context.Background())
		cancel2()
		ex.Do(c2, dep.Call)
	}
	if br.State() != StateClosed {
		t.Fatalf("state = %v, want closed (取消不污染计数)", br.State())
	}
}

// 重试：一次逻辑调用多次尝试，只向熔断器反馈一个最终结果。
func TestRetrySingleReport(t *testing.T) {
	clk := NewFakeClock(t0)
	cfg := execConfig()
	cfg.Timeout = 0
	cfg.MaxAttempts = 3
	cfg.RetryBackoff = 0        // 假时钟不做退避等待，避免测试阻塞
	cfg.ConsecutiveFailures = 2 // 若重复计数，2 次失败即打开
	br := New(cfg, clk)
	ex := NewExecutor(br, nil)

	depErr := errors.New("dep down")
	var attempts atomic.Int64
	r := ex.Do(context.Background(), func(ctx context.Context) (any, error) {
		attempts.Add(1)
		return nil, depErr
	})
	if !errors.Is(r.Err, depErr) {
		t.Fatalf("err = %v, want depErr", r.Err)
	}
	if attempts.Load() != 3 {
		t.Fatalf("attempts = %d, want 3", attempts.Load())
	}
	// 只计一次失败：连续失败计数为 1，未达阈值 2
	if br.State() != StateClosed {
		t.Fatalf("state = %v, want closed (一次逻辑调用只计一次)", br.State())
	}
	// 再来一次逻辑调用（再次失败）才达到连续失败阈值
	ex.Do(context.Background(), func(ctx context.Context) (any, error) { return nil, depErr })
	if br.State() != StateOpen {
		t.Fatalf("state = %v, want open", br.State())
	}
}

// 重试成功：前两次失败第三次成功，最终计为成功。
func TestRetryEventuallySucceeds(t *testing.T) {
	dep := stub.New()
	dep.SetScript([]stub.Behavior{
		{Err: errors.New("x")},
		{Err: errors.New("x")},
		{},
	})
	cfg := execConfig()
	cfg.Timeout = 0
	cfg.MaxAttempts = 3
	br := New(cfg, nil)
	ex := NewExecutor(br, nil)
	r := ex.Do(context.Background(), dep.Call)
	if r.Err != nil || r.Value != "ok" {
		t.Fatalf("got %+v, want ok", r)
	}
	if dep.Calls() != 3 {
		t.Fatalf("calls = %d, want 3", dep.Calls())
	}
	if br.State() != StateClosed {
		t.Fatalf("state = %v, want closed", br.State())
	}
}

// 熔断打开后快速失败并降级，原因可区分，且不再打到依赖。
func TestOpenFallbackSkipsDependency(t *testing.T) {
	dep := stub.New()
	depErr := errors.New("dep down")
	dep.Set(stub.Behavior{Err: depErr})
	cfg := execConfig()
	cfg.Timeout = 0
	cfg.ConsecutiveFailures = 2
	br := New(cfg, nil)
	ex := NewExecutor(br, func(ctx context.Context, reason FallbackReason, err error) (any, error) {
		return "fb:" + reason.String(), nil
	})

	ex.Do(context.Background(), dep.Call)
	ex.Do(context.Background(), dep.Call) // 连续失败 2 次 -> 打开
	if br.State() != StateOpen {
		t.Fatal("want open")
	}
	callsBefore := dep.Calls()
	r := ex.Do(context.Background(), dep.Call)
	if r.Value != "fb:open" || r.Reason != ReasonOpen {
		t.Fatalf("got %+v, want open fallback", r)
	}
	if dep.Calls() != callsBefore {
		t.Fatal("打开状态下不应再调用依赖")
	}
}

// 降级函数自身失败：错误透传，调用方不被挂死。
func TestFallbackFailurePropagates(t *testing.T) {
	dep := stub.New()
	dep.Set(stub.Behavior{Err: errors.New("dep down")})
	cfg := execConfig()
	cfg.Timeout = 0
	fbErr := errors.New("fallback failed")
	ex := NewExecutor(New(cfg, nil), func(ctx context.Context, reason FallbackReason, err error) (any, error) {
		return nil, fbErr
	})
	done := make(chan Result, 1)
	go func() { done <- ex.Do(context.Background(), dep.Call) }()
	select {
	case r := <-done:
		if !errors.Is(r.Err, fbErr) || r.Reason != ReasonFailure {
			t.Fatalf("got %+v, want fallback error", r)
		}
	case <-time.After(time.Second):
		t.Fatal("降级失败不应挂死调用方")
	}
}

// 舱壁隔离：单个依赖并发在途受限，超额请求快速降级。
func TestBulkheadIsolation(t *testing.T) {
	dep := stub.New()
	dep.Set(stub.Behavior{Latency: 100 * time.Millisecond})
	cfg := execConfig()
	cfg.Timeout = 0
	cfg.MaxConcurrent = 2
	ex := NewExecutor(New(cfg, nil), func(ctx context.Context, reason FallbackReason, err error) (any, error) {
		return "fb:" + reason.String(), nil
	})

	var wg sync.WaitGroup
	results := make(chan Result, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- ex.Do(context.Background(), dep.Call)
		}()
	}
	wg.Wait()
	close(results)
	var ok, bulkhead int
	for r := range results {
		switch {
		case r.Reason == ReasonNone && r.Err == nil:
			ok++
		case r.Reason == ReasonBulkheadFull:
			bulkhead++
		default:
			t.Errorf("unexpected result %+v", r)
		}
	}
	if ok != 2 || bulkhead != 6 {
		t.Fatalf("ok=%d bulkhead=%d, want 2/6", ok, bulkhead)
	}
}

// 分组隔离：一个依赖熔断不影响另一个依赖。
func TestGroupIsolation(t *testing.T) {
	depA := stub.New()
	depA.Set(stub.Behavior{Err: errors.New("a down")})
	depB := stub.New()
	cfg := execConfig()
	cfg.Timeout = 0
	cfg.ConsecutiveFailures = 2
	exA := NewExecutor(New(cfg, nil), nil)
	exB := NewExecutor(New(cfg, nil), nil)

	exA.Do(context.Background(), depA.Call)
	exA.Do(context.Background(), depA.Call)
	if st := exA.br.State(); st != StateOpen {
		t.Fatalf("A state = %v, want open", st)
	}
	r := exB.Do(context.Background(), depB.Call)
	if r.Err != nil || r.Value != "ok" {
		t.Fatalf("B 不应受 A 影响, got %+v", r)
	}
}
