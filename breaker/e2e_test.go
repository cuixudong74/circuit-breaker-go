package breaker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cuixudong74/circuit-breaker-go/stub"
)

// 端到端：依赖连续报错 -> 熔断打开 -> 降级 -> 到期半开试探 -> 依赖恢复 -> 关闭。
// 全程进程内运行，使用假时钟，结果可重复。
func TestEndToEndFailureAndRecovery(t *testing.T) {
	clk := NewFakeClock(t0)
	cfg := execConfig()
	cfg.Timeout = 0
	cfg.ConsecutiveFailures = 2
	cfg.OpenDuration = 5 * time.Second
	cfg.HalfOpenSuccesses = 2
	cfg.HalfOpenMaxConcurrent = 1
	br := New(cfg, clk)

	var fbReasons []FallbackReason
	ex := NewExecutor(br, func(ctx context.Context, reason FallbackReason, err error) (any, error) {
		fbReasons = append(fbReasons, reason)
		return "degraded", nil
	})

	dep := stub.New()
	depErr := errors.New("dep down")
	dep.Set(stub.Behavior{Err: depErr})

	// 1. 依赖报错：第一次失败走降级(failure)，第二次失败后熔断打开
	ex.Do(context.Background(), dep.Call)
	ex.Do(context.Background(), dep.Call)
	if br.State() != StateOpen {
		t.Fatalf("state = %v, want open", br.State())
	}

	// 2. 打开期间：快速降级(open)，不再打到依赖
	callsBefore := dep.Calls()
	r := ex.Do(context.Background(), dep.Call)
	if r.Value != "degraded" || r.Reason != ReasonOpen {
		t.Fatalf("got %+v, want open degraded", r)
	}
	if dep.Calls() != callsBefore {
		t.Fatal("打开期间不应调用依赖")
	}

	// 3. 到期后进入半开：依赖已恢复，试探成功
	clk.Advance(5 * time.Second)
	dep.Set(stub.Behavior{}) // 依赖恢复
	r = ex.Do(context.Background(), dep.Call)
	if r.Err != nil || r.Reason != ReasonNone {
		t.Fatalf("第一次试探应成功, got %+v", r)
	}
	if br.State() != StateHalfOpen {
		t.Fatalf("state = %v, want half-open (成功数未达 2)", br.State())
	}

	// 4. 第二次试探成功 -> 关闭，后续全量放行
	r = ex.Do(context.Background(), dep.Call)
	if r.Err != nil {
		t.Fatalf("第二次试探应成功, got %+v", r)
	}
	if br.State() != StateClosed {
		t.Fatalf("state = %v, want closed", br.State())
	}
	r = ex.Do(context.Background(), dep.Call)
	if r.Value != "ok" || r.Reason != ReasonNone {
		t.Fatalf("恢复后应直通, got %+v", r)
	}

	// 降级原因可区分：failure(第一次) + failure(第二次) + open(打开期间)
	want := []FallbackReason{ReasonFailure, ReasonFailure, ReasonOpen}
	if len(fbReasons) != len(want) {
		t.Fatalf("fallback reasons = %v, want %v", fbReasons, want)
	}
	for i := range want {
		if fbReasons[i] != want[i] {
			t.Fatalf("fallback reasons = %v, want %v", fbReasons, want)
		}
	}
}

// 端到端：依赖变慢 -> 慢调用比例触发熔断 -> 到期后试探恢复。
// 慢调用耗时由熔断器时钟度量，本用例使用真实时钟与真实短延迟。
func TestEndToEndSlowDependency(t *testing.T) {
	cfg := execConfig()
	cfg.Timeout = 0
	cfg.ConsecutiveFailures = 100 // 只用慢调用比例路径
	cfg.MinRequests = 3
	cfg.SlowThreshold = 50 * time.Millisecond
	cfg.SlowRatio = 0.5
	cfg.OpenDuration = 100 * time.Millisecond
	cfg.HalfOpenSuccesses = 1
	br := New(cfg, nil) // 真实时钟
	ex := NewExecutor(br, func(ctx context.Context, reason FallbackReason, err error) (any, error) {
		return "degraded", nil
	})

	dep := stub.New()
	dep.Set(stub.Behavior{Latency: 80 * time.Millisecond}) // 慢但成功

	// 3 次慢调用 -> 慢比例 1.0 >= 0.5 打开
	for i := 0; i < 3; i++ {
		ex.Do(context.Background(), dep.Call)
	}
	if br.State() != StateOpen {
		t.Fatalf("state = %v, want open (慢调用比例触发)", br.State())
	}

	// 到期后试探：依赖已恢复正常速度 -> 成功并关闭
	time.Sleep(150 * time.Millisecond)
	dep.Set(stub.Behavior{})
	r := ex.Do(context.Background(), dep.Call)
	if r.Err != nil {
		t.Fatalf("试探成功不应报错, got %+v", r)
	}
	if br.State() != StateClosed {
		t.Fatalf("state = %v, want closed", br.State())
	}
}
