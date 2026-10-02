package breaker

import (
	"testing"
	"time"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func testConfig() Config {
	return Config{
		Window:                10 * time.Second,
		Bucket:                10,
		MinRequests:           4,
		FailureRatio:          0.5,
		SlowThreshold:         100 * time.Millisecond,
		SlowRatio:             0.5,
		ConsecutiveFailures:   3,
		OpenDuration:          5 * time.Second,
		HalfOpenMaxConcurrent: 2,
		HalfOpenSuccesses:     2,
		MaxConcurrent:         10,
		MaxAttempts:           1,
	}
}

// 连续失败达到阈值 -> 打开。
func TestConsecutiveFailuresOpens(t *testing.T) {
	clk := NewFakeClock(t0)
	b := New(testConfig(), clk)
	for i := 0; i < 3; i++ {
		b.report(time.Millisecond, true, false)
	}
	if got := b.State(); got != StateOpen {
		t.Fatalf("state = %v, want open", got)
	}
	if _, err := b.acquire(); err != ErrOpen {
		t.Fatalf("acquire err = %v, want ErrOpen", err)
	}
}

// 成功会清零连续失败计数。
func TestSuccessResetsConsecutive(t *testing.T) {
	clk := NewFakeClock(t0)
	cfg := testConfig()
	cfg.FailureRatio = 1.0 // 隔离比例路径，只验证连续失败计数
	cfg.SlowRatio = 1.0
	b := New(cfg, clk)
	b.report(time.Millisecond, true, false)
	b.report(time.Millisecond, true, false)
	b.report(time.Millisecond, false, false) // 清零
	b.report(time.Millisecond, true, false)
	b.report(time.Millisecond, true, false)
	if got := b.State(); got != StateClosed {
		t.Fatalf("state = %v, want closed (连续失败被成功打断)", got)
	}
}

// 窗口失败率超阈值 -> 打开（连续失败未达阈值时）。
func TestFailureRatioOpens(t *testing.T) {
	clk := NewFakeClock(t0)
	cfg := testConfig()
	cfg.ConsecutiveFailures = 100 // 禁用连续失败路径
	b := New(cfg, clk)
	// 4 个请求 3 个失败，失败率 0.75 >= 0.5
	b.report(time.Millisecond, true, false)
	b.report(time.Millisecond, false, false)
	b.report(time.Millisecond, true, false)
	if got := b.State(); got != StateClosed {
		t.Fatalf("提前打开")
	}
	b.report(time.Millisecond, true, false) // total=4 触发判定
	if got := b.State(); got != StateOpen {
		t.Fatalf("state = %v, want open", got)
	}
}

// 慢调用比例超阈值 -> 打开（即使全部成功）。
func TestSlowRatioOpens(t *testing.T) {
	clk := NewFakeClock(t0)
	cfg := testConfig()
	cfg.ConsecutiveFailures = 100
	b := New(cfg, clk)
	for i := 0; i < 4; i++ {
		b.report(200*time.Millisecond, false, false) // 全部成功但都是慢调用
	}
	if got := b.State(); got != StateOpen {
		t.Fatalf("state = %v, want open", got)
	}
}

// 窗口内请求数不足 MinRequests 时不触发比例判定。
func TestMinRequestsGate(t *testing.T) {
	clk := NewFakeClock(t0)
	cfg := testConfig()
	cfg.ConsecutiveFailures = 100
	b := New(cfg, clk)
	b.report(time.Millisecond, true, false)
	b.report(time.Millisecond, true, false)
	b.report(time.Millisecond, true, false) // 3 < MinRequests=4
	if got := b.State(); got != StateClosed {
		t.Fatalf("state = %v, want closed (请求数不足)", got)
	}
}

// 打开持续 OpenDuration 后惰性转半开；半开限流。
func TestOpenToHalfOpenAndProbeLimit(t *testing.T) {
	clk := NewFakeClock(t0)
	b := New(testConfig(), clk)
	for i := 0; i < 3; i++ {
		b.report(time.Millisecond, true, false)
	}
	if b.State() != StateOpen {
		t.Fatal("want open")
	}
	// 未到期仍是打开
	clk.Advance(4 * time.Second)
	if _, err := b.acquire(); err != ErrOpen {
		t.Fatalf("err = %v, want ErrOpen (未到期)", err)
	}
	// 到期后第一个请求触发半开
	clk.Advance(1 * time.Second)
	r1, err := b.acquire()
	if err != nil {
		t.Fatalf("半开应放行试探, err = %v", err)
	}
	if b.State() != StateHalfOpen {
		t.Fatalf("state = %v, want half-open", b.State())
	}
	// 并发试探上限为 2
	r2, err := b.acquire()
	if err != nil {
		t.Fatalf("第二个试探应放行, err = %v", err)
	}
	if _, err := b.acquire(); err != ErrHalfOpenLimited {
		t.Fatalf("err = %v, want ErrHalfOpenLimited", err)
	}
	// 释放一个试探槽位后可再放行
	r1()
	r3, err := b.acquire()
	if err != nil {
		t.Fatalf("释放后应可再放行, err = %v", err)
	}
	_ = r2
	_ = r3
}

// 半开试探失败 -> 立即回到打开。
func TestHalfOpenProbeFailureReopens(t *testing.T) {
	clk := NewFakeClock(t0)
	b := New(testConfig(), clk)
	for i := 0; i < 3; i++ {
		b.report(time.Millisecond, true, false)
	}
	clk.Advance(5 * time.Second)
	if _, err := b.acquire(); err != nil {
		t.Fatal(err)
	}
	b.report(time.Millisecond, true, false) // 试探失败
	if got := b.State(); got != StateOpen {
		t.Fatalf("state = %v, want open (试探失败立即回开)", got)
	}
	if _, err := b.acquire(); err != ErrOpen {
		t.Fatalf("err = %v, want ErrOpen", err)
	}
}

// 半开连续成功达到阈值 -> 关闭，且统计被重置。
func TestHalfOpenSuccessesClose(t *testing.T) {
	clk := NewFakeClock(t0)
	b := New(testConfig(), clk)
	for i := 0; i < 3; i++ {
		b.report(time.Millisecond, true, false)
	}
	clk.Advance(5 * time.Second)
	if _, err := b.acquire(); err != nil {
		t.Fatal(err)
	}
	b.report(time.Millisecond, false, false)
	if b.State() != StateHalfOpen {
		t.Fatalf("state = %v, want half-open (成功数未达 2)", b.State())
	}
	b.report(time.Millisecond, false, false)
	if got := b.State(); got != StateClosed {
		t.Fatalf("state = %v, want closed", got)
	}
	// 关闭后统计已重置：零星失败不会立即再次打开
	b.report(time.Millisecond, true, false)
	if got := b.State(); got != StateClosed {
		t.Fatalf("state = %v, want closed (统计已重置)", got)
	}
}

// 取消不计入统计，不触发熔断。
func TestCanceledNotCounted(t *testing.T) {
	clk := NewFakeClock(t0)
	b := New(testConfig(), clk)
	for i := 0; i < 10; i++ {
		b.report(time.Second, true, true) // canceled=true
	}
	if got := b.State(); got != StateClosed {
		t.Fatalf("state = %v, want closed (取消不计数)", got)
	}
}

// 窗口滚动后过期数据不再参与判定。
func TestWindowExpiry(t *testing.T) {
	clk := NewFakeClock(t0)
	cfg := testConfig()
	cfg.ConsecutiveFailures = 100
	b := New(cfg, clk)
	// 制造 3 失败 1 成功（失败率 0.75），但第 4 个请求前窗口已滚过
	b.report(time.Millisecond, true, false)
	b.report(time.Millisecond, true, false)
	b.report(time.Millisecond, true, false)
	clk.Advance(11 * time.Second) // 超过整个窗口
	b.report(time.Millisecond, false, false)
	if got := b.State(); got != StateClosed {
		t.Fatalf("state = %v, want closed (过期失败不应计入)", got)
	}
}
