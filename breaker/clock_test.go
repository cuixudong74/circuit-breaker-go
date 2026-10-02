package breaker

import (
	"context"
	"testing"
	"time"
)

// 时间回拨不得凭空触发熔断：关闭状态下回拨后统计不清空、不误判。
func TestClockBackwardNoSpuriousTrip(t *testing.T) {
	clk := NewFakeClock(t0)
	cfg := testConfig()
	cfg.ConsecutiveFailures = 100
	cfg.MinRequests = 4
	b := New(cfg, clk)
	// 2 失败 2 成功，失败率 0.5 恰好达阈值边缘
	b.report(time.Millisecond, true, false)
	b.report(time.Millisecond, false, false)
	b.report(time.Millisecond, true, false)
	clk.Advance(-time.Hour) // 系统时间大幅回拨
	b.report(time.Millisecond, false, false)
	// 回拨不得导致统计丢失或重复计数口径变化：total=4, failure=2, 0.5>=0.5 触发
	// 这里验证的是"确定性"：回拨前后同一窗口口径一致，不多算也不少算。
	if got := b.State(); got != StateOpen {
		t.Fatalf("state = %v, want open (回拨不应改变窗口内既有统计口径)", got)
	}
}

// 时间回拨不得让打开状态长期不恢复：恢复判定基于单调时钟，
// 回拨只是推迟到期点，再次前进到 OpenDuration 后必须能转半开。
func TestClockBackwardDelaysButRecovers(t *testing.T) {
	clk := NewFakeClock(t0)
	b := New(testConfig(), clk)
	for i := 0; i < 3; i++ {
		b.report(time.Millisecond, true, false)
	}
	if b.State() != StateOpen {
		t.Fatal("want open")
	}
	clk.Advance(4 * time.Second)
	clk.Advance(-10 * time.Second) // 回拨：相当于到期点被推迟
	if _, err := b.acquire(); err != ErrOpen {
		t.Fatalf("err = %v, want ErrOpen (回拨后未到期)", err)
	}
	// 重新前进至累计到达 OpenDuration 后必须恢复，不得永久卡死
	clk.Advance(11 * time.Second)
	if _, err := b.acquire(); err != nil {
		t.Fatalf("err = %v, want nil (前进后应恢复试探)", err)
	}
	if b.State() != StateHalfOpen {
		t.Fatalf("state = %v, want half-open", b.State())
	}
}

// 时间大幅前进：窗口统计全部过期，打开状态到期恢复。
func TestClockForwardExpiresWindow(t *testing.T) {
	clk := NewFakeClock(t0)
	cfg := testConfig()
	cfg.ConsecutiveFailures = 100
	b := New(cfg, clk)
	b.report(time.Millisecond, true, false)
	b.report(time.Millisecond, true, false)
	b.report(time.Millisecond, true, false)
	clk.Advance(time.Hour) // 远超窗口
	// 过期后失败不再计入：1 个成功不应触发任何判定
	b.report(time.Millisecond, false, false)
	if got := b.State(); got != StateClosed {
		t.Fatalf("state = %v, want closed (过期数据不得参与判定)", got)
	}
}

// FakeClock.Sleep：Advance 到期唤醒，stop 关闭提前返回。
func TestFakeClockSleep(t *testing.T) {
	clk := NewFakeClock(t0)
	done := make(chan bool, 1)
	go func() { done <- clk.Sleep(5*time.Second, nil) }()
	waitFor(t, time.Second, func() bool { return clk.Waiters() == 1 })
	clk.Advance(4 * time.Second)
	select {
	case <-done:
		t.Fatal("未到期不应唤醒")
	case <-time.After(20 * time.Millisecond):
	}
	clk.Advance(1 * time.Second)
	if !<-done {
		t.Fatal("到期应返回 true")
	}

	stop := make(chan struct{})
	go func() { done <- clk.Sleep(time.Hour, stop) }()
	waitFor(t, time.Second, func() bool { return clk.Waiters() == 1 })
	close(stop)
	if <-done {
		t.Fatal("stop 关闭应返回 false")
	}
}

// 重试退避基于单调时钟：退避期间回拨不得凭空提前唤醒，补足前进量后必须唤醒。
func TestRetryBackoffWithFakeClock(t *testing.T) {
	clk := NewFakeClock(t0)
	cfg := execConfig()
	cfg.Timeout = 0
	cfg.MaxAttempts = 2
	cfg.RetryBackoff = 100 * time.Millisecond
	cfg.ConsecutiveFailures = 100
	br := New(cfg, clk)
	ex := NewExecutor(br, nil)

	done := make(chan Result, 1)
	go func() {
		done <- ex.Do(context.Background(), func(ctx context.Context) (any, error) {
			return nil, errTest
		})
	}()
	// 等第一次尝试失败进入退避（退避等待已注册到假时钟）
	waitFor(t, time.Second, func() bool { return clk.Waiters() == 1 })
	clk.Advance(-time.Hour)            // 退避期间回拨：不得凭空提前唤醒
	clk.Advance(99 * time.Millisecond) // 前进不足退避时长
	select {
	case <-done:
		t.Fatal("回拨+不足量前进不应结束退避")
	case <-time.After(20 * time.Millisecond):
	}
	clk.Advance(time.Hour + time.Millisecond) // 补足前进量后必须唤醒，不得卡死
	select {
	case r := <-done:
		if r.Err == nil {
			t.Fatal("want error")
		}
	case <-time.After(time.Second):
		t.Fatal("退避应在单调时钟前进后结束")
	}
}

var errTest = errTestT{}

type errTestT struct{}

func (errTestT) Error() string { return "test error" }

func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("waitFor 超时")
}
