package stub

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDefaultSuccess(t *testing.T) {
	d := New()
	v, err := d.Call(context.Background())
	if err != nil || v != "ok" {
		t.Fatalf("got %v,%v", v, err)
	}
}

func TestScriptConsumedInOrder(t *testing.T) {
	d := New()
	errBoom := errors.New("boom")
	d.SetScript([]Behavior{{Err: errBoom}, {}, {}})
	for i, want := range []error{errBoom, nil, nil} {
		_, err := d.Call(context.Background())
		if !errors.Is(err, want) {
			t.Fatalf("call %d: err = %v, want %v", i, err, want)
		}
	}
	// 脚本用完后重复最后一个
	if _, err := d.Call(context.Background()); err != nil {
		t.Fatalf("脚本耗尽后应重复最后一个行为, err = %v", err)
	}
	if d.Calls() != 4 {
		t.Fatalf("calls = %d, want 4", d.Calls())
	}
}

func TestLatencyAndCancel(t *testing.T) {
	d := New()
	d.Set(Behavior{Latency: time.Hour})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := d.Call(ctx); done <- err }()
	time.Sleep(10 * time.Millisecond)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestHangWaitsForCancel(t *testing.T) {
	d := New()
	d.Set(Behavior{Hang: true})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := d.Call(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("挂起应被 ctx 超时打断")
	}
}
