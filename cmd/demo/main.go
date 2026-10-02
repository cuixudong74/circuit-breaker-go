// Command demo runs a fully local, reproducible fault-isolation scenario
// against an in-process dependency stub. No real external service is used;
// the clock is fake so every transition is deterministic.
package main

import (
	"context"
	"fmt"
	"time"

	cb "github.com/cuixudong74/circuit-breaker-go/circuitbreaker"
)

func main() {
	clk := cb.NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	stub := cb.NewFaultyDependency(clk)

	dep := cb.NewDependency[string]("order-service",
		cb.WithClock(clk),
		cb.WithConsecutiveFailures(3),
		cb.WithMinimumCalls(1000),
		cb.WithFailureRateThreshold(100),
		cb.WithSlowCallRateThreshold(100),
		cb.WithOpenTimeout(10*time.Second),
		cb.WithHalfOpen(3, 1, 2),
		cb.WithBulkhead(2, 1),
		cb.WithTimeout(2*time.Second),
		cb.WithFallback(func(_ context.Context, err error) (string, error) {
			return "degraded-response", nil
		}),
	)

	ctx := context.Background()

	fmt.Println("== 1. healthy calls stay closed ==")
	stub.SetOK()
	for i := 0; i < 3; i++ {
		v, err := dep.Execute(ctx, func(ctx context.Context) (string, error) { return "ok", stub.Call(ctx) })
		fmt.Printf("  call %d: v=%q err=%v state=%s\n", i, v, err, dep.State())
	}

	fmt.Println("== 2. bulkhead bounds concurrency and wait queue ==")
	block := make(chan struct{})
	fn := func(ctx context.Context) (string, error) {
		<-block
		return "ok", nil
	}
	type pair struct {
		v   string
		err error
	}
	ch := make(chan pair, 4)
	for i := 0; i < 4; i++ {
		go func() {
			v, err := dep.Execute(ctx, fn)
			ch <- pair{v, err}
		}()
	}
	time.Sleep(50 * time.Millisecond)
	inFlight, waiters := dep.BulkheadStats()
	fmt.Printf("  while blocked: inFlight=%d waiters=%d (max 2 + 1 queue)\n", inFlight, waiters)
	close(block)
	for i := 0; i < 4; i++ {
		p := <-ch
		fmt.Printf("  call %d: v=%q err=%v\n", i, p.v, p.err)
	}

	fmt.Println("== 3. dependency errors: breaker opens, fallback serves ==")
	stub.SetError(cb.ErrStub)
	for i := 0; i < 4; i++ {
		v, err := dep.Execute(ctx, func(ctx context.Context) (string, error) { return "", stub.Call(ctx) })
		fmt.Printf("  call %d: v=%q err=%v state=%s\n", i, v, err, dep.State())
	}

	fmt.Println("== 4. open: calls rejected with fallback, dependency untouched ==")
	callsBefore := stub.Calls()
	v, err := dep.Execute(ctx, func(ctx context.Context) (string, error) { return "should-not-run", nil })
	fmt.Printf("  call: v=%q err=%v state=%s (dependency calls: %d -> %d)\n",
		v, err, dep.State(), callsBefore, stub.Calls())

	fmt.Println("== 5. open timeout elapses: half-open probes, then recovery ==")
	clk.Advance(10*time.Second + time.Nanosecond)
	fmt.Printf("  after open timeout: state=%s\n", dep.State())
	stub.SetOK()
	for i := 0; i < 2; i++ {
		v, err := dep.Execute(ctx, func(ctx context.Context) (string, error) { return "ok", stub.Call(ctx) })
		fmt.Printf("  probe %d: v=%q err=%v state=%s\n", i, v, err, dep.State())
	}

	fmt.Println("== 6. clock rollback does not reopen the breaker ==")
	clk.Set(clk.Now().Add(-1 * time.Hour))
	clk.Advance(2 * time.Hour)
	fmt.Printf("  after rollback+forward: state=%s (want closed)\n", dep.State())

	fmt.Println("== 7. metrics ==")
	m := dep.Metrics()
	fmt.Printf("  state=%s total=%d success=%d failure=%d slow=%d rejected=%d\n",
		m.State, m.TotalCalls, m.SuccessfulCalls, m.FailedCalls, m.SlowCalls, m.RejectedCalls)
}
