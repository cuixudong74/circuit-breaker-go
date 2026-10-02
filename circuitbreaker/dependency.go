package circuitbreaker

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Dependency wraps one downstream dependency with fault isolation:
// bulkhead (bounded concurrency + wait queue), circuit breaker, per-attempt
// timeout, bounded retries (one logical call reports exactly one outcome) and
// a bounded fallback.
type Dependency[T any] struct {
	cfg      Config
	breaker  *CircuitBreaker
	bulkhead *Bulkhead
}

// NewDependency creates a protected dependency.
func NewDependency[T any](name string, opts ...Option) *Dependency[T] {
	cfg := defaultConfig()
	cfg.Name = name
	for _, o := range opts {
		o(&cfg)
	}
	if err := cfg.validate(); err != nil {
		panic(fmt.Sprintf("circuitbreaker: invalid config for %q: %v", name, err))
	}
	return &Dependency[T]{
		cfg:      cfg,
		breaker:  newCircuitBreaker(cfg),
		bulkhead: NewBulkhead(cfg.MaxConcurrent, cfg.MaxWaitQueue),
	}
}

// Execute runs fn under protection.
//
// Retries: a logical call is attempted up to MaxAttempts times; only the final
// outcome is recorded in the breaker, so retries never multiply failure
// counts. Cancellation by the caller is never recorded and never triggers the
// fallback.
func (d *Dependency[T]) Execute(ctx context.Context, fn func(context.Context) (T, error)) (T, error) {
	var zero T
	cfg := d.cfg

	for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
		if attempt > 1 {
			bo := d.backoff(attempt)
			select {
			case <-cfg.Clock.After(bo):
			case <-ctx.Done():
				return zero, ctx.Err()
			}
		}

		// 1. Acquire a bulkhead slot (bounded wait queue).
		if err := d.bulkhead.Acquire(ctx); err != nil {
			if ctx.Err() != nil {
				return zero, ctx.Err() // caller canceled while queuing
			}
			d.breaker.recordRejected()
			return d.fallback(ctx, ErrBulkheadFull)
		}

		// 2. Breaker state / half-open probe permit.
		gen := -1
		if d.breaker.currentState() == StateHalfOpen {
			g, ok := d.breaker.beginProbe()
			if !ok {
				if d.breaker.currentState() == StateHalfOpen {
					d.bulkhead.Release()
					d.breaker.recordRejected()
					return d.fallback(ctx, ErrHalfOpenBusy)
				}
				// Breaker recovered to closed between the two checks:
				// run as a normal call.
			} else {
				gen = g
			}
		} else if d.breaker.currentState() == StateOpen {
			d.bulkhead.Release()
			d.breaker.recordRejected()
			return d.fallback(ctx, ErrCircuitOpen)
		}

		// 3. Per-attempt timeout context. The timeout is driven by the
		// configured Clock (not context.WithTimeout, which always uses the
		// wall clock), so tests can reproduce timeouts deterministically.
		runCtx, cancel := context.WithCancel(ctx)
		start := cfg.Clock.Now()
		var timeoutCh <-chan time.Time
		if cfg.Timeout > 0 {
			timeoutCh = cfg.Clock.After(cfg.Timeout)
		}
		type callResult struct {
			v   T
			err error
		}
		resultCh := make(chan callResult, 1)
		go func() {
			v, err := fn(runCtx)
			resultCh <- callResult{v: v, err: err}
			d.bulkhead.Release() // self-release: no waiter goroutine, bounded zombies
		}()

		// 4. Wait for result, timeout or caller cancellation.
		select {
		case r := <-resultCh:
			cancel()
			if ctx.Err() != nil {
				// Caller canceled concurrently: ignore the result, do not
				// count, do not fallback.
				if gen >= 0 {
					d.breaker.endProbe(gen, probeAbandoned)
				}
				return zero, ctx.Err()
			}
			elapsed := cfg.Clock.Since(start)
			if r.err == nil {
				d.breaker.recordSuccess(elapsed)
				if gen >= 0 {
					d.breaker.endProbe(gen, probeSuccess)
				}
				return r.v, nil
			}
			if attempt < cfg.MaxAttempts && cfg.Retryable(r.err) {
				continue
			}
			d.breaker.recordFailure()
			if gen >= 0 {
				d.breaker.endProbe(gen, probeFailure)
			}
			return d.fallback(ctx, r.err)

		case <-timeoutCh:
			cancel()
			if ctx.Err() != nil {
				// Caller cancellation raced with the timeout: not counted.
				if gen >= 0 {
					d.breaker.endProbe(gen, probeAbandoned)
				}
				return zero, ctx.Err()
			}
			// Our per-attempt timeout.
			if attempt < cfg.MaxAttempts && cfg.Retryable(ErrTimeout) {
				// The timed-out attempt still holds its bulkhead slot until
				// fn returns; the next attempt queues for a slot, so
				// retries can never amplify concurrency.
				continue
			}
			d.breaker.recordFailure()
			if gen >= 0 {
				d.breaker.endProbe(gen, probeFailure)
			}
			return d.fallback(ctx, ErrTimeout)

		case <-ctx.Done():
			cancel()
			// Caller cancellation: not counted, no fallback.
			if gen >= 0 {
				d.breaker.endProbe(gen, probeAbandoned)
			}
			return zero, ctx.Err()
		}
	}
	return zero, errors.New("circuit breaker: unreachable")
}

// fallback invokes the configured fallback under its own timeout so a failing
// or hanging fallback cannot block the caller. The timeout is clock-driven.
func (d *Dependency[T]) fallback(ctx context.Context, cause error) (T, error) {
	var zero T
	if d.cfg.Fallback == nil {
		return zero, cause
	}
	fbCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	timer := d.cfg.Clock.After(d.cfg.FallbackTimeout)
	type fbResult struct {
		v   T
		err error
	}
	ch := make(chan fbResult, 1)
	go func() {
		v, err := d.cfg.Fallback(fbCtx, cause)
		if err != nil {
			ch <- fbResult{err: err}
			return
		}
		tv, ok := v.(T)
		if !ok {
			ch <- fbResult{err: fmt.Errorf("fallback returned %T, want %T", v, zero)}
			return
		}
		ch <- fbResult{v: tv}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			return zero, r.err
		}
		return r.v, nil
	case <-timer:
		cancel()
		return zero, fmt.Errorf("%w: fallback timed out after %v (cause: %v)", ErrFallbackTimeout, d.cfg.FallbackTimeout, cause)
	case <-ctx.Done():
		cancel()
		return zero, ctx.Err()
	}
}

// backoff computes the retry backoff for the given 1-based attempt, doubling
// per attempt up to MaxRetryBackoff.
func (d *Dependency[T]) backoff(attempt int) time.Duration {
	bo := d.cfg.RetryBackoff
	for i := 2; i < attempt; i++ {
		bo *= 2
		if bo > d.cfg.MaxRetryBackoff || bo <= 0 {
			return d.cfg.MaxRetryBackoff
		}
	}
	return bo
}

// State returns the current breaker state, reevaluating an open timeout.
func (d *Dependency[T]) State() State {
	return d.breaker.currentState()
}

// Metrics returns a snapshot of breaker counters.
func (d *Dependency[T]) Metrics() Metrics {
	return d.breaker.metrics()
}

// Name returns the dependency name.
func (d *Dependency[T]) Name() string {
	return d.cfg.Name
}

// BulkheadStats returns the current number of in-flight calls and queued
// waiters for this dependency.
func (d *Dependency[T]) BulkheadStats() (inFlight, waiters int64) {
	return d.bulkhead.InFlight(), d.bulkhead.Waiters()
}
