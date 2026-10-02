package circuitbreaker

import (
	"context"
	"sync/atomic"
)

// Bulkhead bounds the number of calls in flight and the wait queue per
// dependency, so a slow dependency cannot consume unbounded goroutines or
// memory.
//
// The fast path is a buffered channel of size maxConcurrent. Calls that cannot
// acquire immediately enter a bounded wait queue; when the queue is full they
// fail fast with ErrBulkheadFull instead of piling up goroutines.
type Bulkhead struct {
	sem        chan struct{}
	maxWaiters int64
	waiters    int64
}

// NewBulkhead creates a bulkhead with the given limits.
func NewBulkhead(maxConcurrent, maxWaitQueue int64) *Bulkhead {
	if maxConcurrent < 1 {
		maxConcurrent = 1
	}
	if maxWaitQueue < 0 {
		maxWaitQueue = 0
	}
	return &Bulkhead{
		sem:        make(chan struct{}, maxConcurrent),
		maxWaiters: maxWaitQueue,
	}
}

// Acquire obtains a slot, waiting up to maxWaitQueue queued calls. It returns
// ErrBulkheadFull when the queue is full, or the context error when the
// caller cancels while waiting.
func (b *Bulkhead) Acquire(ctx context.Context) error {
	select {
	case b.sem <- struct{}{}:
		return nil
	default:
	}
	if atomic.AddInt64(&b.waiters, 1) > b.maxWaiters {
		atomic.AddInt64(&b.waiters, -1)
		return ErrBulkheadFull
	}
	defer atomic.AddInt64(&b.waiters, -1)
	select {
	case b.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Release returns a slot.
func (b *Bulkhead) Release() {
	<-b.sem
}

// InFlight returns the number of calls currently holding a slot.
func (b *Bulkhead) InFlight() int64 {
	return int64(len(b.sem))
}

// Waiters returns the number of calls currently queued.
func (b *Bulkhead) Waiters() int64 {
	return atomic.LoadInt64(&b.waiters)
}
