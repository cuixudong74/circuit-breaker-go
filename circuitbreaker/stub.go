package circuitbreaker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// StubMode selects the behavior of a FaultyDependency.
type StubMode int

const (
	// StubOK returns success immediately.
	StubOK StubMode = iota
	// StubError returns a configured error immediately.
	StubError
	// StubSlow succeeds after a configured delay.
	StubSlow
	// StubHang blocks until Release is called or the caller cancels.
	StubHang
)

// ErrStub is the default error used by StubError mode.
var ErrStub = errors.New("stub dependency failure")

// FaultyDependency is an in-process, controllable dependency stub that
// reproduces timeouts, errors, slowness and recovery without any real
// service. It is safe for concurrent use.
type FaultyDependency struct {
	clock Clock

	mu        sync.Mutex
	mode      StubMode
	slowDelay time.Duration
	err       error
	hang      chan struct{}

	calls    int64
	inFlight int64
}

// NewFaultyDependency creates a stub driven by the given clock.
func NewFaultyDependency(clock Clock) *FaultyDependency {
	return &FaultyDependency{
		clock: clock,
		mode:  StubOK,
		hang:  make(chan struct{}),
	}
}

// Call behaves according to the current mode.
func (f *FaultyDependency) Call(ctx context.Context) error {
	atomic.AddInt64(&f.calls, 1)
	atomic.AddInt64(&f.inFlight, 1)
	defer atomic.AddInt64(&f.inFlight, -1)

	f.mu.Lock()
	mode := f.mode
	delay := f.slowDelay
	err := f.err
	hang := f.hang
	f.mu.Unlock()

	switch mode {
	case StubOK:
		return nil
	case StubError:
		return err
	case StubSlow:
		select {
		case <-f.clock.After(delay):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	case StubHang:
		select {
		case <-hang:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// SetOK switches to immediate success.
func (f *FaultyDependency) SetOK() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mode = StubOK
}

// SetError switches to immediate error with the given error (ErrStub if nil).
func (f *FaultyDependency) SetError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mode = StubError
	if err == nil {
		err = ErrStub
	}
	f.err = err
}

// SetSlow switches to success after d.
func (f *FaultyDependency) SetSlow(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mode = StubSlow
	f.slowDelay = d
}

// SetHang switches to blocking until Release is called.
func (f *FaultyDependency) SetHang() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mode = StubHang
	f.hang = make(chan struct{})
}

// Release unblocks all current and future (until next SetHang) hanging calls.
func (f *FaultyDependency) Release() {
	f.mu.Lock()
	defer f.mu.Unlock()
	close(f.hang)
}

// Calls returns the total number of calls started.
func (f *FaultyDependency) Calls() int64 {
	return atomic.LoadInt64(&f.calls)
}

// InFlight returns the number of calls currently running.
func (f *FaultyDependency) InFlight() int64 {
	return atomic.LoadInt64(&f.inFlight)
}
