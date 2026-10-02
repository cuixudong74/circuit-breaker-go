package circuitbreaker

import (
	"sync"
	"time"
)

// Clock abstracts time so that tests can drive a deterministic clock.
//
// Implementations must use monotonic time for elapsed-time calculations:
// time.Time values produced by the real clock carry a monotonic reading, so
// Clock.Since / time.Time.Sub are immune to wall-clock adjustments (NTP
// corrections, manual time changes). A manually driven FakeClock is used in
// tests to reproduce timeouts, window expiry and recovery deterministically.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
	Sleep(d time.Duration)
	Since(t time.Time) time.Duration
}

type realClock struct{}

// RealClock returns the wall-clock backed implementation.
func RealClock() Clock { return realClock{} }

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }
func (realClock) Sleep(d time.Duration)                  { time.Sleep(d) }
func (realClock) Since(t time.Time) time.Duration        { return time.Since(t) }

// FakeClock is a manually advanced clock for reproducible tests.
//
// Timers created with After fire when Advance/Set reaches (or passes) their
// deadline. All methods are safe for concurrent use.
type FakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

type fakeTimer struct {
	deadline time.Time
	ch       chan time.Time
}

// NewFakeClock creates a FakeClock starting at the given time.
func NewFakeClock(start time.Time) *FakeClock {
	return &FakeClock{now: start}
}

func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *FakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	if d <= 0 {
		ch <- c.now
		return ch
	}
	t := &fakeTimer{deadline: c.now.Add(d), ch: ch}
	// Insert sorted by deadline (stable order for equal deadlines).
	i := len(c.timers)
	for i > 0 && c.timers[i-1].deadline.After(t.deadline) {
		i--
	}
	c.timers = append(c.timers, nil)
	copy(c.timers[i+1:], c.timers[i:])
	c.timers[i] = t
	return ch
}

func (c *FakeClock) Sleep(d time.Duration) {
	<-c.After(d)
}

func (c *FakeClock) Since(t time.Time) time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now.Sub(t)
}

// Advance moves the clock forward by d and fires all due timers.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.fireLocked()
	c.mu.Unlock()
}

// Set sets the clock to an absolute time and fires all due timers.
// Setting the clock backwards is supported: elapsed-time calculations then
// produce negative durations, which callers treat as "no time has passed".
func (c *FakeClock) Set(t time.Time) {
	c.mu.Lock()
	c.now = t
	c.fireLocked()
	c.mu.Unlock()
}

func (c *FakeClock) fireLocked() {
	due := 0
	for _, t := range c.timers {
		if !t.deadline.After(c.now) {
			due++
		} else {
			break
		}
	}
	for i := 0; i < due; i++ {
		select {
		case c.timers[i].ch <- c.now:
		default:
		}
	}
	if due > 0 {
		copy(c.timers, c.timers[due:])
		c.timers = c.timers[:len(c.timers)-due]
	}
}
