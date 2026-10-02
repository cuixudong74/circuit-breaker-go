package circuitbreaker

import (
	"context"
	"fmt"
	"time"
)

// Config holds all tunables for a Dependency.
//
// Defaults are chosen to be safe for production-like use; tests and demos
// override the timing-related fields with short values.
type Config struct {
	Name string

	// FailureRateThreshold opens the breaker when the failure percentage in
	// the sliding window reaches this value (and MinimumNumberOfCalls is
	// met). Range (0, 100]. Default 50.
	FailureRateThreshold float64
	// SlowCallRateThreshold opens the breaker when the slow-call percentage
	// reaches this value (and MinimumNumberOfCalls is met). Range (0, 100].
	// Default 100.
	SlowCallRateThreshold float64
	// SlowCallThreshold is the duration above which a successful call is
	// counted as slow. Default 1s.
	SlowCallThreshold time.Duration
	// MinimumNumberOfCalls is the minimum number of calls in the window
	// before rate-based opening is considered. Default 100.
	MinimumNumberOfCalls int64
	// ConsecutiveFailureThreshold opens the breaker after this many
	// consecutive failures. 0 disables consecutive-failure opening.
	// Default 5.
	ConsecutiveFailureThreshold int

	// SlidingWindowDuration is the length of the count-based sliding window.
	// Default 10s.
	SlidingWindowDuration time.Duration
	// SlidingWindowBuckets is the number of time buckets the window is
	// divided into. Bucket duration = SlidingWindowDuration /
	// SlidingWindowBuckets. Default 10.
	SlidingWindowBuckets int

	// OpenTimeout is the time the breaker stays open before transitioning to
	// half-open. Default 60s.
	OpenTimeout time.Duration
	// HalfOpenPermits is the total number of probe calls released while
	// half-open. Default 10.
	HalfOpenPermits int
	// HalfOpenConcurrency is the maximum number of probe calls in flight at
	// the same time while half-open. Default 0 means equal to HalfOpenPermits.
	HalfOpenConcurrency int
	// HalfOpenSuccessThreshold is the number of consecutive successful
	// probe calls required to close the breaker. Must be <= HalfOpenPermits.
	// Default 3.
	HalfOpenSuccessThreshold int

	// MaxConcurrent is the maximum number of calls in flight per dependency
	// (bulkhead). Default 100.
	MaxConcurrent int64
	// MaxWaitQueue is the maximum number of calls allowed to wait for a
	// bulkhead slot. 0 means no queueing (fast-fail). Default 10.
	MaxWaitQueue int64

	// Timeout is the per-attempt call timeout. 0 disables timeouts.
	// Default 2s.
	Timeout time.Duration
	// MaxAttempts is the maximum number of attempts for one logical call
	// (including the first). 1 disables retries. Default 1.
	MaxAttempts int
	// RetryBackoff is the base backoff between attempts; it doubles per
	// attempt up to MaxRetryBackoff. Default 100ms.
	RetryBackoff time.Duration
	// MaxRetryBackoff caps the backoff. Default 2s.
	MaxRetryBackoff time.Duration
	// Retryable decides whether an error may be retried. When nil, timeouts
	// are retryable.
	Retryable func(error) bool
	// Fallback is invoked when a call is rejected or fails after all
	// attempts. It receives the original error. It must respect the context
	// timeout.
	Fallback func(ctx context.Context, err error) (any, error)
	// FallbackTimeout bounds the fallback execution so a slow fallback
	// cannot hang the caller. Default 1s.
	FallbackTimeout time.Duration

	// Clock drives all timing. Defaults to the real wall clock.
	Clock Clock
}

// Option configures a Dependency.
type Option func(*Config)

func defaultConfig() Config {
	return Config{
		FailureRateThreshold:        50,
		SlowCallRateThreshold:       100,
		SlowCallThreshold:           time.Second,
		MinimumNumberOfCalls:        100,
		ConsecutiveFailureThreshold: 5,
		SlidingWindowDuration:       10 * time.Second,
		SlidingWindowBuckets:        10,
		OpenTimeout:                 60 * time.Second,
		HalfOpenPermits:             10,
		HalfOpenSuccessThreshold:    3,
		MaxConcurrent:               100,
		MaxWaitQueue:                10,
		Timeout:                     2 * time.Second,
		MaxAttempts:                 1,
		RetryBackoff:                100 * time.Millisecond,
		MaxRetryBackoff:             2 * time.Second,
		FallbackTimeout:             time.Second,
	}
}

func (c *Config) validate() error {
	if c.FailureRateThreshold <= 0 || c.FailureRateThreshold > 100 {
		return fmt.Errorf("FailureRateThreshold must be in (0,100], got %v", c.FailureRateThreshold)
	}
	if c.SlowCallRateThreshold <= 0 || c.SlowCallRateThreshold > 100 {
		return fmt.Errorf("SlowCallRateThreshold must be in (0,100], got %v", c.SlowCallRateThreshold)
	}
	if c.SlowCallThreshold <= 0 {
		return fmt.Errorf("SlowCallThreshold must be > 0, got %v", c.SlowCallThreshold)
	}
	if c.MinimumNumberOfCalls < 1 {
		return fmt.Errorf("MinimumNumberOfCalls must be >= 1, got %d", c.MinimumNumberOfCalls)
	}
	if c.ConsecutiveFailureThreshold < 0 {
		return fmt.Errorf("ConsecutiveFailureThreshold must be >= 0, got %d", c.ConsecutiveFailureThreshold)
	}
	if c.SlidingWindowDuration <= 0 {
		return fmt.Errorf("SlidingWindowDuration must be > 0, got %v", c.SlidingWindowDuration)
	}
	if c.SlidingWindowBuckets < 1 {
		return fmt.Errorf("SlidingWindowBuckets must be >= 1, got %d", c.SlidingWindowBuckets)
	}
	if c.SlidingWindowDuration < time.Duration(c.SlidingWindowBuckets) {
		return fmt.Errorf("SlidingWindowDuration %v must be >= %d ns (one bucket)", c.SlidingWindowDuration, c.SlidingWindowBuckets)
	}
	if c.OpenTimeout < 0 {
		return fmt.Errorf("OpenTimeout must be >= 0, got %v", c.OpenTimeout)
	}
	if c.HalfOpenPermits < 1 {
		return fmt.Errorf("HalfOpenPermits must be >= 1, got %d", c.HalfOpenPermits)
	}
	if c.HalfOpenConcurrency < 0 {
		return fmt.Errorf("HalfOpenConcurrency must be >= 0, got %d", c.HalfOpenConcurrency)
	}
	if c.HalfOpenSuccessThreshold < 1 || c.HalfOpenSuccessThreshold > c.HalfOpenPermits {
		return fmt.Errorf("HalfOpenSuccessThreshold must be in [1, HalfOpenPermits=%d], got %d", c.HalfOpenPermits, c.HalfOpenSuccessThreshold)
	}
	if c.MaxConcurrent < 1 {
		return fmt.Errorf("MaxConcurrent must be >= 1, got %d", c.MaxConcurrent)
	}
	if c.MaxWaitQueue < 0 {
		return fmt.Errorf("MaxWaitQueue must be >= 0, got %d", c.MaxWaitQueue)
	}
	if c.Timeout < 0 {
		return fmt.Errorf("Timeout must be >= 0, got %v", c.Timeout)
	}
	if c.MaxAttempts < 1 {
		return fmt.Errorf("MaxAttempts must be >= 1, got %d", c.MaxAttempts)
	}
	if c.RetryBackoff < 0 || c.MaxRetryBackoff < c.RetryBackoff {
		return fmt.Errorf("invalid retry backoff: base=%v max=%v", c.RetryBackoff, c.MaxRetryBackoff)
	}
	if c.FallbackTimeout <= 0 {
		return fmt.Errorf("FallbackTimeout must be > 0, got %v", c.FallbackTimeout)
	}
	if c.Clock == nil {
		c.Clock = RealClock()
	}
	if c.HalfOpenConcurrency == 0 || c.HalfOpenConcurrency > c.HalfOpenPermits {
		c.HalfOpenConcurrency = c.HalfOpenPermits
	}
	if c.Retryable == nil {
		c.Retryable = func(err error) bool { return err == ErrTimeout }
	}
	return nil
}

// --- Functional options ---

// WithFailureRateThreshold sets the failure percentage threshold (0..100).
func WithFailureRateThreshold(p float64) Option {
	return func(c *Config) { c.FailureRateThreshold = p }
}

// WithSlowCallRateThreshold sets the slow-call percentage threshold (0..100).
func WithSlowCallRateThreshold(p float64) Option {
	return func(c *Config) { c.SlowCallRateThreshold = p }
}

// WithSlowCallThreshold sets the duration above which a call is slow.
func WithSlowCallThreshold(d time.Duration) Option {
	return func(c *Config) { c.SlowCallThreshold = d }
}

// WithMinimumCalls sets the minimum number of calls before rate checks fire.
func WithMinimumCalls(n int64) Option { return func(c *Config) { c.MinimumNumberOfCalls = n } }

// WithConsecutiveFailures sets the consecutive-failure threshold (0 disables).
func WithConsecutiveFailures(n int) Option {
	return func(c *Config) { c.ConsecutiveFailureThreshold = n }
}

// WithWindow sets the sliding window duration and bucket count.
func WithWindow(d time.Duration, buckets int) Option {
	return func(c *Config) {
		c.SlidingWindowDuration = d
		c.SlidingWindowBuckets = buckets
	}
}

// WithOpenTimeout sets the open-to-half-open wait.
func WithOpenTimeout(d time.Duration) Option { return func(c *Config) { c.OpenTimeout = d } }

// WithHalfOpen sets probe permits, probe concurrency and success threshold.
func WithHalfOpen(permits, concurrency, successThreshold int) Option {
	return func(c *Config) {
		c.HalfOpenPermits = permits
		c.HalfOpenConcurrency = concurrency
		c.HalfOpenSuccessThreshold = successThreshold
	}
}

// WithBulkhead sets max in-flight calls and max wait queue size.
func WithBulkhead(maxConcurrent, maxWaitQueue int64) Option {
	return func(c *Config) {
		c.MaxConcurrent = maxConcurrent
		c.MaxWaitQueue = maxWaitQueue
	}
}

// WithTimeout sets the per-attempt timeout (0 disables).
func WithTimeout(d time.Duration) Option { return func(c *Config) { c.Timeout = d } }

// WithRetry sets max attempts and backoff bounds.
func WithRetry(maxAttempts int, baseBackoff, maxBackoff time.Duration) Option {
	return func(c *Config) {
		c.MaxAttempts = maxAttempts
		c.RetryBackoff = baseBackoff
		c.MaxRetryBackoff = maxBackoff
	}
}

// WithRetryable sets the predicate deciding which errors may be retried.
func WithRetryable(predicate func(error) bool) Option {
	return func(c *Config) { c.Retryable = predicate }
}

// WithFallbackTimeout bounds fallback execution time.
func WithFallbackTimeout(d time.Duration) Option { return func(c *Config) { c.FallbackTimeout = d } }

// WithFallback sets the fallback used on rejection/failure.
func WithFallback[T any](fb func(ctx context.Context, err error) (T, error)) Option {
	return func(c *Config) {
		c.Fallback = func(ctx context.Context, err error) (any, error) { return fb(ctx, err) }
	}
}

// WithClock overrides the clock (used by tests).
func WithClock(clk Clock) Option { return func(c *Config) { c.Clock = clk } }
