package circuitbreaker

import "errors"

// Sentinel errors returned by the isolation layer.
var (
	// ErrCircuitOpen is returned when the breaker is open.
	ErrCircuitOpen = errors.New("circuit breaker is open")
	// ErrHalfOpenBusy is returned when no more probe calls are allowed in half-open.
	ErrHalfOpenBusy = errors.New("circuit breaker half-open probe limit reached")
	// ErrBulkheadFull is returned when the per-dependency wait queue is full.
	ErrBulkheadFull = errors.New("bulkhead wait queue is full")
	// ErrTimeout is returned when a dependency call exceeds its timeout.
	ErrTimeout = errors.New("dependency call timed out")
	// ErrFallbackTimeout is returned when the fallback itself exceeds its timeout.
	ErrFallbackTimeout = errors.New("fallback call timed out")
)
