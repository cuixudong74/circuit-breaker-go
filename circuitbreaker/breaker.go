package circuitbreaker

import (
	"sync"
	"time"
)

// State is the circuit breaker state.
type State int

const (
	// StateClosed allows calls through; outcomes are counted in the sliding
	// window and drive rate-based opening.
	StateClosed State = iota
	// StateOpen rejects every call until OpenTimeout elapses, then moves to
	// half-open.
	StateOpen
	// StateHalfOpen releases a limited number of probe calls; a failed probe
	// reopens immediately, enough consecutive successes close the breaker.
	StateHalfOpen
)

func (s State) String() string {
	return [...]string{"closed", "open", "half-open"}[s]
}

type probeOutcome int

const (
	// probeAbandoned means the caller canceled while the probe was running;
	// it neither counts as success nor failure but releases the in-flight slot.
	probeAbandoned probeOutcome = iota
	probeSuccess
	probeFailure
)

// CircuitBreaker implements the closed/open/half-open state machine.
//
// All state transitions and counter updates happen under a single mutex, so
// concurrent callers can never observe inconsistent state or over-issue probe
// permits.
type CircuitBreaker struct {
	cfg   Config
	clock Clock

	mu                  sync.Mutex
	state               State
	window              *window
	openedAt            time.Time
	consecutiveFailures int

	// half-open state
	halfOpenGen       int
	probesRemaining   int
	probesInFlight    int
	halfOpenSuccesses int

	rejected int64
}

func newCircuitBreaker(cfg Config) *CircuitBreaker {
	return &CircuitBreaker{
		cfg:    cfg,
		clock:  cfg.Clock,
		state:  StateClosed,
		window: newWindow(cfg.Clock, cfg.SlidingWindowDuration, cfg.SlidingWindowBuckets),
	}
}

// reevaluate moves open -> half-open once the open timeout has elapsed. It is
// called on every entry path and also from the open timer.
func (b *CircuitBreaker) reevaluate() {
	b.mu.Lock()
	b.reevaluateLocked()
	b.mu.Unlock()
}

func (b *CircuitBreaker) reevaluateLocked() {
	if b.state == StateOpen && b.clock.Now().Sub(b.openedAt) >= b.cfg.OpenTimeout {
		b.toHalfOpenLocked()
	}
}

func (b *CircuitBreaker) currentState() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.reevaluateLocked()
	return b.state
}

// beginProbe reserves one probe call in half-open. It returns the probe
// generation (to ignore outcomes from an earlier half-open round) and whether
// the probe is allowed.
func (b *CircuitBreaker) beginProbe() (int, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.reevaluateLocked()
	if b.state != StateHalfOpen {
		return 0, false
	}
	if b.probesInFlight >= b.cfg.HalfOpenConcurrency {
		return b.halfOpenGen, false
	}
	if b.probesRemaining <= 0 {
		return b.halfOpenGen, false
	}
	b.probesRemaining--
	b.probesInFlight++
	return b.halfOpenGen, true
}

// endProbe reports the outcome of a probe call. Stale outcomes from a previous
// half-open generation are ignored.
func (b *CircuitBreaker) endProbe(gen int, outcome probeOutcome) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state != StateHalfOpen || gen != b.halfOpenGen {
		return
	}
	b.probesInFlight--
	switch outcome {
	case probeSuccess:
		b.halfOpenSuccesses++
		if b.halfOpenSuccesses >= b.cfg.HalfOpenSuccessThreshold {
			b.toClosedLocked()
		}
	case probeFailure:
		b.toOpenLocked()
	case probeAbandoned:
		// neither success nor failure
	}
}

// recordSuccess records a successful call. Slow successes also increment the
// slow counter. Only effective in closed state.
func (b *CircuitBreaker) recordSuccess(elapsed time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state != StateClosed {
		return
	}
	b.consecutiveFailures = 0
	slow := elapsed > b.cfg.SlowCallThreshold
	b.window.add(true, false, slow)
	b.maybeOpenLocked()
}

// recordFailure records a failed call (error or timeout). Only effective in
// closed state; in half-open the probe outcome drives transitions.
func (b *CircuitBreaker) recordFailure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state != StateClosed {
		return
	}
	b.consecutiveFailures++
	b.window.add(false, true, false)
	b.maybeOpenLocked()
}

func (b *CircuitBreaker) recordRejected() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.rejected++
}

// maybeOpenLocked checks the open conditions. Caller holds the lock.
func (b *CircuitBreaker) maybeOpenLocked() {
	if b.cfg.ConsecutiveFailureThreshold > 0 &&
		b.consecutiveFailures >= b.cfg.ConsecutiveFailureThreshold {
		b.toOpenLocked()
		return
	}
	total, _, failure, slow := b.window.totals()
	if total < b.cfg.MinimumNumberOfCalls {
		return
	}
	if pct(failure, total) >= b.cfg.FailureRateThreshold {
		b.toOpenLocked()
		return
	}
	if pct(slow, total) >= b.cfg.SlowCallRateThreshold {
		b.toOpenLocked()
		return
	}
}

func pct(part, total int64) float64 {
	if total == 0 {
		return 0
	}
	return float64(part) / float64(total) * 100
}

func (b *CircuitBreaker) toOpenLocked() {
	if b.state == StateOpen {
		return
	}
	b.state = StateOpen
	b.openedAt = b.clock.Now()
	b.consecutiveFailures = 0
	b.window.reset()
	b.scheduleOpenTimer()
}

func (b *CircuitBreaker) toHalfOpenLocked() {
	if b.state != StateOpen {
		return
	}
	b.state = StateHalfOpen
	b.halfOpenGen++
	b.probesRemaining = b.cfg.HalfOpenPermits
	b.probesInFlight = 0
	b.halfOpenSuccesses = 0
	b.window.reset()
}

func (b *CircuitBreaker) toClosedLocked() {
	b.state = StateClosed
	b.consecutiveFailures = 0
	b.window.reset()
}

// scheduleOpenTimer transitions open -> half-open when the timeout elapses.
// Lazy reevaluation on every call covers the same path; the timer keeps the
// state fresh even without traffic.
func (b *CircuitBreaker) scheduleOpenTimer() {
	ch := b.clock.After(b.cfg.OpenTimeout)
	go func() {
		<-ch
		b.mu.Lock()
		if b.state == StateOpen {
			b.toHalfOpenLocked()
		}
		b.mu.Unlock()
	}()
}

// Metrics is a point-in-time snapshot of breaker counters.
type Metrics struct {
	State               State
	TotalCalls          int64
	SuccessfulCalls     int64
	FailedCalls         int64
	SlowCalls           int64
	RejectedCalls       int64
	ConsecutiveFailures int
	HalfOpenProbesLeft  int
	HalfOpenInFlight    int
	HalfOpenSuccesses   int
}

func (b *CircuitBreaker) metrics() Metrics {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.reevaluateLocked()
	total, success, failure, slow := b.window.totals()
	return Metrics{
		State:               b.state,
		TotalCalls:          total,
		SuccessfulCalls:     success,
		FailedCalls:         failure,
		SlowCalls:           slow,
		RejectedCalls:       b.rejected,
		ConsecutiveFailures: b.consecutiveFailures,
		HalfOpenProbesLeft:  b.probesRemaining,
		HalfOpenInFlight:    b.probesInFlight,
		HalfOpenSuccesses:   b.halfOpenSuccesses,
	}
}
