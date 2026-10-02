package breaker

import (
	"sync"
	"time"
)

// State 熔断器状态。
type State int

const (
	StateClosed   State = iota // 关闭：正常放行
	StateOpen                  // 打开：快速拒绝
	StateHalfOpen              // 半开：限量试探
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// Breaker 单个依赖的熔断器与舱壁。并发安全。
//
// 状态转移语义（所有判定基于单调时钟，详见 README）：
//   - 关闭 -> 打开：连续失败 >= ConsecutiveFailures，
//     或窗口内请求数 >= MinRequests 且（失败率 >= FailureRatio 或慢调用率 >= SlowRatio）。
//   - 打开 -> 半开：打开持续 OpenDuration 后，下一次请求到达时转移（惰性转移，
//     保证无流量时不会空转，且转移时刻确定：第一个到期后请求的 acquire 时刻）。
//   - 半开 -> 关闭：连续成功试探 >= HalfOpenSuccesses。
//   - 半开 -> 打开：任一试探失败，立即转移。
type Breaker struct {
	cfg    Config
	clock  Clock
	mu     sync.Mutex
	state  State
	metric *windowMetrics

	consecFailures int // 连续失败计数（成功清零）
	openedAt       time.Time
	halfConcurrent int // 半开状态当前在途试探数
	halfSuccesses  int // 半开状态连续成功数
	inflight       int // 舱壁在途调用数
}

// New 创建熔断器。clk 为 nil 时使用真实时钟。
func New(cfg Config, clk Clock) *Breaker {
	if clk == nil {
		clk = RealClock{}
	}
	c := cfg.withDefaults()
	return &Breaker{
		cfg:    c,
		clock:  clk,
		state:  StateClosed,
		metric: newWindowMetrics(clk, c.Window, c.Bucket),
	}
}

// State 返回当前状态（可能触发到期的打开->半开惰性转移）。
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.maybeHalfOpen(b.clock.Now())
	return b.state
}

// acquire 尝试放行一次调用。成功返回释放函数（必须且只能调用一次），
// 失败返回拒绝原因（ErrOpen / ErrHalfOpenLimited / ErrBulkheadFull）。
func (b *Breaker) acquire() (release func(), err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.clock.Now()
	b.maybeHalfOpen(now)

	switch b.state {
	case StateOpen:
		return nil, ErrOpen
	case StateHalfOpen:
		if b.halfConcurrent >= b.cfg.HalfOpenMaxConcurrent {
			return nil, ErrHalfOpenLimited
		}
		// 试探流量也受舱壁约束，防止半开叠加全量压垮依赖。
		if b.inflight >= b.cfg.MaxConcurrent {
			return nil, ErrBulkheadFull
		}
		b.halfConcurrent++
		b.inflight++
		return func() {
			b.mu.Lock()
			defer b.mu.Unlock()
			b.halfConcurrent--
			b.inflight--
		}, nil
	default: // StateClosed
		if b.inflight >= b.cfg.MaxConcurrent {
			return nil, ErrBulkheadFull
		}
		b.inflight++
		return func() {
			b.mu.Lock()
			defer b.mu.Unlock()
			b.inflight--
		}, nil
	}
}

// report 上报一次已完成调用的结果。dur 为实际耗时。
// canceled（调用方取消）不计入任何统计，避免污染熔断判定。
func (b *Breaker) report(dur time.Duration, failed, canceled bool) {
	if canceled {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.clock.Now()
	slow := b.cfg.SlowThreshold > 0 && dur >= b.cfg.SlowThreshold
	b.metric.record(now, failed, slow)

	switch b.state {
	case StateHalfOpen:
		if failed {
			// 试探失败：立即回到打开，不放大试探流量。
			b.openLocked(now)
		} else {
			b.halfSuccesses++
			if b.halfSuccesses >= b.cfg.HalfOpenSuccesses {
				b.closeLocked()
			}
		}
	case StateClosed:
		if failed {
			b.consecFailures++
		} else {
			b.consecFailures = 0
		}
		if b.consecFailures >= b.cfg.ConsecutiveFailures {
			b.openLocked(now)
			return
		}
		s := b.metric.snapshot(now)
		if s.total >= b.cfg.MinRequests {
			if float64(s.failure)/float64(s.total) >= b.cfg.FailureRatio ||
				float64(s.slow)/float64(s.total) >= b.cfg.SlowRatio {
				b.openLocked(now)
			}
		}
	case StateOpen:
		// 打开前已放行的在途调用仍计入统计，但不触发转移。
	}
}

// maybeHalfOpen 打开状态到期则转半开。调用方必须持锁。
func (b *Breaker) maybeHalfOpen(now time.Time) {
	if b.state == StateOpen && !now.Before(b.openedAt.Add(b.cfg.OpenDuration)) {
		b.state = StateHalfOpen
		b.halfConcurrent = 0
		b.halfSuccesses = 0
	}
}

// openLocked 转移到打开状态。调用方必须持锁。
func (b *Breaker) openLocked(now time.Time) {
	b.state = StateOpen
	b.openedAt = now
	b.halfConcurrent = 0
	b.halfSuccesses = 0
}

// closeLocked 转移到关闭状态，并重置统计避免陈旧数据立即再次触发熔断。
// 调用方必须持锁。
func (b *Breaker) closeLocked() {
	b.state = StateClosed
	b.consecFailures = 0
	b.halfConcurrent = 0
	b.halfSuccesses = 0
	b.metric = newWindowMetrics(b.clock, b.cfg.Window, b.cfg.Bucket)
}
