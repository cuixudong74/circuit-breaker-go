package breaker

import "time"

// Config 熔断器配置。所有计数口径见 README 文档。
type Config struct {
	// Name 依赖分组名，每个依赖一个独立熔断器与舱壁。
	Name string

	// ---- 熔断统计（滑动窗口） ----
	// Window 滑动窗口总长度，按 Bucket 数均分。
	Window time.Duration
	// Bucket 窗口分桶数，>=1。
	Bucket int
	// MinRequests 窗口内最少请求数，低于该值不触发熔断判定。
	MinRequests int
	// FailureRatio 失败率阈值 (0,1]，窗口内失败率 >= 该值触发打开。
	FailureRatio float64
	// SlowThreshold 单次调用耗时 >= 该值记为慢调用。
	SlowThreshold time.Duration
	// SlowRatio 慢调用比例阈值 (0,1]，>= 该值触发打开。
	SlowRatio float64
	// ConsecutiveFailures 连续失败达到该值立即打开（与窗口判定并行生效）。
	ConsecutiveFailures int

	// ---- 打开与半开 ----
	// OpenDuration 打开状态持续时间，到期后进入半开。
	OpenDuration time.Duration
	// HalfOpenMaxConcurrent 半开状态允许的最大并发试探数。
	HalfOpenMaxConcurrent int
	// HalfOpenSuccesses 半开状态连续成功达到该数则关闭（恢复）。
	HalfOpenSuccesses int

	// ---- 调用保护 ----
	// MaxConcurrent 舱壁：该依赖允许的最大并发在途调用数。
	MaxConcurrent int
	// Timeout 单次尝试的超时时间，<=0 表示不限制。
	Timeout time.Duration

	// ---- 重试 ----
	// MaxAttempts 一次逻辑调用的最大尝试次数（含首次），>=1。
	// 重试只发生在可重试失败上；一次逻辑调用只向熔断器反馈一个最终结果。
	MaxAttempts int
	// RetryBackoff 重试间隔基数，第 n 次重试等待 backoff*n。
	RetryBackoff time.Duration
}

func (c *Config) withDefaults() Config {
	out := *c
	if out.Window <= 0 {
		out.Window = 10 * time.Second
	}
	if out.Bucket <= 0 {
		out.Bucket = 10
	}
	if out.MinRequests <= 0 {
		out.MinRequests = 10
	}
	if out.FailureRatio <= 0 || out.FailureRatio > 1 {
		out.FailureRatio = 0.5
	}
	if out.SlowThreshold <= 0 {
		out.SlowThreshold = time.Second
	}
	if out.SlowRatio <= 0 || out.SlowRatio > 1 {
		out.SlowRatio = 0.5
	}
	if out.ConsecutiveFailures <= 0 {
		out.ConsecutiveFailures = 5
	}
	if out.OpenDuration <= 0 {
		out.OpenDuration = 5 * time.Second
	}
	if out.HalfOpenMaxConcurrent <= 0 {
		out.HalfOpenMaxConcurrent = 1
	}
	if out.HalfOpenSuccesses <= 0 {
		out.HalfOpenSuccesses = 1
	}
	if out.MaxConcurrent <= 0 {
		out.MaxConcurrent = 64
	}
	if out.MaxAttempts <= 0 {
		out.MaxAttempts = 1
	}
	if out.RetryBackoff < 0 {
		out.RetryBackoff = 0
	}
	return out
}
