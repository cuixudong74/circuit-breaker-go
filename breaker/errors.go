package breaker

import "errors"

// 预定义错误，调用方可用 errors.Is 判定降级原因。
var (
	// ErrOpen 熔断器处于打开状态，请求被快速拒绝。
	ErrOpen = errors.New("breaker: circuit open")
	// ErrHalfOpenLimited 半开状态下试探并发已满，请求被限流拒绝。
	ErrHalfOpenLimited = errors.New("breaker: half-open probe limit reached")
	// ErrBulkheadFull 舱壁（并发隔离槽位）已满，请求被拒绝。
	ErrBulkheadFull = errors.New("breaker: bulkhead full")
	// ErrTimeout 单次调用超过配置的超时时间。
	ErrTimeout = errors.New("breaker: call timeout")
	// ErrCanceled 调用方主动取消，不计入熔断统计。
	ErrCanceled = errors.New("breaker: call canceled")
)

// FallbackReason 区分触发降级的原因。
type FallbackReason int

const (
	ReasonNone            FallbackReason = iota // 未降级
	ReasonOpen                                  // 熔断打开
	ReasonHalfOpenLimited                       // 半开试探限流
	ReasonBulkheadFull                          // 舱壁已满
	ReasonTimeout                               // 调用超时
	ReasonFailure                               // 依赖返回错误
)

func (r FallbackReason) String() string {
	switch r {
	case ReasonOpen:
		return "open"
	case ReasonHalfOpenLimited:
		return "half_open_limited"
	case ReasonBulkheadFull:
		return "bulkhead_full"
	case ReasonTimeout:
		return "timeout"
	case ReasonFailure:
		return "failure"
	default:
		return "none"
	}
}
