package breaker

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Result 一次逻辑调用的结果。
type Result struct {
	Value  any            // 成功或降级返回值
	Err    error          // 最终错误（含降级失败）
	Reason FallbackReason // 降级原因，未降级为 ReasonNone
}

// Executor 对单个依赖执行受保护的调用。
type Executor struct {
	br       *Breaker
	fallback func(ctx context.Context, reason FallbackReason, err error) (any, error)
}

// NewExecutor 基于熔断器创建执行器。fallback 可为 nil（无降级，直接返回错误）。
func NewExecutor(br *Breaker, fallback func(ctx context.Context, reason FallbackReason, err error) (any, error)) *Executor {
	return &Executor{br: br, fallback: fallback}
}

// attemptResult 单次尝试的产出。
type attemptResult struct {
	v   any
	err error
}

// Do 执行一次逻辑调用：熔断判定 -> 舱壁隔离 -> 超时控制 -> 重试 -> 降级。
//
// 语义保证：
//   - 一次 Do 无论重试多少次，只向熔断器反馈一个最终结果；
//   - 调用方 ctx 取消时立即返回 ErrCanceled，不计入熔断统计，不触发降级；
//   - 单次尝试超时按失败处理（可重试），最终失败走降级；
//   - 降级函数自身出错时，错误透传给调用方，不会挂死或吞掉。
func (e *Executor) Do(ctx context.Context, fn func(ctx context.Context) (any, error)) Result {
	cfg := e.br.cfg

	release, err := e.br.acquire()
	if err != nil {
		return e.degrade(ctx, reasonOf(err), err)
	}

	start := e.br.clock.Now()
	var finalErr error
	failed := false
	canceled := false

	for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
		if ctx.Err() != nil {
			canceled = true
			break
		}
		v, aerr := e.attempt(ctx, fn)
		if aerr == nil {
			release()
			e.br.report(e.br.clock.Now().Sub(start), false, false)
			return Result{Value: v, Reason: ReasonNone}
		}
		if errors.Is(aerr, ErrCanceled) {
			canceled = true
			break
		}
		finalErr = aerr
		failed = true
		// 重试间隔：第 n 次重试前等待 backoff*n，可被取消打断。
		if attempt < cfg.MaxAttempts && cfg.RetryBackoff > 0 {
			if !sleepOrDone(e.br.clock, cfg.RetryBackoff*time.Duration(attempt), ctx.Done()) {
				canceled = true
				break
			}
		}
	}

	release()
	e.br.report(e.br.clock.Now().Sub(start), failed, canceled)
	if canceled {
		return Result{Err: ErrCanceled, Reason: ReasonNone}
	}
	return e.degrade(ctx, reasonOf(finalErr), finalErr)
}

// attempt 执行单次尝试，带超时控制。
func (e *Executor) attempt(ctx context.Context, fn func(ctx context.Context) (any, error)) (any, error) {
	attemptCtx := ctx
	cancel := func() {}
	if e.br.cfg.Timeout > 0 {
		attemptCtx, cancel = context.WithTimeout(ctx, e.br.cfg.Timeout)
	}
	defer cancel()

	ch := make(chan attemptResult, 1) // 缓冲为 1，超时返回后 fn 完成不会泄漏 goroutine
	go func() {
		v, err := fn(attemptCtx)
		ch <- attemptResult{v, err}
	}()

	select {
	case r := <-ch:
		if r.err != nil && attemptCtx.Err() != nil && ctx.Err() == nil &&
			errors.Is(attemptCtx.Err(), context.DeadlineExceeded) {
			// fn 恰好在超时后返回错误：按超时口径归类。
			return nil, ErrTimeout
		}
		return r.v, r.err
	case <-attemptCtx.Done():
		if ctx.Err() != nil {
			return nil, ErrCanceled // 调用方取消，不污染熔断计数
		}
		return nil, ErrTimeout
	}
}

// degrade 走降级路径。fallback 为 nil 时直接返回原始错误。
func (e *Executor) degrade(ctx context.Context, reason FallbackReason, err error) Result {
	if e.fallback == nil {
		return Result{Err: err, Reason: reason}
	}
	v, ferr := e.fallback(ctx, reason, err)
	return Result{Value: v, Err: ferr, Reason: reason}
}

// reasonOf 将内部错误映射为降级原因。
func reasonOf(err error) FallbackReason {
	switch {
	case errors.Is(err, ErrOpen):
		return ReasonOpen
	case errors.Is(err, ErrHalfOpenLimited):
		return ReasonHalfOpenLimited
	case errors.Is(err, ErrBulkheadFull):
		return ReasonBulkheadFull
	case errors.Is(err, ErrTimeout):
		return ReasonTimeout
	default:
		return ReasonFailure
	}
}

// sleepOrDone 等待 d，期间 done 关闭则提前返回 false。
// done 为 nil（如 context.Background()）时只等待 d。
func sleepOrDone(clk Clock, d time.Duration, done <-chan struct{}) bool {
	if done == nil {
		return clk.Sleep(d, nil)
	}
	stop := make(chan struct{})
	var once sync.Once
	go func() {
		select {
		case <-done:
			once.Do(func() { close(stop) })
		case <-stop:
		}
	}()
	ok := clk.Sleep(d, stop)
	once.Do(func() { close(stop) })
	return ok
}
