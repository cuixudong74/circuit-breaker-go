# circuit-breaker-go

面向下游依赖的故障隔离与熔断库。按依赖分组隔离调用，单个依赖变慢或连续
报错时自动熔断，不拖垮调用方、不连累其他依赖。整个链路只在本进程内运行，
不依赖任何真实外部服务；通过内置的内存调用桩与可控时钟，所有超时、报错、
变慢、恢复与时间跳变都能在本地可复现地验证。

## 快速开始

```go
clk := cb.RealClock() // 生产环境用真实时钟
dep := cb.NewDependency[*Order]("order-service",
    cb.WithConsecutiveFailures(5),
    cb.WithFailureRateThreshold(50),
    cb.WithSlowCallThreshold(500*time.Millisecond),
    cb.WithOpenTimeout(30*time.Second),
    cb.WithHalfOpen(10, 3, 5),
    cb.WithBulkhead(20, 5),
    cb.WithTimeout(2*time.Second),
    cb.WithFallback(func(ctx context.Context, err error) (*Order, error) {
        return cachedOrder(), nil // 降级兜底
    }),
)

order, err := dep.Execute(ctx, func(ctx context.Context) (*Order, error) {
    return orderClient.Get(ctx, id)
})
```

本地可复现演示（使用假时钟与内存桩）：

```sh
go run ./cmd/demo
```

## 核心概念

### 状态机

```
                          连续失败 / 失败率 / 慢调用率达标
        ┌──────────────────────────────────────────────┐
        │                                              ▼
   ┌────────┐  打开超时(OpenTimeout)到期   ┌──────────┐
   │ closed │ ───────────────────────────► │   open   │
   │        │ ◄─────────────────────────── │          │
   └────────┘      试探全部成功            └──────────┘
        ▲                                        │
        │ 达到成功阈值(HalfOpenSuccessThreshold)  │ 试探失败
        └────────────────────────────────────────┤
                                                  ▼
                                            ┌──────────┐
                                            │ half-open│
                                            └──────────┘
```

- **closed（关闭）**：正常放行。每次调用的结果（成功 / 失败 / 慢）计入滑动
  窗口。
- **open（打开）**：拒绝一切调用（走降级路径），依赖完全不被触碰。打开状态
  持续 `OpenTimeout` 后进入半开。
- **half-open（半开）**：只放行有限数量的试探调用，探测依赖是否恢复。

### 状态转移的判定口径

closed -> open，满足以下任一条件即触发（每次调用结束后判定）：

1. **连续失败**：连续失败次数达到 `ConsecutiveFailureThreshold`（默认 5；
   设为 0 可关闭该条件）。连续失败计数在每次成功时清零；窗口滚动不会清零
   （它按调用次序累计，与时间无关）。
2. **失败率**：窗口内失败占比 >= `FailureRateThreshold`（默认 50%），且
   窗口内调用总数 >= `MinimumNumberOfCalls`（默认 100）。
3. **慢调用率**：窗口内慢调用占比 >= `SlowCallRateThreshold`（默认 100%），
   且调用总数 >= `MinimumNumberOfCalls`。

open -> half-open：打开状态持续 `OpenTimeout`（默认 60s）后自动转移。转移
由两条路径驱动：一个定时器（保证无流量时也能转移）与每次调用时的惰性
重评估（`State()` / `Execute()` 入口），二者等价且幂等。

half-open -> closed：试探调用连续成功数达到 `HalfOpenSuccessThreshold`
（默认 3）。

half-open -> open：任意一次试探失败（错误或超时）**立即**回到打开状态，不
等待其他试探，也不把试探流量放大成全量流量。

> 半开状态下的慢试探按成功计（依赖可达，只是慢）；是否仍慢由关闭后的慢
> 调用率重新判定。

### 半开放行（试探）

- `HalfOpenPermits`（默认 10）：半开期间总共放行的试探调用数。
- `HalfOpenConcurrency`（默认等于 Permits）：同时在飞的试探调用上限。
- 每次试探占用一个 permit；在飞数达到并发上限或 permit 用完后，后续调用
  立即被拒（`ErrHalfOpenBusy`），不排队、不放大。
- permit 在进入半开时一次性重置；试探失败立即重开并重置 permit。
- 每轮半开有独立的 generation 编号，上一轮残留的试探调用回来时结果被
  忽略，不会污染新一轮的计数。

### 滑动窗口

- 窗口时长 `SlidingWindowDuration`（默认 10s），划分为
  `SlidingWindowBuckets`（默认 10）个桶，桶时长 = 窗口时长 / 桶数。
- 每个桶记录：总数、成功数、失败数、慢调用数。
- 窗口按**单调时钟**滚动：每次记录时把过期的桶清零复用。整个窗口过期后
  重置。
- **时钟回拨**：单调时钟（`time.Time` 携带 monotonic 读数）下，`Sub` 不会
  因墙上时间被回拨而产生负的间隔。测试用的 `FakeClock` 支持手动回拨，此
  时窗口整体重置——不会凭空触发熔断，也不会阻止恢复。
- 窗口在进入 open / half-open / closed 时重置，保证每轮状态的计数口径
  干净确定。

### 批量隔离（Bulkhead）

每个依赖一个独立的 bulkhead：

- `MaxConcurrent`（默认 100）：同时在飞调用上限（带缓冲的信号量）。
- `MaxWaitQueue`（默认 10）：等待队列上限。队列满时立即失败
  （`ErrBulkheadFull`），不堆积 goroutine。
- 调用方取消（ctx.Done）在队列中等待时直接返回，不计数。

### 超时、取消与重试

- **超时**：`Timeout`（默认 2s）为每次尝试的超时。超时由配置的时钟驱动
  （`Clock.After`），因此测试可以用假时钟确定性地复现超时。超时计入
  失败。
- **取消**：调用方 ctx 被取消时，立即返回 `ctx.Err()`，**不计入熔断
  计数**，也**不触发降级**。
- **重试**：`MaxAttempts`（默认 1，即不重试）为一次逻辑调用的最大尝试
  次数。可重试错误由 `Retryable` 判定（默认仅超时可重试）。重试间隔按
  `RetryBackoff` 指数退避（每次翻倍，上限 `MaxRetryBackoff`），退避由
  时钟驱动。
- **一次逻辑调用最终只反馈一个结论**：无论内部重试多少次，熔断窗口只
  记录一次结果（成功或失败）。
- 超时后仍在运行的尝试不会被强制杀死（Go 无法安全杀 goroutine），它
  们在后台运行并在结束时自行释放 bulkhead 槽位；因此在飞数始终受
  `MaxConcurrent` 约束，不会出现无界 goroutine。重试需要重新排队获取
  槽位，不会放大并发。

### 降级与兜底

以下情况走 `Fallback`：

- 熔断打开（`ErrCircuitOpen`）
- 半开试探被拒（`ErrHalfOpenBusy`）
- bulkhead 队列满（`ErrBulkheadFull`）
- 所有尝试失败 / 超时

降级原因通过传入的 error 可区分（`errors.Is`）。降级本身有独立超时
`FallbackTimeout`（默认 1s），超时返回 `ErrFallbackTimeout`，不会挂死
调用方。调用方取消时不触发降级。

## 配置项一览

| 选项 | 默认值 | 说明 |
| --- | --- | --- |
| `WithFailureRateThreshold(p)` | 50 | 失败率阈值（%），窗口内达标则打开 |
| `WithSlowCallRateThreshold(p)` | 100 | 慢调用率阈值（%） |
| `WithSlowCallThreshold(d)` | 1s | 超过该时长的成功调用计为慢 |
| `WithMinimumCalls(n)` | 100 | 触发率判定的最小窗口调用数 |
| `WithConsecutiveFailures(n)` | 5 | 连续失败阈值，0 关闭 |
| `WithWindow(d, buckets)` | 10s / 10 | 滑动窗口时长与桶数 |
| `WithOpenTimeout(d)` | 60s | 打开 -> 半开的等待 |
| `WithHalfOpen(permits, concurrency, success)` | 10 / 10 / 3 | 试探放行数、并发、成功阈值 |
| `WithBulkhead(maxConcurrent, maxWait)` | 100 / 10 | 在飞上限与等待队列上限 |
| `WithTimeout(d)` | 2s | 每次尝试超时，0 关闭 |
| `WithRetry(attempts, base, max)` | 1 / 100ms / 2s | 最大尝试数与退避 |
| `WithRetryable(fn)` | 仅超时 | 判定可重试错误 |
| `WithFallbackTimeout(d)` | 1s | 降级超时 |
| `WithFallback[T](fn)` | 无 | 降级函数 |
| `WithClock(clk)` | 真实时钟 | 时钟（测试用假时钟） |

## 错误类型

| 错误 | 含义 | 是否计入熔断 |
| --- | --- | --- |
| `ErrCircuitOpen` | 熔断打开 | 否（计为 rejected） |
| `ErrHalfOpenBusy` | 试探放行已满 | 否（计为 rejected） |
| `ErrBulkheadFull` | bulkhead 队列满 | 否（计为 rejected） |
| `ErrTimeout` | 调用超时 | 是（失败） |
| `ErrFallbackTimeout` | 降级超时 | — |
| `context.Canceled` | 调用方取消 | 否 |

## 可观测性

`Dependency.Metrics()` 返回当前快照：状态、窗口内总数 / 成功 / 失败 / 慢
调用数、累计拒绝数、连续失败数、半开剩余 permit / 在飞 / 成功数。
`Dependency.BulkheadStats()` 返回当前在飞数与排队数。

## 测试

```sh
go test -race -count=1 ./...
```

测试使用 `FakeClock` 与内存桩 `FaultyDependency`，覆盖：状态转移、失败率 /
慢调用率 / 连续失败判定、半开试探（数量 / 并发 / 失败重开 / 槽位释放）、
bulkhead 并发与队列、超时、取消（含退避期间取消）、重试的单一结果语义、
降级与降级超时、时钟回拨与跳变、窗口过期、多依赖隔离、并发竞争下的状态
自洽（无超发、无重复放行）。

## 设计说明与局限

- Go 无法安全终止正在运行的 goroutine，因此超时 / 取消后调用方立即返回，
  后台尝试在结束时自行释放槽位；在飞数受 bulkhead 上界约束。
- 降级函数若无视 ctx 永久阻塞，会留下一个 goroutine（与业务调用同级）；
  降级超时保证调用方不被挂死。
- 半开状态下慢试探按成功处理；是否因慢重新打开由关闭后的慢调用率判定。
