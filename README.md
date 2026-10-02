# circuit-breaker-go

进程内的故障隔离与熔断库。按依赖分组隔离调用，提供熔断状态机、半开试探、
超时/取消/重试、降级兜底与基于单调时钟的滑动窗口统计。配套内存调用桩
（`stub` 包）可在本地复现依赖的超时、报错、变慢与恢复，全链路不依赖任何
真实外部服务。

## 快速开始

```go
br := breaker.New(breaker.Config{
    Name:                "payment",
    MaxConcurrent:       64,               // 舱壁：该依赖最大并发在途
    Timeout:             200 * time.Millisecond,
    ConsecutiveFailures: 5,
    FailureRatio:        0.5,
    SlowThreshold:       time.Second,
    SlowRatio:           0.5,
    OpenDuration:        5 * time.Second,
    HalfOpenMaxConcurrent: 2,
    HalfOpenSuccesses:   2,
    MaxAttempts:         2,
    RetryBackoff:        50 * time.Millisecond,
}, nil) // nil 使用真实单调时钟；测试可传 breaker.NewFakeClock

ex := breaker.NewExecutor(br, func(ctx context.Context, reason breaker.FallbackReason, err error) (any, error) {
    return cachedValue(reason), nil // 降级路径，reason 可区分降级原因
})

res := ex.Do(ctx, dep.Call) // dep 可以是 stub.Dependency 或任何同签名函数
```

## 熔断状态机语义

三个状态：**关闭（closed）→ 打开（open）→ 半开（half-open）→ 关闭**。
所有转移在互斥锁内完成，多调用方并发触发时任一时刻状态与计数自洽。

### 关闭 → 打开（满足任一条件即转移）

1. **连续失败**：连续失败次数 `>= ConsecutiveFailures`（成功即清零，取消不计）。
2. **失败率**：窗口内请求数 `>= MinRequests` 且 `失败数/总数 >= FailureRatio`。
3. **慢调用比例**：窗口内请求数 `>= MinRequests` 且 `慢调用数/总数 >= SlowRatio`。
   单次调用耗时 `>= SlowThreshold` 记为慢调用（慢调用若同时失败，两者都计）。

### 打开 → 半开

打开持续 `OpenDuration` 后**惰性转移**：下一个到达的请求在判定时触发转移
（无流量时不空转，转移时刻确定）。打开期间所有请求以 `ErrOpen` 快速拒绝，
不再调用依赖。

### 半开 → 关闭 / 打开

- 半开状态最多同时放行 `HalfOpenMaxConcurrent` 个试探请求，超出以
  `ErrHalfOpenLimited` 拒绝——试探流量不会被放大成全量流量。
- 连续成功 `>= HalfOpenSuccesses` → 关闭，并重置全部统计与计数
  （避免陈旧数据立即再次触发熔断）。
- 任一试探失败 → **立即**回到打开，重新计时 `OpenDuration`。

### 计数口径

- 一次 `Executor.Do` 逻辑调用无论内部重试几次，**只反馈一个最终结果**。
- 调用方 `ctx` 取消：不计入任何统计，不触发降级，返回 `ErrCanceled`。
- 单次尝试超时：按失败计入（可重试），最终结果为超时则降级原因记 `timeout`。
- 慢调用耗时口径：从首次尝试开始到最终结果产出的单调时钟耗时。
- 打开前已放行的在途调用仍计入统计，但不触发状态转移。

## 故障隔离（舱壁）

每个依赖一个独立 `Breaker`，`MaxConcurrent` 限制该依赖的最大并发在途调用，
超额请求以 `ErrBulkheadFull` 快速失败（走降级）。单个依赖熔断或变慢不影响
其他依赖的熔断器与槽位，调用方的协程与等待队列不会无界增长：
等待只发生在单次尝试的 `Timeout` 与重试退避内，均有界。

## 单调时钟与时间跳变

- 所有统计与状态转移只依赖注入的 `Clock`。`RealClock` 使用 `time.Now()` 的
  单调读数，系统时间被调整或回拨不影响熔断判定。
- 滑动窗口均分为 `Bucket` 个桶，桶 `i` 覆盖 `[start_i, start_i+Window/Bucket)`，
  边界确定；`now` 落后于当前桶起点（回拨）时不滚动、不清空统计。
- 打开状态的恢复判定同样基于单调时钟：回拨只会推迟到期点，不会凭空熔断，
  也不会永久卡死——重新前进满 `OpenDuration` 后必然允许试探。
- 测试使用 `FakeClock`（只能通过 `Advance` 前进/回拨），配合内存通道，
  全部场景可重复复现。

## 配置项

| 字段 | 默认 | 说明 |
| --- | --- | --- |
| `Name` | `""` | 依赖分组名（每个依赖一个 Breaker） |
| `Window` | 10s | 滑动窗口总长度 |
| `Bucket` | 10 | 窗口分桶数 |
| `MinRequests` | 10 | 窗口内触发比例判定的最小请求数 |
| `FailureRatio` | 0.5 | 失败率阈值，达到即打开 |
| `SlowThreshold` | 1s | 慢调用耗时阈值 |
| `SlowRatio` | 0.5 | 慢调用比例阈值，达到即打开 |
| `ConsecutiveFailures` | 5 | 连续失败打开阈值 |
| `OpenDuration` | 5s | 打开持续时间，到期转半开 |
| `HalfOpenMaxConcurrent` | 1 | 半开最大并发试探数 |
| `HalfOpenSuccesses` | 1 | 半开连续成功关闭阈值 |
| `MaxConcurrent` | 64 | 舱壁：最大并发在途调用 |
| `Timeout` | 0（不限） | 单次尝试超时 |
| `MaxAttempts` | 1 | 一次逻辑调用最大尝试次数（含首次） |
| `RetryBackoff` | 0 | 重试退避基数，第 n 次重试等待 `backoff*n` |

## 降级原因

`Result.Reason` 区分降级路径：`open`（熔断打开）、`half_open_limited`
（半开限流）、`bulkhead_full`（舱壁已满）、`timeout`（超时）、
`failure`（依赖报错）、`none`（未降级）。降级函数自身出错时错误透传给
调用方，不会挂死。

## 本地复现

```sh
go test ./... -race -count=1
```

测试覆盖：状态转移（连续失败/失败率/慢比例/最小请求数门限）、并发竞争
（舱壁不超发、半开试探不超发、转移不抖动）、试探放行与回开、超时与取消
（取消不污染计数）、重试单次反馈、降级原因区分、时间前进/回拨跳变，
以及"报错→熔断→降级→试探→恢复"的端到端链路。全部使用 `FakeClock`
与 `stub.Dependency` 内存桩，可重复、无外部依赖。
