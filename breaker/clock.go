// Package breaker 提供故障隔离与熔断能力。
package breaker

import "time"

// Clock 抽象时钟，生产环境使用真实时钟，测试使用可控假时钟。
// 所有统计与状态转移只依赖该时钟，保证系统时间回拨不影响熔断语义。
type Clock interface {
	// Now 返回单调递增的当前时间（纳秒时间戳语义由实现保证）。
	Now() time.Time
	// Sleep 阻塞 d 或直到 stop 关闭。
	Sleep(d time.Duration, stop <-chan struct{}) bool
}

// RealClock 基于 time 包的真实时钟，time.Now() 含单调时钟读数。
type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }

func (RealClock) Sleep(d time.Duration, stop <-chan struct{}) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-stop:
		return false
	}
}
