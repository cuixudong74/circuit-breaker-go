// Package stub 提供进程内的依赖调用桩，可编程模拟超时、报错、变慢与恢复。
// 全部通过内存通道与可控时钟完成，不访问任何真实外部服务。
package stub

import (
	"context"
	"sync"
	"time"
)

// Behavior 调用桩行为。
type Behavior struct {
	Latency time.Duration // 每次调用的模拟耗时
	Err     error         // 非 nil 时调用返回该错误
	Hang    bool          // 挂起直到 ctx 取消（模拟无响应依赖）
}

// Dependency 可编程的内存依赖桩，并发安全。
type Dependency struct {
	mu     sync.Mutex
	script []Behavior // 按调用顺序消费，用完后重复最后一个
	calls  int
}

// New 创建一个默认立即成功的依赖桩。
func New() *Dependency { return &Dependency{} }

// Set 设置后续所有调用的固定行为。
func (d *Dependency) Set(b Behavior) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.script = []Behavior{b}
}

// SetScript 设置按调用顺序消费的行为脚本（第 1 次调用取 script[0]，依此类推，
// 超出长度后重复最后一个），可用于模拟"先报错后恢复"等场景。
func (d *Dependency) SetScript(script []Behavior) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.script = append([]Behavior(nil), script...)
}

// Calls 返回已收到的调用总数。
func (d *Dependency) Calls() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.calls
}

// next 取出本次调用的行为并计数。
func (d *Dependency) next() Behavior {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls++
	if len(d.script) == 0 {
		return Behavior{}
	}
	i := d.calls - 1
	if i >= len(d.script) {
		i = len(d.script) - 1
	}
	return d.script[i]
}

// Call 模拟一次依赖调用，行为由当前脚本决定。
// Latency 通过定时器模拟，ctx 取消会立即中断等待。
func (d *Dependency) Call(ctx context.Context) (any, error) {
	b := d.next()
	if b.Hang {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if b.Latency > 0 {
		t := time.NewTimer(b.Latency)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return nil, ctx.Err()
		}
	}
	if b.Err != nil {
		return nil, b.Err
	}
	return "ok", nil
}
