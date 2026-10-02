package breaker

import (
	"sync"
	"time"
)

// FakeClock 可控假时钟，仅用于测试与本地复现。
// 时间只通过 Advance 前进，天然单调，可精确模拟时间跳变与回拨场景。
type FakeClock struct {
	mu      sync.Mutex
	now     time.Time
	waiters map[int]fakeWaiter
	nextID  int
}

type fakeWaiter struct {
	at time.Time
	ch chan struct{}
}

// NewFakeClock 创建从 start 开始的假时钟。
func NewFakeClock(start time.Time) *FakeClock {
	return &FakeClock{now: start, waiters: make(map[int]fakeWaiter)}
}

func (f *FakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// Advance 将时间前进 d，并唤醒所有到期的 Sleep。
// d 可为负（模拟回拨）：已注册的等待不会被触发，统计也不会凭空过期。
func (f *FakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
	for id, w := range f.waiters {
		if !f.now.Before(w.at) {
			close(w.ch)
			delete(f.waiters, id)
		}
	}
}

// Sleep 阻塞直到时钟前进满 d（返回 true）或 stop 关闭（返回 false）。
func (f *FakeClock) Sleep(d time.Duration, stop <-chan struct{}) bool {
	if d <= 0 {
		return true
	}
	f.mu.Lock()
	id := f.nextID
	f.nextID++
	ch := make(chan struct{})
	f.waiters[id] = fakeWaiter{at: f.now.Add(d), ch: ch}
	f.mu.Unlock()

	if stop == nil {
		<-ch
		return true
	}
	select {
	case <-ch:
		return true
	case <-stop:
		f.mu.Lock()
		delete(f.waiters, id)
		f.mu.Unlock()
		return false
	}
}

// Waiters 返回当前挂起的 Sleep 数量，主要用于测试同步。
func (f *FakeClock) Waiters() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.waiters)
}
