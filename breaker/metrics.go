package breaker

import "time"

// counts 单个时间桶内的统计。
type counts struct {
	total   int
	failure int
	slow    int
}

func (c *counts) add(o counts) {
	c.total += o.total
	c.failure += o.failure
	c.slow += o.slow
}

// windowMetrics 基于单调时钟的滑动窗口统计。
//
// 窗口被均分为 N 个桶，桶按时间顺序环形复用。过期判定只依赖
// Clock 提供的单调时间：系统时间回拨不会影响 Now() 的单调性
// （RealClock 使用 time.Now() 的单调读数），因此不会凭空过期
// 或长期滞留。时间回拨在假时钟下表现为 now 不变或前进变慢，
// 桶边界依然确定：桶 i 覆盖 [start_i, start_i+bucketDur)。
type windowMetrics struct {
	clock     Clock
	bucketDur time.Duration
	buckets   []counts
	starts    []time.Time // 每个桶的起始时间，零值表示未启用
	head      int         // 当前（最新）桶下标
}

func newWindowMetrics(clk Clock, window time.Duration, buckets int) *windowMetrics {
	return &windowMetrics{
		clock:     clk,
		bucketDur: window / time.Duration(buckets),
		buckets:   make([]counts, buckets),
		starts:    make([]time.Time, buckets),
	}
}

// advance 将 head 推进到 now 所属的桶，并重置途中跳过的过期桶。
// 调用方必须持锁。
func (m *windowMetrics) advance(now time.Time) {
	cur := m.starts[m.head]
	if cur.IsZero() {
		m.starts[m.head] = now.Truncate(m.bucketDur)
		return
	}
	// now 落后于当前桶起点（时钟回拨）：归入当前桶，不做任何滚动，
	// 保证不凭空清空统计。
	if now.Before(cur) {
		return
	}
	for !now.Before(cur.Add(m.bucketDur)) {
		m.head = (m.head + 1) % len(m.buckets)
		cur = cur.Add(m.bucketDur)
		m.buckets[m.head] = counts{}
		m.starts[m.head] = cur
	}
}

// record 记录一次调用结果。failed/slow 互不为包含关系，可同时为真。
// 调用方必须持锁。
func (m *windowMetrics) record(now time.Time, failed, slow bool) {
	m.advance(now)
	m.buckets[m.head].total++
	if failed {
		m.buckets[m.head].failure++
	}
	if slow {
		m.buckets[m.head].slow++
	}
}

// snapshot 返回当前窗口内的聚合统计（仅未过期桶）。
// 调用方必须持锁。
func (m *windowMetrics) snapshot(now time.Time) counts {
	m.advance(now)
	var out counts
	windowStart := m.starts[m.head].Add(m.bucketDur).Add(-time.Duration(len(m.buckets)) * m.bucketDur)
	for i := range m.buckets {
		if m.starts[i].IsZero() || m.starts[i].Before(windowStart) {
			continue
		}
		out.add(m.buckets[i])
	}
	return out
}
