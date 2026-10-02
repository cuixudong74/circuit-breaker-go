package circuitbreaker

import "time"

// bucket holds per-window-bucket counters.
type bucket struct {
	total   int64
	success int64
	failure int64
	slow    int64
}

// window is a count-based sliding window divided into fixed-duration buckets.
//
// Time handling: bucket boundaries are derived from the clock's monotonic
// readings. When the clock is driven backwards (tests), elapsed durations are
// negative and the whole window is reset, so a wall-clock rollback can never
// fabricate failures. When the last bucket ages out, the ring slot is reused
// and zeroed.
type window struct {
	clock    Clock
	span     time.Duration // total window duration
	bucket   time.Duration // per-bucket duration
	buckets  []bucket
	start    time.Time // start time of the current bucket 0
	curIndex int
}

func newWindow(clock Clock, span time.Duration, bucketCount int) *window {
	b := span / time.Duration(bucketCount)
	return &window{
		clock:   clock,
		span:    span,
		bucket:  b,
		buckets: make([]bucket, bucketCount),
		start:   clock.Now(),
	}
}

// advance rolls the window forward to now, zeroing expired buckets.
func (w *window) advance(now time.Time) {
	elapsed := now.Sub(w.start)
	if elapsed < 0 || elapsed >= w.span {
		// Clock went backwards or the whole window expired: fresh start.
		w.start = now
		w.buckets = make([]bucket, len(w.buckets))
		w.curIndex = 0
		return
	}
	idx := int(elapsed / w.bucket)
	if idx == w.curIndex {
		return
	}
	for i := w.curIndex + 1; i <= idx; i++ {
		w.buckets[i%len(w.buckets)] = bucket{}
	}
	w.curIndex = idx
}

func (w *window) add(success, failure, slow bool) {
	now := w.clock.Now()
	w.advance(now)
	b := &w.buckets[w.curIndex]
	b.total++
	switch {
	case failure:
		b.failure++
	case success:
		b.success++
		if slow {
			b.slow++
		}
	}
}

func (w *window) totals() (total, success, failure, slow int64) {
	for _, b := range w.buckets {
		total += b.total
		success += b.success
		failure += b.failure
		slow += b.slow
	}
	return
}

func (w *window) reset() {
	w.start = w.clock.Now()
	w.buckets = make([]bucket, len(w.buckets))
	w.curIndex = 0
}
