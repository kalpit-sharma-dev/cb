package resilience

import (
	"context"
	"sync"
	"time"
)

// SlidingWindowType chooses how calls are aggregated.
type SlidingWindowType string

const (
	// CountBased tracks the last N calls.
	CountBased SlidingWindowType = "COUNT_BASED"
	// TimeBased tracks calls that happened in the last N seconds.
	TimeBased SlidingWindowType = "TIME_BASED"
)

// Clock abstracts time for tests.
type Clock interface {
	Now() time.Time
	Sleep(ctx context.Context, d time.Duration) error
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) Sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type callRecord struct {
	counted       bool
	success       bool
	failed        bool
	slow          bool
	failedForRate bool
}

type aggregate struct {
	total         int
	failed        int
	failedForRate int
	slow          int
	success       int
	failureRate   float64
	slowRate      float64
}

type slidingWindow interface {
	Record(now time.Time, record callRecord) aggregate
	Aggregate(now time.Time) aggregate
	Reset()
}

type countBasedWindow struct {
	mu      sync.Mutex
	records []callRecord
	next    int
	filled  int
	sum     aggregate
}

func newCountBasedWindow(size int) *countBasedWindow {
	if size < 1 {
		size = 1
	}
	return &countBasedWindow{records: make([]callRecord, size)}
}

func (w *countBasedWindow) Record(_ time.Time, record callRecord) aggregate {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.filled == len(w.records) {
		old := w.records[w.next]
		w.remove(old)
	} else {
		w.filled++
	}

	w.records[w.next] = record
	w.add(record)
	w.next = (w.next + 1) % len(w.records)
	w.recomputeRates()
	return w.sum
}

func (w *countBasedWindow) Aggregate(_ time.Time) aggregate {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.recomputeRates()
	return w.sum
}

func (w *countBasedWindow) Reset() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.records = make([]callRecord, len(w.records))
	w.next = 0
	w.filled = 0
	w.sum = aggregate{}
}

func (w *countBasedWindow) add(record callRecord) {
	if !record.counted {
		return
	}
	w.sum.total++
	if record.failed {
		w.sum.failed++
	}
	if record.failedForRate {
		w.sum.failedForRate++
	}
	if record.slow {
		w.sum.slow++
	}
	if record.success {
		w.sum.success++
	}
}

func (w *countBasedWindow) remove(record callRecord) {
	if !record.counted {
		return
	}
	w.sum.total--
	if record.failed {
		w.sum.failed--
	}
	if record.failedForRate {
		w.sum.failedForRate--
	}
	if record.slow {
		w.sum.slow--
	}
	if record.success {
		w.sum.success--
	}
}

func (w *countBasedWindow) recomputeRates() {
	if w.sum.total == 0 {
		w.sum.failureRate = 0
		w.sum.slowRate = 0
		return
	}
	w.sum.failureRate = float64(w.sum.failedForRate) * 100 / float64(w.sum.total)
	w.sum.slowRate = float64(w.sum.slow) * 100 / float64(w.sum.total)
}

type timeBucket struct {
	second int64
	agg    aggregate
}

type timeBasedWindow struct {
	mu      sync.Mutex
	buckets []timeBucket
	total   aggregate
}

func newTimeBasedWindow(size int) *timeBasedWindow {
	if size < 1 {
		size = 1
	}
	return &timeBasedWindow{buckets: make([]timeBucket, size)}
}

func (w *timeBasedWindow) Record(now time.Time, record callRecord) aggregate {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.evictExpired(now)
	idx := int(now.Unix() % int64(len(w.buckets)))
	sec := now.Unix()
	bucket := &w.buckets[idx]
	if bucket.second != sec {
		w.subtract(bucket.agg)
		bucket.second = sec
		bucket.agg = aggregate{}
	}

	inc := aggregate{}
	if record.counted {
		inc.total = 1
		if record.failed {
			inc.failed = 1
		}
		if record.failedForRate {
			inc.failedForRate = 1
		}
		if record.slow {
			inc.slow = 1
		}
		if record.success {
			inc.success = 1
		}
	}

	bucket.agg.total += inc.total
	bucket.agg.failed += inc.failed
	bucket.agg.failedForRate += inc.failedForRate
	bucket.agg.slow += inc.slow
	bucket.agg.success += inc.success

	w.add(inc)
	w.recomputeRates()
	return w.total
}

func (w *timeBasedWindow) Aggregate(now time.Time) aggregate {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.evictExpired(now)
	w.recomputeRates()
	return w.total
}

func (w *timeBasedWindow) Reset() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buckets = make([]timeBucket, len(w.buckets))
	w.total = aggregate{}
}

func (w *timeBasedWindow) evictExpired(now time.Time) {
	min := now.Unix() - int64(len(w.buckets)) + 1
	for i := range w.buckets {
		bucket := &w.buckets[i]
		if bucket.second == 0 || bucket.second >= min {
			continue
		}
		w.subtract(bucket.agg)
		bucket.second = 0
		bucket.agg = aggregate{}
	}
}

func (w *timeBasedWindow) add(agg aggregate) {
	w.total.total += agg.total
	w.total.failed += agg.failed
	w.total.failedForRate += agg.failedForRate
	w.total.slow += agg.slow
	w.total.success += agg.success
}

func (w *timeBasedWindow) subtract(agg aggregate) {
	w.total.total -= agg.total
	w.total.failed -= agg.failed
	w.total.failedForRate -= agg.failedForRate
	w.total.slow -= agg.slow
	w.total.success -= agg.success
}

func (w *timeBasedWindow) recomputeRates() {
	if w.total.total == 0 {
		w.total.failureRate = 0
		w.total.slowRate = 0
		return
	}
	w.total.failureRate = float64(w.total.failedForRate) * 100 / float64(w.total.total)
	w.total.slowRate = float64(w.total.slow) * 100 / float64(w.total.total)
}
