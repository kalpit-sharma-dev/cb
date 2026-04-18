package resilience

import (
	"context"
	"errors"
	"testing"
	"time"
)

func BenchmarkSlidingWindowCountBased_RecordAggregate(b *testing.B) {
	window := newCountBasedWindow(256)
	now := time.Unix(0, 0)
	record := callRecord{counted: true, success: true}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		now = now.Add(time.Millisecond)
		record.failed = i%7 == 0
		record.success = !record.failed
		record.slow = i%13 == 0
		record.failedForRate = record.failed || record.slow
		window.Record(now, record)
		_ = window.Aggregate(now)
	}
}

func BenchmarkSlidingWindowTimeBased_RecordAggregate(b *testing.B) {
	window := newTimeBasedWindow(60)
	now := time.Unix(0, 0)
	record := callRecord{counted: true, success: true}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		now = now.Add(100 * time.Millisecond)
		record.failed = i%9 == 0
		record.success = !record.failed
		record.slow = i%17 == 0
		record.failedForRate = record.failed || record.slow
		window.Record(now, record)
		_ = window.Aggregate(now)
	}
}

func BenchmarkDecorator_Call_NoComponents(b *testing.B) {
	decorator := Decorate(func(context.Context) (interface{}, error) { return "ok", nil })
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = decorator.Call(ctx)
	}
}

func BenchmarkDecorator_Call_FullChain(b *testing.B) {
	chain := benchmarkChain()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = chain.Call(ctx)
	}
}

func BenchmarkDecorator_ContentionParallel_FullChain(b *testing.B) {
	chain := benchmarkChain()

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		for pb.Next() {
			_, err := chain.Call(ctx)
			if err != nil && !errors.Is(err, ErrCircuitOpen) && !errors.Is(err, ErrBulkheadFull) && !errors.Is(err, ErrRateLimitExceeded) {
				b.Fatalf("unexpected chain error: %v", err)
			}
		}
	})
}

func BenchmarkCircuitBreaker_ContentionParallel(b *testing.B) {
	cb := NewCircuitBreaker("bench-cb-parallel",
		WithSlidingWindowType(CountBased),
		WithSlidingWindowSize(4096),
		WithMinimumNumberOfCalls(1_000_000),
		WithFailureRateThreshold(100),
		WithSlowCallRateThreshold(100),
		WithSlowCallDurationThreshold(time.Hour),
		WithPermittedNumberOfCallsInHalfOpenState(1024),
	)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		for pb.Next() {
			_, err := cb.Execute(ctx, func(context.Context) (interface{}, error) { return nil, nil })
			if err != nil {
				b.Fatalf("unexpected breaker error: %v", err)
			}
		}
	})
}

func BenchmarkBulkhead_ContentionParallel(b *testing.B) {
	bh := NewBulkhead("bench-bh-parallel",
		WithMaxConcurrentCalls(8192),
		WithMaxWaitDuration(0),
	)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		for pb.Next() {
			_, err := bh.Execute(ctx, func(context.Context) (interface{}, error) { return nil, nil })
			if err != nil {
				b.Fatalf("unexpected bulkhead error: %v", err)
			}
		}
	})
}

func BenchmarkRateLimiter_ContentionParallel(b *testing.B) {
	rl := NewRateLimiter("bench-rl-parallel",
		WithLimitForPeriod(5_000_000),
		WithLimitRefreshPeriod(time.Second),
		WithTimeoutDuration(0),
	)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		for pb.Next() {
			if err := rl.Wait(ctx); err != nil {
				b.Fatalf("unexpected rate limiter error: %v", err)
			}
		}
	})
}

func benchmarkChain() *Decorator {
	cb := NewCircuitBreaker("bench-cb",
		WithSlidingWindowType(CountBased),
		WithSlidingWindowSize(4096),
		WithMinimumNumberOfCalls(1_000_000),
		WithFailureRateThreshold(100),
		WithSlowCallRateThreshold(100),
		WithSlowCallDurationThreshold(time.Hour),
		WithPermittedNumberOfCallsInHalfOpenState(1024),
	)
	retry := NewRetry("bench-retry", WithMaxAttempts(1))
	bh := NewBulkhead("bench-bh", WithMaxConcurrentCalls(8192), WithMaxWaitDuration(0))
	rl := NewRateLimiter("bench-rl",
		WithLimitForPeriod(5_000_000),
		WithLimitRefreshPeriod(time.Second),
		WithTimeoutDuration(0),
	)

	return Decorate(func(context.Context) (interface{}, error) { return "ok", nil }).
		WithRateLimiter(rl).
		WithBulkhead(bh).
		WithCircuitBreaker(cb).
		WithRetry(retry)
}
