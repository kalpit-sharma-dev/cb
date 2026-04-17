package resilience

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(start time.Time) *fakeClock {
	return &fakeClock{now: start}
}

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeClock) Sleep(ctx context.Context, d time.Duration) error {
	if d > 0 {
		f.Advance(d)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func (f *fakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	f.now = f.now.Add(d)
	f.mu.Unlock()
}

type ignoredErr struct{}

func (ignoredErr) Error() string { return "ignored" }

type testListener struct {
	mu           sync.Mutex
	success      int
	failure      int
	slow         int
	ignored      int
	stateChanges []StateChangeEvent
}

func (l *testListener) OnSuccess(_ CallEvent) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.success++
}

func (l *testListener) OnFailure(_ CallEvent) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.failure++
}

func (l *testListener) OnSlowCall(_ CallEvent) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.slow++
}

func (l *testListener) OnIgnored(_ CallEvent) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ignored++
}

func (l *testListener) OnStateChange(event StateChangeEvent) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.stateChanges = append(l.stateChanges, event)
}

func TestCircuitBreaker_CountBased_OpensOnFailureRate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
	}{
		{name: "opens when failure threshold reached"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cb := NewCircuitBreaker("cb-count",
				WithSlidingWindowType(CountBased),
				WithSlidingWindowSize(4),
				WithMinimumNumberOfCalls(4),
				WithFailureRateThreshold(50),
				WithSlowCallDurationThreshold(time.Hour),
			)

			for i := 0; i < 2; i++ {
				_, _ = cb.Execute(context.Background(), func(context.Context) (interface{}, error) {
					return nil, errors.New("boom")
				})
			}
			for i := 0; i < 2; i++ {
				_, _ = cb.Execute(context.Background(), func(context.Context) (interface{}, error) {
					return nil, nil
				})
			}

			if cb.State() != StateOpen {
				t.Fatalf("expected OPEN, got %s", cb.State())
			}
		})
	}
}

func TestCircuitBreaker_TimeBased_OpensOnFailureRate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
	}{
		{name: "opens for time-based window"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			clk := newFakeClock(time.Unix(1000, 0))
			cb := NewCircuitBreaker("cb-time",
				WithClock(clk),
				WithSlidingWindowType(TimeBased),
				WithSlidingWindowSize(3),
				WithMinimumNumberOfCalls(3),
				WithFailureRateThreshold(50),
				WithSlowCallDurationThreshold(time.Hour),
			)

			for i := 0; i < 2; i++ {
				_, _ = cb.Execute(context.Background(), func(context.Context) (interface{}, error) {
					return nil, errors.New("boom")
				})
				clk.Advance(time.Second)
			}
			_, _ = cb.Execute(context.Background(), func(context.Context) (interface{}, error) {
				return nil, nil
			})

			if cb.State() != StateOpen {
				t.Fatalf("expected OPEN, got %s", cb.State())
			}
		})
	}
}

func TestCircuitBreaker_SlowCallRateOpensBreaker(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
	}{
		{name: "slow call rate opens breaker"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			clk := newFakeClock(time.Unix(0, 0))
			cb := NewCircuitBreaker("cb-slow",
				WithClock(clk),
				WithSlidingWindowSize(4),
				WithMinimumNumberOfCalls(4),
				WithFailureRateThreshold(100),
				WithSlowCallDurationThreshold(50*time.Millisecond),
				WithSlowCallRateThreshold(50),
			)

			for i := 0; i < 2; i++ {
				_, _ = cb.Execute(context.Background(), func(context.Context) (interface{}, error) {
					clk.Advance(60 * time.Millisecond)
					return nil, nil
				})
			}
			for i := 0; i < 2; i++ {
				_, _ = cb.Execute(context.Background(), func(context.Context) (interface{}, error) {
					return nil, nil
				})
			}

			if cb.State() != StateOpen {
				t.Fatalf("expected OPEN, got %s", cb.State())
			}
		})
	}
}

func TestCircuitBreaker_HalfOpen_ClosesOnSuccess(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
	}{
		{name: "half open closes when probes pass"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cb := NewCircuitBreaker("cb-half-close",
				WithSlidingWindowSize(2),
				WithMinimumNumberOfCalls(2),
				WithFailureRateThreshold(50),
				WithPermittedNumberOfCallsInHalfOpenState(2),
				WithWaitDurationInOpenState(20*time.Millisecond),
			)
			for i := 0; i < 2; i++ {
				_, _ = cb.Execute(context.Background(), func(context.Context) (interface{}, error) {
					return nil, errors.New("fail")
				})
			}

			time.Sleep(30 * time.Millisecond)
			for i := 0; i < 2; i++ {
				_, err := cb.Execute(context.Background(), func(context.Context) (interface{}, error) {
					return nil, nil
				})
				if err != nil {
					t.Fatalf("expected probe success, got %v", err)
				}
			}
			if cb.State() != StateClosed {
				t.Fatalf("expected CLOSED, got %s", cb.State())
			}
		})
	}
}

func TestCircuitBreaker_HalfOpen_ReOpensOnFailure(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
	}{
		{name: "failed probe reopens"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cb := NewCircuitBreaker("cb-half-open",
				WithSlidingWindowSize(2),
				WithMinimumNumberOfCalls(2),
				WithFailureRateThreshold(50),
				WithPermittedNumberOfCallsInHalfOpenState(2),
				WithWaitDurationInOpenState(20*time.Millisecond),
			)
			for i := 0; i < 2; i++ {
				_, _ = cb.Execute(context.Background(), func(context.Context) (interface{}, error) {
					return nil, errors.New("fail")
				})
			}

			time.Sleep(30 * time.Millisecond)
			_, _ = cb.Execute(context.Background(), func(context.Context) (interface{}, error) {
				return nil, errors.New("probe fail")
			})
			if cb.State() != StateOpen {
				t.Fatalf("expected OPEN, got %s", cb.State())
			}
		})
	}
}

func TestCircuitBreaker_IgnoreErrors_DoNotCount(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
	}{
		{name: "ignored errors are excluded"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cb := NewCircuitBreaker("cb-ignore",
				WithSlidingWindowSize(4),
				WithMinimumNumberOfCalls(2),
				WithFailureRateThreshold(50),
				WithIgnoreErrors(func(err error) bool {
					var ie ignoredErr
					return errors.As(err, &ie)
				}),
			)
			for i := 0; i < 4; i++ {
				_, _ = cb.Execute(context.Background(), func(context.Context) (interface{}, error) {
					return nil, ignoredErr{}
				})
			}
			if got := cb.Metrics().NumberOfBufferedCalls; got != 0 {
				t.Fatalf("expected 0 buffered calls, got %d", got)
			}
		})
	}
}

func TestCircuitBreaker_RecordErrors_Whitelist(t *testing.T) {
	t.Parallel()
	retryable := errors.New("retryable")
	other := errors.New("other")
	tests := []struct {
		name string
	}{
		{name: "only whitelisted errors fail"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cb := NewCircuitBreaker("cb-record",
				WithSlidingWindowSize(4),
				WithMinimumNumberOfCalls(10),
				WithFailureRateThreshold(50),
				WithRecordErrors(func(err error) bool { return errors.Is(err, retryable) }),
			)
			for _, errVal := range []error{retryable, other, nil, nil} {
				_, _ = cb.Execute(context.Background(), func(context.Context) (interface{}, error) {
					return nil, errVal
				})
			}
			if got := cb.Metrics().NumberOfFailedCalls; got != 1 {
				t.Fatalf("expected 1 failed call, got %d", got)
			}
		})
	}
}

func TestCircuitBreaker_MetricsSnapshot(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
	}{
		{name: "snapshot exposes counters and rates"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			clk := newFakeClock(time.Unix(0, 0))
			cb := NewCircuitBreaker("cb-metrics",
				WithClock(clk),
				WithSlidingWindowSize(5),
				WithMinimumNumberOfCalls(10),
				WithFailureRateThreshold(100),
				WithSlowCallDurationThreshold(20*time.Millisecond),
				WithSlowCallRateThreshold(100),
			)
			_, _ = cb.Execute(context.Background(), func(context.Context) (interface{}, error) {
				return nil, errors.New("fail")
			})
			_, _ = cb.Execute(context.Background(), func(context.Context) (interface{}, error) {
				clk.Advance(30 * time.Millisecond)
				return nil, nil
			})
			_, _ = cb.Execute(context.Background(), func(context.Context) (interface{}, error) {
				return nil, nil
			})

			m := cb.Metrics()
			if m.NumberOfBufferedCalls != 3 || m.NumberOfFailedCalls != 1 || m.NumberOfSlowCalls != 1 || m.NumberOfSuccessfulCalls != 2 {
				t.Fatalf("unexpected metrics: %+v", m)
			}
		})
	}
}

func TestCircuitBreaker_EventsPublished(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
	}{
		{name: "success failure slow ignored and state events"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			clk := newFakeClock(time.Unix(0, 0))
			cb := NewCircuitBreaker("cb-events",
				WithClock(clk),
				WithSlidingWindowSize(4),
				WithMinimumNumberOfCalls(3),
				WithFailureRateThreshold(66),
				WithSlowCallDurationThreshold(10*time.Millisecond),
				WithSlowCallRateThreshold(90),
				WithIgnoreErrors(func(err error) bool {
					var ie ignoredErr
					return errors.As(err, &ie)
				}),
			)
			listener := &testListener{}
			cb.AddListener(listener)

			_, _ = cb.Execute(context.Background(), func(context.Context) (interface{}, error) {
				return nil, errors.New("fail")
			})
			_, _ = cb.Execute(context.Background(), func(context.Context) (interface{}, error) {
				clk.Advance(11 * time.Millisecond)
				return nil, nil
			})
			_, _ = cb.Execute(context.Background(), func(context.Context) (interface{}, error) {
				return nil, ignoredErr{}
			})

			listener.mu.Lock()
			defer listener.mu.Unlock()
			if listener.failure == 0 || listener.slow == 0 || listener.ignored == 0 {
				t.Fatalf("missing events: %+v", listener)
			}
		})
	}
}

func TestRetry_ExponentialBackoff(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		failures     int
		wantAttempts int32
		wantElapsed  time.Duration
	}{
		{name: "backoff grows exponentially", failures: 2, wantAttempts: 3, wantElapsed: 300 * time.Millisecond},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			clk := newFakeClock(time.Unix(0, 0))
			r := NewRetry("retry-exp",
				WithClock(clk),
				WithMaxAttempts(4),
				WithWaitDuration(100*time.Millisecond),
				WithExponentialBackoff(2.0, 500*time.Millisecond),
			)
			var attempts int32
			_, err := r.Execute(context.Background(), func(context.Context) (interface{}, error) {
				n := atomic.AddInt32(&attempts, 1)
				if int(n) <= tt.failures {
					return nil, errors.New("retry")
				}
				return "ok", nil
			})
			if err != nil {
				t.Fatalf("expected success, got %v", err)
			}
			if attempts != tt.wantAttempts {
				t.Fatalf("expected %d attempts, got %d", tt.wantAttempts, attempts)
			}
			if elapsed := clk.Now().Sub(time.Unix(0, 0)); elapsed != tt.wantElapsed {
				t.Fatalf("expected elapsed %v, got %v", tt.wantElapsed, elapsed)
			}
		})
	}
}

func TestRetry_RespectsRetryOnPredicate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		shouldRetry  bool
		wantAttempts int32
	}{
		{name: "predicate false stops", shouldRetry: false, wantAttempts: 1},
		{name: "predicate true retries", shouldRetry: true, wantAttempts: 3},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := NewRetry("retry-predicate",
				WithMaxAttempts(3),
				WithRetryOn(func(error) bool { return tt.shouldRetry }),
			)
			var attempts int32
			_, _ = r.Execute(context.Background(), func(context.Context) (interface{}, error) {
				atomic.AddInt32(&attempts, 1)
				return nil, errors.New("err")
			})
			if attempts != tt.wantAttempts {
				t.Fatalf("expected %d attempts, got %d", tt.wantAttempts, attempts)
			}
		})
	}
}

func TestBulkhead_RejectsBeyondConcurrencyLimit(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
	}{
		{name: "rejects when full"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			bh := NewBulkhead("bh", WithMaxConcurrentCalls(1), WithMaxWaitDuration(0))
			entered := make(chan struct{})
			release := make(chan struct{})

			go func() {
				_, _ = bh.Execute(context.Background(), func(context.Context) (interface{}, error) {
					close(entered)
					<-release
					return nil, nil
				})
			}()
			<-entered

			_, err := bh.Execute(context.Background(), func(context.Context) (interface{}, error) {
				return nil, nil
			})
			if !errors.Is(err, ErrBulkheadFull) {
				t.Fatalf("expected ErrBulkheadFull, got %v", err)
			}
			close(release)
		})
	}
}

func TestRateLimiter_BlocksBeyondLimit(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
	}{
		{name: "second permit denied"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rl := NewRateLimiter("rl",
				WithLimitForPeriod(1),
				WithLimitRefreshPeriod(time.Second),
				WithTimeoutDuration(0),
			)
			if err := rl.Wait(context.Background()); err != nil {
				t.Fatalf("unexpected first wait error: %v", err)
			}
			if err := rl.Wait(context.Background()); !errors.Is(err, ErrRateLimitExceeded) {
				t.Fatalf("expected ErrRateLimitExceeded, got %v", err)
			}
		})
	}
}

func TestDecorator_FullChain(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
	}{
		{name: "decorator chain retries and succeeds"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			clk := newFakeClock(time.Unix(0, 0))
			cb := NewCircuitBreaker("cb", WithClock(clk), WithSlidingWindowSize(10), WithMinimumNumberOfCalls(10))
			r := NewRetry("retry", WithClock(clk), WithMaxAttempts(3), WithWaitDuration(10*time.Millisecond))
			bh := NewBulkhead("bh", WithMaxConcurrentCalls(2))
			rl := NewRateLimiter("rl", WithLimitForPeriod(10), WithLimitRefreshPeriod(time.Second))
			var attempts int32

			result, err := Decorate(func(context.Context) (interface{}, error) {
				if atomic.AddInt32(&attempts, 1) == 1 {
					return nil, errors.New("fail once")
				}
				return "ok", nil
			}).
				WithRateLimiter(rl).
				WithBulkhead(bh).
				WithCircuitBreaker(cb).
				WithRetry(r).
				Call(context.Background())
			if err != nil {
				t.Fatalf("expected success, got %v", err)
			}
			if result != "ok" {
				t.Fatalf("expected ok, got %v", result)
			}
			if attempts != 2 {
				t.Fatalf("expected 2 attempts, got %d", attempts)
			}
		})
	}
}
