package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"resilience/resilience"
)

func main() {
	cb := resilience.NewCircuitBreaker("decorator-cb",
		resilience.WithSlidingWindowSize(10),
		resilience.WithMinimumNumberOfCalls(5),
		resilience.WithFailureRateThreshold(60),
		resilience.WithWaitDurationInOpenState(2*time.Second),
	)
	retry := resilience.NewRetry("decorator-retry",
		resilience.WithMaxAttempts(3),
		resilience.WithWaitDuration(100*time.Millisecond),
	)
	bh := resilience.NewBulkhead("decorator-bh",
		resilience.WithMaxConcurrentCalls(10),
		resilience.WithMaxWaitDuration(10*time.Millisecond),
	)
	rl := resilience.NewRateLimiter("decorator-rl",
		resilience.WithLimitForPeriod(100),
		resilience.WithLimitRefreshPeriod(time.Second),
		resilience.WithTimeoutDuration(0),
	)

	var calls int32
	result, err := resilience.Decorate(func(context.Context) (interface{}, error) {
		current := atomic.AddInt32(&calls, 1)
		if current == 1 {
			return nil, errors.New("transient first call error")
		}
		return "decorator-chain-success", nil
	}).
		WithRateLimiter(rl).
		WithBulkhead(bh).
		WithCircuitBreaker(cb).
		WithRetry(retry).
		Call(context.Background())
	if err != nil {
		log.Fatalf("decorated call failed: %v", err)
	}

	fmt.Printf("result=%v total-calls=%d circuit-state=%s\n", result, calls, cb.State())
}
