package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"resilience/resilience"
)

func main() {
	rl := resilience.NewRateLimiter("rate-limiter-example",
		resilience.WithLimitForPeriod(3),
		resilience.WithLimitRefreshPeriod(time.Second),
		resilience.WithTimeoutDuration(0),
	)

	ctx := context.Background()
	for i := 1; i <= 8; i++ {
		err := rl.Wait(ctx)
		if err != nil {
			if errors.Is(err, resilience.ErrRateLimitExceeded) {
				log.Printf("request %d denied (rate limit exceeded)", i)
			} else {
				log.Printf("request %d failed: %v", i, err)
			}
		} else {
			log.Printf("request %d allowed", i)
		}
		time.Sleep(200 * time.Millisecond)
	}

	fmt.Printf("rate limiter metrics: %+v\n", rl.Metrics())
}
