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
	retry := resilience.NewRetry("retry-example",
		resilience.WithMaxAttempts(4),
		resilience.WithWaitDuration(200*time.Millisecond),
		resilience.WithExponentialBackoff(2.0, 2*time.Second),
		resilience.WithRetryOn(func(err error) bool {
			return err != nil && !errors.Is(err, context.Canceled)
		}),
		resilience.WithOnRetry(func(attempt int, err error) {
			log.Printf("retrying after attempt=%d err=%v", attempt, err)
		}),
	)

	var calls int
	result, err := retry.Execute(context.Background(), func(context.Context) (interface{}, error) {
		calls++
		if calls < 3 {
			return nil, errors.New("transient upstream timeout")
		}
		return "retried-successfully", nil
	})
	if err != nil {
		log.Fatalf("retry failed: %v", err)
	}

	fmt.Printf("result=%v attempts=%d metrics=%+v\n", result, calls, retry.Metrics())
}
