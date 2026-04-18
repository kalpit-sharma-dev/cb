package main

import (
	"context"
	"errors"
	"log"
	"time"

	"resilience/resilience"
)

type Repository struct {
	cb       *resilience.CircuitBreaker
	retry    *resilience.Retry
	bulkhead *resilience.Bulkhead
}

func NewRepository() *Repository {
	return &Repository{
		cb: resilience.NewCircuitBreaker("example-db-cb",
			resilience.WithSlidingWindowType(resilience.CountBased),
			resilience.WithSlidingWindowSize(20),
			resilience.WithMinimumNumberOfCalls(10),
			resilience.WithFailureRateThreshold(50),
			resilience.WithSlowCallDurationThreshold(200*time.Millisecond),
			resilience.WithSlowCallRateThreshold(60),
		),
		retry: resilience.NewRetry("example-db-retry",
			resilience.WithMaxAttempts(3),
			resilience.WithWaitDuration(30*time.Millisecond),
			resilience.WithRetryOn(func(err error) bool {
				if err == nil {
					return false
				}
				return err.Error() != "not found"
			}),
		),
		bulkhead: resilience.NewBulkhead("example-db-bh",
			resilience.WithMaxConcurrentCalls(16),
			resilience.WithMaxWaitDuration(25*time.Millisecond),
		),
	}
}

func (r *Repository) GetOrderStatus(ctx context.Context, id string) (string, error) {
	out, err := resilience.Decorate(func(context.Context) (interface{}, error) {
		// Stand-in for QueryRowContext / Scan.
		time.Sleep(25 * time.Millisecond)
		switch id {
		case "":
			return "", errors.New("invalid id")
		case "404":
			return "", errors.New("not found")
		default:
			return "PAID", nil
		}
	}).
		WithBulkhead(r.bulkhead).
		WithCircuitBreaker(r.cb).
		WithRetry(r.retry).
		Call(ctx)
	if err != nil {
		return "", err
	}
	status, ok := out.(string)
	if !ok {
		return "", errors.New("unexpected type from db call")
	}
	return status, nil
}

func main() {
	repo := NewRepository()

	for _, id := range []string{"1001", "404", "", "1002"} {
		status, err := repo.GetOrderStatus(context.Background(), id)
		if err != nil {
			log.Printf("id=%q db call failed: %v", id, err)
			continue
		}
		log.Printf("id=%q status=%s", id, status)
	}
}
