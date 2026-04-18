package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"resilience/resilience"
)

func main() {
	cb := resilience.NewCircuitBreaker("manual-cb",
		resilience.WithSlidingWindowType(resilience.CountBased),
		resilience.WithSlidingWindowSize(4),
		resilience.WithMinimumNumberOfCalls(4),
		resilience.WithFailureRateThreshold(50),
		resilience.WithWaitDurationInOpenState(2*time.Second),
		resilience.WithPermittedNumberOfCallsInHalfOpenState(2),
		resilience.WithAutomaticTransitionFromOpenToHalfOpen(false),
	)

	fmt.Printf("initial state: %s\n", cb.State())
	for i := 0; i < 4; i++ {
		_, _ = cb.Execute(context.Background(), func(context.Context) (interface{}, error) {
			return nil, errors.New("dependency failure")
		})
	}
	fmt.Printf("state after failures: %s\n", cb.State())

	_, err := cb.Execute(context.Background(), func(context.Context) (interface{}, error) {
		return "unexpected", nil
	})
	fmt.Printf("call while OPEN before transition: %v\n", err)

	if ok := cb.TransitionToHalfOpen(); !ok {
		fmt.Println("transition request rejected")
		return
	}
	fmt.Println("manual transition requested")

	for i := 0; i < 2; i++ {
		_, err := cb.Execute(context.Background(), func(context.Context) (interface{}, error) {
			return "probe-ok", nil
		})
		if err != nil {
			fmt.Printf("probe %d failed: %v\n", i+1, err)
			return
		}
	}

	fmt.Printf("final state: %s\n", cb.State())
}
