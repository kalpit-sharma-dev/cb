package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"resilience/resilience"
)

func main() {
	bh := resilience.NewBulkhead("example-bulkhead",
		resilience.WithMaxConcurrentCalls(2),
		resilience.WithMaxWaitDuration(100*time.Millisecond),
	)

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := bh.Execute(context.Background(), func(context.Context) (interface{}, error) {
				time.Sleep(200 * time.Millisecond)
				return i, nil
			})
			if err != nil {
				if errors.Is(err, resilience.ErrBulkheadFull) {
					log.Printf("worker=%d rejected by bulkhead", i)
					return
				}
				log.Printf("worker=%d failed: %v", i, err)
				return
			}
			log.Printf("worker=%d completed", i)
		}()
	}
	wg.Wait()

	fmt.Printf("bulkhead metrics: %+v\n", bh.Metrics())
}
