package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"time"

	"resilience/resilience"
)

// PaymentClient demonstrates how to apply resilience controls around outbound REST calls.
type PaymentClient struct {
	httpClient *http.Client
	cb         *resilience.CircuitBreaker
	retry      *resilience.Retry
	bulkhead   *resilience.Bulkhead
	rate       *resilience.RateLimiter
}

func NewPaymentClient(baseTimeout time.Duration) *PaymentClient {
	return &PaymentClient{
		httpClient: &http.Client{Timeout: baseTimeout},
		cb: resilience.NewCircuitBreaker("payment-http",
			resilience.WithSlidingWindowType(resilience.TimeBased),
			resilience.WithSlidingWindowSize(10),
			resilience.WithMinimumNumberOfCalls(10),
			resilience.WithFailureRateThreshold(50),
			resilience.WithSlowCallDurationThreshold(300*time.Millisecond),
			resilience.WithSlowCallRateThreshold(70),
			resilience.WithWaitDurationInOpenState(2*time.Second),
		),
		retry: resilience.NewRetry("payment-retry",
			resilience.WithMaxAttempts(3),
			resilience.WithWaitDuration(50*time.Millisecond),
			resilience.WithExponentialBackoff(2.0, 400*time.Millisecond),
			resilience.WithRetryOn(func(err error) bool {
				return err != nil && !errors.Is(err, context.Canceled)
			}),
		),
		bulkhead: resilience.NewBulkhead("payment-bulkhead",
			resilience.WithMaxConcurrentCalls(64),
			resilience.WithMaxWaitDuration(20*time.Millisecond),
		),
		rate: resilience.NewRateLimiter("payment-rate",
			resilience.WithLimitForPeriod(200),
			resilience.WithLimitRefreshPeriod(time.Second),
			resilience.WithTimeoutDuration(25*time.Millisecond),
		),
	}
}

func (c *PaymentClient) Charge(ctx context.Context, req *http.Request) (map[string]any, error) {
	result, err := resilience.Decorate(func(ctx context.Context) (interface{}, error) {
		resp, err := c.httpClient.Do(req.WithContext(ctx))
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 500 {
			return nil, fmt.Errorf("upstream 5xx (%d)", resp.StatusCode)
		}
		if resp.StatusCode >= 400 {
			return nil, nil
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			return nil, err
		}
		return payload, nil
	}).
		WithRateLimiter(c.rate).
		WithBulkhead(c.bulkhead).
		WithCircuitBreaker(c.cb).
		WithRetry(c.retry).
		Call(ctx)
	if err != nil {
		return nil, err
	}
	return result.(map[string]any), nil
}

func main() {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("fail") == "1" {
			http.Error(w, "simulated upstream failure", http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":    "ok",
			"chargedAt": time.Now().UTC().Format(time.RFC3339),
		})
	}))
	defer upstream.Close()

	client := NewPaymentClient(2 * time.Second)
	ctx := context.Background()

	okReq, _ := http.NewRequest(http.MethodGet, upstream.URL, nil)
	okResp, err := client.Charge(ctx, okReq)
	if err != nil {
		log.Fatalf("successful call failed: %v", err)
	}
	log.Printf("success response: %+v", okResp)

	failReq, _ := http.NewRequest(http.MethodGet, upstream.URL+"?fail=1", nil)
	_, err = client.Charge(ctx, failReq)
	if err != nil {
		log.Printf("expected failure path: %v", err)
	}
}
