package resilience

import (
	"context"
	"math"
	"time"
)

// RetryConfig configures retry behavior.
type RetryConfig struct {
	MaxAttempts   int
	WaitDuration  time.Duration
	BackoffFactor float64
	MaxInterval   time.Duration
	RetryOn       func(error) bool
	OnRetry       func(attempt int, err error)
}

// Retry retries failed operations based on policy.
type Retry struct {
	name   string
	config RetryConfig
	clock  Clock
}

func WithMaxAttempts(max int) Option {
	return func(o *options) { o.retryConfig.MaxAttempts = max }
}

func WithWaitDuration(d time.Duration) Option {
	return func(o *options) { o.retryConfig.WaitDuration = d }
}

func WithExponentialBackoff(multiplier float64, maxInterval time.Duration) Option {
	return func(o *options) {
		o.retryConfig.BackoffFactor = multiplier
		o.retryConfig.MaxInterval = maxInterval
	}
}

func WithRetryOn(pred func(error) bool) Option {
	return func(o *options) { o.retryConfig.RetryOn = pred }
}

func WithOnRetry(hook func(attempt int, err error)) Option {
	return func(o *options) { o.retryConfig.OnRetry = hook }
}

// NewRetry builds a retry policy.
func NewRetry(name string, opts ...Option) *Retry {
	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}
	cfg := o.retryConfig
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 3
	}
	if cfg.BackoffFactor < 0 {
		cfg.BackoffFactor = 0
	}
	return &Retry{name: name, config: cfg, clock: o.clock}
}

// Execute applies retries to fn.
func (r *Retry) Execute(ctx context.Context, fn func(context.Context) (interface{}, error)) (interface{}, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var lastErr error
	for attempt := 1; attempt <= r.config.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		result, err := fn(ctx)
		if err == nil {
			return result, nil
		}
		lastErr = err
		if !r.shouldRetry(err) || attempt == r.config.MaxAttempts {
			return result, err
		}
		if r.config.OnRetry != nil {
			r.config.OnRetry(attempt, err)
		}
		if wait := r.backoffFor(attempt); wait > 0 {
			if sleepErr := r.clock.Sleep(ctx, wait); sleepErr != nil {
				return nil, sleepErr
			}
		}
	}
	return nil, lastErr
}

func (r *Retry) shouldRetry(err error) bool {
	if r.config.RetryOn == nil {
		return err != nil
	}
	return r.config.RetryOn(err)
}

func (r *Retry) backoffFor(attempt int) time.Duration {
	base := r.config.WaitDuration
	if base <= 0 {
		return 0
	}
	if r.config.BackoffFactor <= 0 {
		return base
	}
	mult := math.Pow(r.config.BackoffFactor, float64(attempt-1))
	wait := time.Duration(float64(base) * mult)
	if r.config.MaxInterval > 0 && wait > r.config.MaxInterval {
		return r.config.MaxInterval
	}
	return wait
}
