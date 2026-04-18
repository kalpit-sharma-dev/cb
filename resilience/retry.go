package resilience

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
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

// RetryMetrics is a snapshot of retry runtime counters.
type RetryMetrics struct {
	TotalAttempts  int64
	TotalRetries   int64
	TotalSuccesses int64
	TotalFailures  int64
}

type retryObserver interface {
	OnRetryAttempt(ctx context.Context, name string, attempt int, err error)
	OnRetryResult(ctx context.Context, name string, attempts int, err error)
}

// Retry retries failed operations based on policy.
type Retry struct {
	name   string
	config RetryConfig
	clock  Clock

	observersMu sync.RWMutex
	observers   []retryObserver

	totalAttempts  atomic.Int64
	totalRetries   atomic.Int64
	totalSuccesses atomic.Int64
	totalFailures  atomic.Int64
}

// Name returns the retry name.
func (r *Retry) Name() string {
	return r.name
}

// Metrics returns retry runtime counters.
func (r *Retry) Metrics() RetryMetrics {
	return RetryMetrics{
		TotalAttempts:  r.totalAttempts.Load(),
		TotalRetries:   r.totalRetries.Load(),
		TotalSuccesses: r.totalSuccesses.Load(),
		TotalFailures:  r.totalFailures.Load(),
	}
}

func (r *Retry) addObserver(observer retryObserver) {
	if observer == nil {
		return
	}
	r.observersMu.Lock()
	defer r.observersMu.Unlock()
	r.observers = append(r.observers, observer)
}

func (r *Retry) notifyRetryAttempt(ctx context.Context, attempt int, err error) {
	r.observersMu.RLock()
	defer r.observersMu.RUnlock()
	for _, observer := range r.observers {
		observer.OnRetryAttempt(ctx, r.name, attempt, err)
	}
}

func (r *Retry) notifyRetryResult(ctx context.Context, attempts int, err error) {
	r.observersMu.RLock()
	defer r.observersMu.RUnlock()
	for _, observer := range r.observers {
		observer.OnRetryResult(ctx, r.name, attempts, err)
	}
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
	var attempts int
	for attempt := 1; attempt <= r.config.MaxAttempts; attempt++ {
		attempts = attempt
		if err := ctx.Err(); err != nil {
			r.totalFailures.Add(1)
			r.notifyRetryResult(ctx, attempts-1, err)
			return nil, err
		}
		r.totalAttempts.Add(1)
		result, err := fn(ctx)
		if err == nil {
			r.totalSuccesses.Add(1)
			r.notifyRetryResult(ctx, attempts, nil)
			return result, nil
		}
		lastErr = err
		if !r.shouldRetry(err) || attempt == r.config.MaxAttempts {
			r.totalFailures.Add(1)
			r.notifyRetryResult(ctx, attempts, err)
			return result, err
		}

		r.totalRetries.Add(1)
		r.notifyRetryAttempt(ctx, attempt, err)
		if r.config.OnRetry != nil {
			r.config.OnRetry(attempt, err)
		}
		if wait := r.backoffFor(attempt); wait > 0 {
			if sleepErr := r.clock.Sleep(ctx, wait); sleepErr != nil {
				r.totalFailures.Add(1)
				r.notifyRetryResult(ctx, attempts, sleepErr)
				return nil, sleepErr
			}
		}
	}
	if attempts > 0 {
		r.totalFailures.Add(1)
		r.notifyRetryResult(ctx, attempts, lastErr)
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
