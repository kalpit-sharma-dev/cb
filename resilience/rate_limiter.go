package resilience

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

// RateLimiterConfig configures throughput limits.
type RateLimiterConfig struct {
	LimitForPeriod     int
	LimitRefreshPeriod time.Duration
	TimeoutDuration    time.Duration
}

// RateLimiterMetrics exposes runtime counters.
type RateLimiterMetrics struct {
	AllowedCalls   int64
	DeniedCalls    int64
	WaitingCalls   int64
	LimitForPeriod int
}

type rateLimiterObserver interface {
	OnRateLimit(ctx context.Context, name string, allowed bool)
}

// RateLimiter wraps x/time/rate.Limiter.
type RateLimiter struct {
	name    string
	config  RateLimiterConfig
	limiter *rate.Limiter

	observersMu sync.RWMutex
	observers   []rateLimiterObserver

	allowedCalls atomic.Int64
	deniedCalls  atomic.Int64
	waitingCalls atomic.Int64
}

// Name returns the rate-limiter name.
func (r *RateLimiter) Name() string {
	return r.name
}

func (r *RateLimiter) addObserver(observer rateLimiterObserver) {
	if observer == nil {
		return
	}
	r.observersMu.Lock()
	defer r.observersMu.Unlock()
	r.observers = append(r.observers, observer)
}

func (r *RateLimiter) notify(ctx context.Context, allowed bool) {
	r.observersMu.RLock()
	defer r.observersMu.RUnlock()
	for _, observer := range r.observers {
		observer.OnRateLimit(ctx, r.name, allowed)
	}
}

// Metrics returns a snapshot of rate limiter counters.
func (r *RateLimiter) Metrics() RateLimiterMetrics {
	return RateLimiterMetrics{
		AllowedCalls:   r.allowedCalls.Load(),
		DeniedCalls:    r.deniedCalls.Load(),
		WaitingCalls:   r.waitingCalls.Load(),
		LimitForPeriod: r.config.LimitForPeriod,
	}
}

func WithLimitForPeriod(limit int) Option {
	return func(o *options) { o.rateLimiter.LimitForPeriod = limit }
}

func WithLimitRefreshPeriod(d time.Duration) Option {
	return func(o *options) { o.rateLimiter.LimitRefreshPeriod = d }
}

func WithTimeoutDuration(d time.Duration) Option {
	return func(o *options) { o.rateLimiter.TimeoutDuration = d }
}

// NewRateLimiter creates a new rate limiter.
func NewRateLimiter(name string, opts ...Option) *RateLimiter {
	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}
	cfg := o.rateLimiter
	if cfg.LimitForPeriod <= 0 {
		cfg.LimitForPeriod = 50
	}
	if cfg.LimitRefreshPeriod <= 0 {
		cfg.LimitRefreshPeriod = time.Second
	}

	limitPerSecond := float64(cfg.LimitForPeriod) / cfg.LimitRefreshPeriod.Seconds()
	limiter := rate.NewLimiter(rate.Limit(limitPerSecond), cfg.LimitForPeriod)
	return &RateLimiter{name: name, config: cfg, limiter: limiter}
}

// Execute waits for permission and then executes fn.
func (r *RateLimiter) Execute(ctx context.Context, fn func(context.Context) (interface{}, error)) (interface{}, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.Wait(ctx); err != nil {
		return nil, err
	}
	return fn(ctx)
}

// Wait acquires one permission.
func (r *RateLimiter) Wait(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if r.config.TimeoutDuration <= 0 {
		if !r.limiter.Allow() {
			r.deniedCalls.Add(1)
			r.notify(ctx, false)
			return ErrRateLimitExceeded
		}
		r.allowedCalls.Add(1)
		r.notify(ctx, true)
		return nil
	}

	r.waitingCalls.Add(1)
	defer r.waitingCalls.Add(-1)
	ctxWait, cancel := context.WithTimeout(ctx, r.config.TimeoutDuration)
	defer cancel()
	if err := r.limiter.Wait(ctxWait); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		r.deniedCalls.Add(1)
		r.notify(ctx, false)
		return ErrRateLimitExceeded
	}
	r.allowedCalls.Add(1)
	r.notify(ctx, true)
	return nil
}
