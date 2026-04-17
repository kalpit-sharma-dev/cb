package resilience

import (
	"context"
	"time"

	"golang.org/x/time/rate"
)

// RateLimiterConfig configures throughput limits.
type RateLimiterConfig struct {
	LimitForPeriod     int
	LimitRefreshPeriod time.Duration
	TimeoutDuration    time.Duration
}

// RateLimiter wraps x/time/rate.Limiter.
type RateLimiter struct {
	name    string
	config  RateLimiterConfig
	limiter *rate.Limiter
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
			return ErrRateLimitExceeded
		}
		return nil
	}
	ctxWait, cancel := context.WithTimeout(ctx, r.config.TimeoutDuration)
	defer cancel()
	if err := r.limiter.Wait(ctxWait); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return ErrRateLimitExceeded
	}
	return nil
}
