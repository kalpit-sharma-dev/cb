package resilience

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// BulkheadConfig configures concurrency limits.
type BulkheadConfig struct {
	MaxConcurrentCalls int
	MaxWaitDuration    time.Duration
}

// BulkheadMetrics exposes current capacity.
type BulkheadMetrics struct {
	AvailableConcurrentCalls int
	MaxConcurrentCalls       int
	TotalCalls               int64
	TotalRejectedCalls       int64
}

type bulkheadObserver interface {
	OnBulkheadCall(name string, admitted bool)
}

// Bulkhead is a semaphore-based concurrency limiter.
type Bulkhead struct {
	name      string
	config    BulkheadConfig
	semaphore chan struct{}
	clock     Clock

	observersMu sync.RWMutex
	observers   []bulkheadObserver

	totalCalls         atomic.Int64
	totalRejectedCalls atomic.Int64
}

// Name returns the bulkhead name.
func (b *Bulkhead) Name() string {
	return b.name
}

func (b *Bulkhead) addObserver(observer bulkheadObserver) {
	if observer == nil {
		return
	}
	b.observersMu.Lock()
	defer b.observersMu.Unlock()
	b.observers = append(b.observers, observer)
}

func (b *Bulkhead) notify(admitted bool) {
	b.observersMu.RLock()
	defer b.observersMu.RUnlock()
	for _, observer := range b.observers {
		observer.OnBulkheadCall(b.name, admitted)
	}
}

func WithMaxConcurrentCalls(max int) Option {
	return func(o *options) { o.bulkhead.MaxConcurrentCalls = max }
}

func WithMaxWaitDuration(d time.Duration) Option {
	return func(o *options) { o.bulkhead.MaxWaitDuration = d }
}

// NewBulkhead creates a new bulkhead.
func NewBulkhead(name string, opts ...Option) *Bulkhead {
	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}
	cfg := o.bulkhead
	if cfg.MaxConcurrentCalls <= 0 {
		cfg.MaxConcurrentCalls = 25
	}
	return &Bulkhead{
		name:      name,
		config:    cfg,
		semaphore: make(chan struct{}, cfg.MaxConcurrentCalls),
		clock:     o.clock,
	}
}

// Execute executes fn when capacity is available.
func (b *Bulkhead) Execute(ctx context.Context, fn func(context.Context) (interface{}, error)) (interface{}, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	b.totalCalls.Add(1)
	release, err := b.acquire(ctx)
	if err != nil {
		b.totalRejectedCalls.Add(1)
		b.notify(false)
		return nil, err
	}
	b.notify(true)
	defer release()
	return fn(ctx)
}

func (b *Bulkhead) acquire(ctx context.Context) (func(), error) {
	if b.config.MaxWaitDuration <= 0 {
		select {
		case b.semaphore <- struct{}{}:
			return func() { <-b.semaphore }, nil
		default:
			return nil, ErrBulkheadFull
		}
	}

	ctxWait, cancel := context.WithTimeout(ctx, b.config.MaxWaitDuration)
	defer cancel()
	select {
	case b.semaphore <- struct{}{}:
		return func() { <-b.semaphore }, nil
	case <-ctxWait.Done():
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, ErrBulkheadFull
	}
}

// Metrics returns current bulkhead stats.
func (b *Bulkhead) Metrics() BulkheadMetrics {
	used := len(b.semaphore)
	return BulkheadMetrics{
		AvailableConcurrentCalls: b.config.MaxConcurrentCalls - used,
		MaxConcurrentCalls:       b.config.MaxConcurrentCalls,
		TotalCalls:               b.totalCalls.Load(),
		TotalRejectedCalls:       b.totalRejectedCalls.Load(),
	}
}
