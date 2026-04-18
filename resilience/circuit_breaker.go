package resilience

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/sony/gobreaker"
)

var (
	ErrCircuitOpen       = errors.New("resilience: circuit breaker open")
	ErrBulkheadFull      = errors.New("resilience: bulkhead full")
	ErrRateLimitExceeded = errors.New("resilience: rate limit exceeded")
)

// State mirrors circuit-breaker states.
type State string

const (
	StateClosed   State = "CLOSED"
	StateOpen     State = "OPEN"
	StateHalfOpen State = "HALF_OPEN"
)

// Executor executes a function with resilience controls.
type Executor interface {
	Execute(ctx context.Context, fn func(context.Context) (interface{}, error)) (interface{}, error)
}

// Config configures the circuit breaker.
type Config struct {
	SlidingWindowType                     SlidingWindowType // CountBased | TimeBased
	SlidingWindowSize                     int               // N calls or N seconds
	MinimumNumberOfCalls                  int               // default 100
	FailureRateThreshold                  float64           // 0–100, default 50
	SlowCallRateThreshold                 float64           // 0–100, default 100
	SlowCallDurationThreshold             time.Duration     // default 60s
	PermittedNumberOfCallsInHalfOpen      int               // default 10
	WaitDurationInOpenState               time.Duration     // default 60s
	AutomaticTransitionFromOpenToHalfOpen bool              // default true
	RecordErrorPredicate                  func(error) bool  // nil = record all
	IgnoreErrorPredicate                  func(error) bool  // nil = ignore none
}

// Option configures resilience components.
type Option func(*options)

type options struct {
	cbConfig    Config
	retryConfig RetryConfig
	bulkhead    BulkheadConfig
	rateLimiter RateLimiterConfig
	clock       Clock
}

func defaultOptions() *options {
	return &options{
		cbConfig: Config{
			SlidingWindowType:                     CountBased,
			SlidingWindowSize:                     100,
			MinimumNumberOfCalls:                  100,
			FailureRateThreshold:                  50,
			SlowCallRateThreshold:                 100,
			SlowCallDurationThreshold:             60 * time.Second,
			PermittedNumberOfCallsInHalfOpen:      10,
			WaitDurationInOpenState:               60 * time.Second,
			AutomaticTransitionFromOpenToHalfOpen: true,
		},
		retryConfig: RetryConfig{
			MaxAttempts:   3,
			WaitDuration:  0,
			BackoffFactor: 0,
			MaxInterval:   0,
		},
		bulkhead: BulkheadConfig{
			MaxConcurrentCalls: 25,
			MaxWaitDuration:    0,
		},
		rateLimiter: RateLimiterConfig{
			LimitForPeriod:     50,
			LimitRefreshPeriod: time.Second,
			TimeoutDuration:    0,
		},
		clock: realClock{},
	}
}

func WithSlidingWindowType(t SlidingWindowType) Option {
	return func(o *options) { o.cbConfig.SlidingWindowType = t }
}

func WithSlidingWindowSize(size int) Option {
	return func(o *options) { o.cbConfig.SlidingWindowSize = size }
}

func WithMinimumNumberOfCalls(min int) Option {
	return func(o *options) { o.cbConfig.MinimumNumberOfCalls = min }
}

func WithFailureRateThreshold(threshold float64) Option {
	return func(o *options) { o.cbConfig.FailureRateThreshold = threshold }
}

func WithSlowCallRateThreshold(threshold float64) Option {
	return func(o *options) { o.cbConfig.SlowCallRateThreshold = threshold }
}

func WithSlowCallDurationThreshold(threshold time.Duration) Option {
	return func(o *options) { o.cbConfig.SlowCallDurationThreshold = threshold }
}

func WithPermittedNumberOfCallsInHalfOpenState(calls int) Option {
	return func(o *options) { o.cbConfig.PermittedNumberOfCallsInHalfOpen = calls }
}

func WithWaitDurationInOpenState(d time.Duration) Option {
	return func(o *options) { o.cbConfig.WaitDurationInOpenState = d }
}

func WithAutomaticTransitionFromOpenToHalfOpen(enabled bool) Option {
	return func(o *options) { o.cbConfig.AutomaticTransitionFromOpenToHalfOpen = enabled }
}

func WithRecordErrors(pred func(error) bool) Option {
	return func(o *options) { o.cbConfig.RecordErrorPredicate = pred }
}

func WithIgnoreErrors(pred func(error) bool) Option {
	return func(o *options) { o.cbConfig.IgnoreErrorPredicate = pred }
}

func WithClock(clock Clock) Option {
	return func(o *options) {
		if clock != nil {
			o.clock = clock
		}
	}
}

// CircuitBreaker wraps gobreaker with Resilience4j-like semantics.
type CircuitBreaker struct {
	name      string
	config    Config
	clock     Clock
	window    slidingWindow
	metrics   *metricsCollector
	publisher *eventPublisher
	gb        *gobreaker.TwoStepCircuitBreaker
	stateMu   sync.RWMutex
	state     State
}

// NewCircuitBreaker builds a new circuit breaker instance.
func NewCircuitBreaker(name string, opts ...Option) *CircuitBreaker {
	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}
	cfg := normalizeCircuitBreakerConfig(o.cbConfig)

	cb := &CircuitBreaker{
		name:      name,
		config:    cfg,
		clock:     o.clock,
		window:    newSlidingWindow(cfg),
		metrics:   newMetricsCollector(),
		publisher: newEventPublisher(),
		state:     StateClosed,
	}

	timeout := cfg.WaitDurationInOpenState
	if !cfg.AutomaticTransitionFromOpenToHalfOpen {
		timeout = 365 * 24 * time.Hour
	}

	settings := gobreaker.Settings{
		Name:        name,
		MaxRequests: uint32(cfg.PermittedNumberOfCallsInHalfOpen),
		Timeout:     timeout,
		ReadyToTrip: func(_ gobreaker.Counts) bool {
			agg := cb.window.Aggregate(cb.clock.Now())
			if agg.total < cfg.MinimumNumberOfCalls {
				return false
			}
			return agg.failureRate >= cfg.FailureRateThreshold || agg.slowRate >= cfg.SlowCallRateThreshold
		},
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			fromState := mapState(from)
			toState := mapState(to)
			cb.setState(toState)
			if toState == StateHalfOpen || toState == StateClosed {
				cb.window.Reset()
				cb.updateSnapshot(aggregate{})
			}
			cb.publisher.PublishStateChange(StateChangeEvent{Name: name, From: fromState, To: toState})
		},
		// Required by task: classify errors externally.
		IsSuccessful: func(error) bool { return true },
	}

	cb.gb = gobreaker.NewTwoStepCircuitBreaker(settings)
	cb.updateSnapshot(cb.window.Aggregate(cb.clock.Now()))
	return cb
}

func normalizeCircuitBreakerConfig(cfg Config) Config {
	if cfg.SlidingWindowType != CountBased && cfg.SlidingWindowType != TimeBased {
		cfg.SlidingWindowType = CountBased
	}
	if cfg.SlidingWindowSize <= 0 {
		cfg.SlidingWindowSize = 100
	}
	if cfg.MinimumNumberOfCalls <= 0 {
		cfg.MinimumNumberOfCalls = 100
	}
	if cfg.FailureRateThreshold <= 0 || cfg.FailureRateThreshold > 100 {
		cfg.FailureRateThreshold = 50
	}
	if cfg.SlowCallRateThreshold <= 0 || cfg.SlowCallRateThreshold > 100 {
		cfg.SlowCallRateThreshold = 100
	}
	if cfg.SlowCallDurationThreshold <= 0 {
		cfg.SlowCallDurationThreshold = 60 * time.Second
	}
	if cfg.PermittedNumberOfCallsInHalfOpen <= 0 {
		cfg.PermittedNumberOfCallsInHalfOpen = 10
	}
	if cfg.WaitDurationInOpenState <= 0 {
		cfg.WaitDurationInOpenState = 60 * time.Second
	}
	return cfg
}

func newSlidingWindow(cfg Config) slidingWindow {
	if cfg.SlidingWindowType == TimeBased {
		return newTimeBasedWindow(cfg.SlidingWindowSize)
	}
	return newCountBasedWindow(cfg.SlidingWindowSize)
}

func mapState(state gobreaker.State) State {
	switch state {
	case gobreaker.StateClosed:
		return StateClosed
	case gobreaker.StateHalfOpen:
		return StateHalfOpen
	default:
		return StateOpen
	}
}

func (cb *CircuitBreaker) classify(err error) (counted bool, failed bool, ignored bool) {
	if err == nil {
		return true, false, false
	}
	if cb.config.IgnoreErrorPredicate != nil && cb.config.IgnoreErrorPredicate(err) {
		return false, false, true
	}
	if cb.config.RecordErrorPredicate != nil {
		if cb.config.RecordErrorPredicate(err) {
			return true, true, false
		}
		return true, false, false
	}
	return true, true, false
}

// AddListener adds an event listener.
func (cb *CircuitBreaker) AddListener(listener EventListener) {
	cb.publisher.AddListener(listener)
}

// Name returns the circuit-breaker name.
func (cb *CircuitBreaker) Name() string {
	return cb.name
}

// State returns current state.
func (cb *CircuitBreaker) State() State {
	cb.stateMu.RLock()
	defer cb.stateMu.RUnlock()
	return cb.state
}

// Metrics returns a consistent snapshot.
func (cb *CircuitBreaker) Metrics() Snapshot {
	agg := cb.window.Aggregate(cb.clock.Now())
	cb.updateSnapshot(agg)
	return cb.metrics.Get()
}

func (cb *CircuitBreaker) updateSnapshot(agg aggregate) {
	cb.metrics.Set(Snapshot{
		FailureRate:             agg.failureRate,
		SlowCallRate:            agg.slowRate,
		NumberOfBufferedCalls:   agg.total,
		NumberOfFailedCalls:     agg.failed,
		NumberOfSlowCalls:       agg.slow,
		NumberOfSuccessfulCalls: agg.success,
		State:                   string(cb.currentState()),
	})
}

func (cb *CircuitBreaker) setState(s State) {
	cb.stateMu.Lock()
	defer cb.stateMu.Unlock()
	cb.state = s
}

func (cb *CircuitBreaker) currentState() State {
	cb.stateMu.RLock()
	defer cb.stateMu.RUnlock()
	return cb.state
}

// Execute runs a function with circuit-breaker semantics.
func (cb *CircuitBreaker) Execute(ctx context.Context, fn func(context.Context) (interface{}, error)) (interface{}, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	started := cb.clock.Now()
	done, err := cb.gb.Allow()
	if err != nil {
		if errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests) {
			return nil, ErrCircuitOpen
		}
		return nil, err
	}

	if ctxErr := ctx.Err(); ctxErr != nil {
		record, _ := cb.recordCall(started, cb.clock.Now(), ctxErr)
		done(!(record.counted && (record.failed || record.slow)))
		return nil, ctxErr
	}

	result, callErr := fn(ctx)
	record, agg := cb.recordCall(started, cb.clock.Now(), callErr)

	failForState := record.counted && (record.failed || record.slow)
	if !failForState && record.counted && cb.State() == StateClosed {
		if agg.total >= cb.config.MinimumNumberOfCalls &&
			(agg.failureRate >= cb.config.FailureRateThreshold || agg.slowRate >= cb.config.SlowCallRateThreshold) {
			failForState = true
		}
	}
	done(!failForState)

	if callErr != nil {
		return result, callErr
	}
	return result, nil
}

func (cb *CircuitBreaker) recordCall(start time.Time, end time.Time, callErr error) (callRecord, aggregate) {
	duration := end.Sub(start)
	counted, failed, ignored := cb.classify(callErr)
	slow := counted && duration >= cb.config.SlowCallDurationThreshold

	record := callRecord{
		counted:       counted,
		failed:        failed,
		failedForRate: failed || slow,
		slow:          slow,
		success:       counted && !failed,
	}

	agg := cb.window.Aggregate(end)
	if counted {
		agg = cb.window.Record(end, record)
	}
	cb.updateSnapshot(agg)

	event := CallEvent{Name: cb.name, Duration: duration, Err: callErr}
	switch {
	case ignored:
		cb.publisher.PublishIgnored(event)
	case failed:
		cb.publisher.PublishFailure(event)
	case slow:
		cb.publisher.PublishSlowCall(event)
	default:
		cb.publisher.PublishSuccess(event)
	}

	return record, agg
}

func (cb *CircuitBreaker) String() string {
	return fmt.Sprintf("CircuitBreaker{name:%q,state:%s}", cb.name, cb.State())
}
