package resilience

import (
	"context"
	"errors"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// OTelBridgeConfig configures OpenTelemetry export bridge behavior.
type OTelBridgeConfig struct {
	Meter         metric.Meter
	MetricsPrefix string
}

type snapshotSources struct {
	cbList       []*CircuitBreaker
	retryList    []*Retry
	bulkheadList []*Bulkhead
	rateList     []*RateLimiter
}

// OTelBridge exports events and snapshot metrics to OpenTelemetry.
type OTelBridge struct {
	cfg OTelBridgeConfig

	eventCBCalls     metric.Int64Counter
	eventCBState     metric.Int64Counter
	eventRetries     metric.Int64Counter
	eventBulkhead    metric.Int64Counter
	eventRateLimiter metric.Int64Counter

	cbFailureRate     metric.Float64ObservableGauge
	cbSlowCallRate    metric.Float64ObservableGauge
	retryAttempts     metric.Int64ObservableGauge
	bulkheadAvailable metric.Int64ObservableGauge
	rateAllowed       metric.Int64ObservableGauge

	registration metric.Registration

	mu      sync.RWMutex
	sources snapshotSources
}

// NewOTelBridge creates an OpenTelemetry bridge instance and registers metric instruments.
func NewOTelBridge(cfg OTelBridgeConfig) (*OTelBridge, error) {
	if cfg.Meter == nil {
		return nil, errors.New("resilience: otel meter is required")
	}
	if cfg.MetricsPrefix == "" {
		cfg.MetricsPrefix = "resilience"
	}

	bridge := &OTelBridge{cfg: cfg}
	if err := bridge.initInstruments(); err != nil {
		return nil, err
	}
	if err := bridge.registerSnapshotCallback(); err != nil {
		return nil, err
	}
	return bridge, nil
}

func (b *OTelBridge) initInstruments() error {
	var err error
	meter := b.cfg.Meter
	prefix := b.cfg.MetricsPrefix

	if b.eventCBCalls, err = meter.Int64Counter(prefix + ".cb.calls_total"); err != nil {
		return err
	}
	if b.eventCBState, err = meter.Int64Counter(prefix + ".cb.state_changes_total"); err != nil {
		return err
	}
	if b.eventRetries, err = meter.Int64Counter(prefix + ".retry.events_total"); err != nil {
		return err
	}
	if b.eventBulkhead, err = meter.Int64Counter(prefix + ".bulkhead.events_total"); err != nil {
		return err
	}
	if b.eventRateLimiter, err = meter.Int64Counter(prefix + ".rate_limiter.events_total"); err != nil {
		return err
	}
	if b.cbFailureRate, err = meter.Float64ObservableGauge(prefix + ".cb.failure_rate"); err != nil {
		return err
	}
	if b.cbSlowCallRate, err = meter.Float64ObservableGauge(prefix + ".cb.slow_call_rate"); err != nil {
		return err
	}
	if b.retryAttempts, err = meter.Int64ObservableGauge(prefix + ".retry.attempts_total"); err != nil {
		return err
	}
	if b.bulkheadAvailable, err = meter.Int64ObservableGauge(prefix + ".bulkhead.available_concurrency"); err != nil {
		return err
	}
	if b.rateAllowed, err = meter.Int64ObservableGauge(prefix + ".rate_limiter.allowed_total"); err != nil {
		return err
	}
	return nil
}

func (b *OTelBridge) registerSnapshotCallback() error {
	registration, err := b.cfg.Meter.RegisterCallback(
		b.observeSnapshots,
		b.cbFailureRate,
		b.cbSlowCallRate,
		b.retryAttempts,
		b.bulkheadAvailable,
		b.rateAllowed,
	)
	if err != nil {
		return err
	}
	b.registration = registration
	return nil
}

// Shutdown unregisters bridge callbacks.
func (b *OTelBridge) Shutdown(context.Context) error {
	if b.registration != nil {
		return b.registration.Unregister()
	}
	return nil
}

// RegisterCircuitBreaker attaches event listeners and snapshot export.
func (b *OTelBridge) RegisterCircuitBreaker(cb *CircuitBreaker) {
	if cb == nil {
		return
	}
	cb.AddListener(&otelCBListener{bridge: b, name: cb.Name()})

	b.mu.Lock()
	b.sources.cbList = append(b.sources.cbList, cb)
	b.mu.Unlock()
}

// RegisterRetry registers retry observer and snapshot source.
func (b *OTelBridge) RegisterRetry(retry *Retry) {
	if retry == nil {
		return
	}
	retry.addObserver(b)

	b.mu.Lock()
	b.sources.retryList = append(b.sources.retryList, retry)
	b.mu.Unlock()
}

// RegisterBulkhead registers bulkhead observer and snapshot source.
func (b *OTelBridge) RegisterBulkhead(bulkhead *Bulkhead) {
	if bulkhead == nil {
		return
	}
	bulkhead.addObserver(b)

	b.mu.Lock()
	b.sources.bulkheadList = append(b.sources.bulkheadList, bulkhead)
	b.mu.Unlock()
}

// RegisterRateLimiter registers rate limiter observer and snapshot source.
func (b *OTelBridge) RegisterRateLimiter(rateLimiter *RateLimiter) {
	if rateLimiter == nil {
		return
	}
	rateLimiter.addObserver(b)

	b.mu.Lock()
	b.sources.rateList = append(b.sources.rateList, rateLimiter)
	b.mu.Unlock()
}

func (b *OTelBridge) observeSnapshots(_ context.Context, obs metric.Observer) error {
	sources := b.snapshotCopy()
	for _, cb := range sources.cbList {
		m := cb.Metrics()
		attrs := metric.WithAttributes(
			attribute.String("name", cb.Name()),
			attribute.String("state", m.State),
		)
		obs.ObserveFloat64(b.cbFailureRate, m.FailureRate, attrs)
		obs.ObserveFloat64(b.cbSlowCallRate, m.SlowCallRate, attrs)
	}
	for _, r := range sources.retryList {
		m := r.Metrics()
		obs.ObserveInt64(b.retryAttempts, m.TotalAttempts,
			metric.WithAttributes(attribute.String("name", r.Name())))
	}
	for _, bh := range sources.bulkheadList {
		m := bh.Metrics()
		obs.ObserveInt64(b.bulkheadAvailable, int64(m.AvailableConcurrentCalls),
			metric.WithAttributes(attribute.String("name", bh.Name())))
	}
	for _, rl := range sources.rateList {
		m := rl.Metrics()
		obs.ObserveInt64(b.rateAllowed, m.AllowedCalls,
			metric.WithAttributes(attribute.String("name", rl.Name())))
	}
	return nil
}

func (b *OTelBridge) snapshotCopy() snapshotSources {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return snapshotSources{
		cbList:       append([]*CircuitBreaker(nil), b.sources.cbList...),
		retryList:    append([]*Retry(nil), b.sources.retryList...),
		bulkheadList: append([]*Bulkhead(nil), b.sources.bulkheadList...),
		rateList:     append([]*RateLimiter(nil), b.sources.rateList...),
	}
}

func (b *OTelBridge) OnRetryAttempt(name string, attempt int, err error) {
	attrs := []attribute.KeyValue{
		attribute.String("name", name),
		attribute.String("event", "retry_attempt"),
		attribute.Int("attempt", attempt),
	}
	if err != nil {
		attrs = append(attrs, attribute.String("error", err.Error()))
	}
	b.eventRetries.Add(context.Background(), 1, metric.WithAttributes(attrs...))
}

func (b *OTelBridge) OnRetryResult(name string, attempts int, err error) {
	outcome := "success"
	if err != nil {
		outcome = "failure"
	}
	b.eventRetries.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("name", name),
		attribute.String("event", "retry_result"),
		attribute.String("outcome", outcome),
		attribute.Int("attempts", attempts),
	))
}

func (b *OTelBridge) OnBulkheadCall(name string, admitted bool) {
	outcome := "admitted"
	if !admitted {
		outcome = "rejected"
	}
	b.eventBulkhead.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("name", name),
		attribute.String("outcome", outcome),
	))
}

func (b *OTelBridge) OnRateLimit(name string, allowed bool) {
	outcome := "allowed"
	if !allowed {
		outcome = "denied"
	}
	b.eventRateLimiter.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("name", name),
		attribute.String("outcome", outcome),
	))
}

type otelCBListener struct {
	bridge *OTelBridge
	name   string
}

func (l *otelCBListener) OnSuccess(_ CallEvent) {
	l.bridge.eventCBCalls.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("name", l.name),
		attribute.String("event", "success"),
	))
}

func (l *otelCBListener) OnFailure(event CallEvent) {
	attrs := []attribute.KeyValue{
		attribute.String("name", l.name),
		attribute.String("event", "failure"),
	}
	if event.Err != nil {
		attrs = append(attrs, attribute.String("error", event.Err.Error()))
	}
	l.bridge.eventCBCalls.Add(context.Background(), 1, metric.WithAttributes(attrs...))
}

func (l *otelCBListener) OnSlowCall(_ CallEvent) {
	l.bridge.eventCBCalls.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("name", l.name),
		attribute.String("event", "slow"),
	))
}

func (l *otelCBListener) OnIgnored(_ CallEvent) {
	l.bridge.eventCBCalls.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("name", l.name),
		attribute.String("event", "ignored"),
	))
}

func (l *otelCBListener) OnStateChange(event StateChangeEvent) {
	l.bridge.eventCBState.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("name", l.name),
		attribute.String("from", string(event.From)),
		attribute.String("to", string(event.To)),
	))
}
