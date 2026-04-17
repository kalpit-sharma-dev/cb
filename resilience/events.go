package resilience

import (
	"sync"
	"time"
)

// EventListener receives notifications about call outcomes and state transitions.
type EventListener interface {
	OnSuccess(event CallEvent)
	OnFailure(event CallEvent)
	OnSlowCall(event CallEvent)
	OnIgnored(event CallEvent)
	OnStateChange(event StateChangeEvent)
}

// CallEvent describes the outcome of an invocation.
type CallEvent struct {
	Name     string
	Duration time.Duration
	Err      error
}

// StateChangeEvent captures a circuit-breaker state transition.
type StateChangeEvent struct {
	Name string
	From State
	To   State
}

type eventPublisher struct {
	mu        sync.RWMutex
	listeners []EventListener
}

func newEventPublisher() *eventPublisher {
	return &eventPublisher{}
}

func (p *eventPublisher) AddListener(listener EventListener) {
	if listener == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.listeners = append(p.listeners, listener)
}

func (p *eventPublisher) snapshotListeners() []EventListener {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.listeners) == 0 {
		return nil
	}
	cp := make([]EventListener, len(p.listeners))
	copy(cp, p.listeners)
	return cp
}

func (p *eventPublisher) PublishSuccess(event CallEvent) {
	for _, listener := range p.snapshotListeners() {
		listener.OnSuccess(event)
	}
}

func (p *eventPublisher) PublishFailure(event CallEvent) {
	for _, listener := range p.snapshotListeners() {
		listener.OnFailure(event)
	}
}

func (p *eventPublisher) PublishSlowCall(event CallEvent) {
	for _, listener := range p.snapshotListeners() {
		listener.OnSlowCall(event)
	}
}

func (p *eventPublisher) PublishIgnored(event CallEvent) {
	for _, listener := range p.snapshotListeners() {
		listener.OnIgnored(event)
	}
}

func (p *eventPublisher) PublishStateChange(event StateChangeEvent) {
	for _, listener := range p.snapshotListeners() {
		listener.OnStateChange(event)
	}
}
