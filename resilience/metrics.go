package resilience

import "sync"

// Snapshot is an immutable view of circuit-breaker metrics.
type Snapshot struct {
	FailureRate             float64
	SlowCallRate            float64
	NumberOfBufferedCalls   int
	NumberOfFailedCalls     int
	NumberOfSlowCalls       int
	NumberOfSuccessfulCalls int
	State                   string
}

type metricsCollector struct {
	mu       sync.RWMutex
	snapshot Snapshot
}

func newMetricsCollector() *metricsCollector {
	return &metricsCollector{}
}

func (m *metricsCollector) Set(snapshot Snapshot) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.snapshot = snapshot
}

func (m *metricsCollector) Get() Snapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.snapshot
}
