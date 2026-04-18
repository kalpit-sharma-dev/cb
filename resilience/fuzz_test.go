package resilience

import (
	"context"
	"errors"
	"testing"
)

func FuzzCircuitBreaker_ErrorPredicateInteractions(f *testing.F) {
	f.Add(uint8(0), uint8(0))
	f.Add(uint8(1), uint8(0))
	f.Add(uint8(0), uint8(1))
	f.Add(uint8(1), uint8(1))

	f.Fuzz(func(t *testing.T, ignoreMask uint8, recordMask uint8) {
		cb := NewCircuitBreaker("fuzz-predicates",
			WithSlidingWindowType(CountBased),
			WithSlidingWindowSize(16),
			WithMinimumNumberOfCalls(1),
			WithFailureRateThreshold(100),
			WithSlowCallRateThreshold(100),
			WithIgnoreErrors(func(err error) bool {
				return errorMatchesMask(err, ignoreMask)
			}),
			WithRecordErrors(func(err error) bool {
				return errorMatchesMask(err, recordMask)
			}),
		)

		sequence := []error{
			errors.New("e0"),
			errors.New("e1"),
			errors.New("e2"),
			errors.New("e3"),
			nil,
			errors.New("e4"),
		}

		for _, errVal := range sequence {
			_, _ = cb.Execute(context.Background(), func(context.Context) (interface{}, error) {
				return nil, errVal
			})
		}

		m := cb.Metrics()
		if m.NumberOfBufferedCalls < 0 {
			t.Fatalf("buffered calls cannot be negative: %+v", m)
		}
		if m.NumberOfBufferedCalls > 16 {
			t.Fatalf("buffered calls exceed window size: %+v", m)
		}
		if m.NumberOfFailedCalls < 0 || m.NumberOfSlowCalls < 0 || m.NumberOfSuccessfulCalls < 0 {
			t.Fatalf("invalid negative metrics: %+v", m)
		}
		if m.NumberOfSuccessfulCalls+m.NumberOfFailedCalls < 0 {
			t.Fatalf("invalid aggregate counts: %+v", m)
		}
		if m.NumberOfSuccessfulCalls+m.NumberOfFailedCalls > m.NumberOfBufferedCalls {
			t.Fatalf("successful+failed should never exceed buffered calls: %+v", m)
		}
		if m.FailureRate < 0 || m.FailureRate > 100 {
			t.Fatalf("failure rate should stay in [0,100], got %f", m.FailureRate)
		}
		if m.SlowCallRate < 0 || m.SlowCallRate > 100 {
			t.Fatalf("slow rate should stay in [0,100], got %f", m.SlowCallRate)
		}
	})
}

func errorMatchesMask(err error, mask uint8) bool {
	if err == nil {
		return false
	}
	if len(err.Error()) == 0 {
		return false
	}
	idx := int(err.Error()[len(err.Error())-1] - '0')
	if idx < 0 || idx > 7 {
		idx = 0
	}
	return (mask & (1 << idx)) != 0
}
