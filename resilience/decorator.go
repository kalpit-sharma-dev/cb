package resilience

import "context"

// Func is the function signature decorated by resilience components.
type Func func(context.Context) (interface{}, error)

// Decorator composes resilience components around a function.
type Decorator struct {
	fn    Func
	chain []Executor
}

// Decorate starts a fluent decorator chain.
func Decorate(fn Func) *Decorator {
	return &Decorator{fn: fn}
}

// WithCircuitBreaker adds a circuit breaker to the chain.
func (d *Decorator) WithCircuitBreaker(cb *CircuitBreaker) *Decorator {
	if cb != nil {
		d.chain = append(d.chain, cb)
	}
	return d
}

// WithRetry adds retry to the chain.
func (d *Decorator) WithRetry(retry *Retry) *Decorator {
	if retry != nil {
		d.chain = append(d.chain, retry)
	}
	return d
}

// WithBulkhead adds bulkhead to the chain.
func (d *Decorator) WithBulkhead(bh *Bulkhead) *Decorator {
	if bh != nil {
		d.chain = append(d.chain, bh)
	}
	return d
}

// WithRateLimiter adds rate limiter to the chain.
func (d *Decorator) WithRateLimiter(rl *RateLimiter) *Decorator {
	if rl != nil {
		d.chain = append(d.chain, rl)
	}
	return d
}

// Call executes the decorated function.
func (d *Decorator) Call(ctx context.Context) (interface{}, error) {
	if d.fn == nil {
		return nil, nil
	}
	wrapped := d.fn
	for i := len(d.chain) - 1; i >= 0; i-- {
		exec := d.chain[i]
		next := wrapped
		wrapped = func(ctx context.Context) (interface{}, error) {
			return exec.Execute(ctx, next)
		}
	}
	return wrapped(ctx)
}
