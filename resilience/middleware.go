package resilience

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
)

// MiddlewareConfig controls how resilience is applied to incoming requests.
type MiddlewareConfig struct {
	CircuitBreaker *CircuitBreaker
	Retry          *Retry
	Bulkhead       *Bulkhead
	RateLimiter    *RateLimiter
}

// GorillaMuxMiddleware creates a net/http middleware that wraps handlers with
// configured resilience components. HTTP 5xx responses are treated as failures.
func GorillaMuxMiddleware(cfg MiddlewareConfig) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			err := executeWithComponents(r.Context(), cfg, func(ctx context.Context) error {
				rec := &statusCapturingResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}
				next.ServeHTTP(rec, r.WithContext(ctx))
				if rec.statusCode >= http.StatusInternalServerError {
					return fmt.Errorf("handler returned status %d", rec.statusCode)
				}
				return nil
			})
			if err == nil {
				return
			}
			writeHTTPResilienceError(w, err)
		})
	}
}

// GinMiddleware creates a Gin middleware with resilience semantics. HTTP 5xx
// responses and Gin context errors are treated as failures.
func GinMiddleware(cfg MiddlewareConfig) gin.HandlerFunc {
	return func(c *gin.Context) {
		err := executeWithComponents(c.Request.Context(), cfg, func(ctx context.Context) error {
			c.Request = c.Request.WithContext(ctx)
			c.Next()
			if len(c.Errors) > 0 {
				return c.Errors.Last()
			}
			if c.Writer.Status() >= http.StatusInternalServerError {
				return fmt.Errorf("handler returned status %d", c.Writer.Status())
			}
			return nil
		})
		if err == nil {
			return
		}
		abortGinResilienceError(c, err)
	}
}

func executeWithComponents(ctx context.Context, cfg MiddlewareConfig, exec func(context.Context) error) error {
	_, err := Decorate(func(ctx context.Context) (interface{}, error) {
		if err := exec(ctx); err != nil {
			return nil, err
		}
		return nil, nil
	}).
		WithRateLimiter(cfg.RateLimiter).
		WithBulkhead(cfg.Bulkhead).
		WithCircuitBreaker(cfg.CircuitBreaker).
		WithRetry(cfg.Retry).
		Call(ctx)
	return err
}

func writeHTTPResilienceError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrCircuitOpen):
		http.Error(w, "circuit open", http.StatusServiceUnavailable)
	case errors.Is(err, ErrBulkheadFull):
		http.Error(w, "bulkhead full", http.StatusTooManyRequests)
	case errors.Is(err, ErrRateLimitExceeded):
		http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
	default:
		http.Error(w, "internal error", http.StatusInternalServerError)
	}
}

func abortGinResilienceError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, ErrCircuitOpen):
		c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": "circuit open"})
	case errors.Is(err, ErrBulkheadFull):
		c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{"error": "bulkhead full"})
	case errors.Is(err, ErrRateLimitExceeded):
		c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{"error": "rate limit exceeded"})
	default:
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
	}
}

type statusCapturingResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (w *statusCapturingResponseWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}
