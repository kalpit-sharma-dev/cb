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
			rec := &statusCapturingResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}
			err := executeWithComponents(r.Context(), cfg, rec, true, func(ctx context.Context) error {
				next.ServeHTTP(rec, r.WithContext(ctx))
				if rec.statusCode >= http.StatusInternalServerError {
					return fmt.Errorf("handler returned status %d", rec.statusCode)
				}
				return nil
			})
			if err == nil {
				return
			}
			writeHTTPResilienceError(rec, err)
		})
	}
}

// GinMiddleware creates a Gin middleware with resilience semantics. HTTP 5xx
// responses and Gin context errors are treated as failures.
func GinMiddleware(cfg MiddlewareConfig) gin.HandlerFunc {
	return func(c *gin.Context) {
		err := executeWithComponents(c.Request.Context(), cfg, nil, true, func(ctx context.Context) error {
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

func executeWithComponents(
	ctx context.Context,
	cfg MiddlewareConfig,
	httpWriter *statusCapturingResponseWriter,
	disableRetry bool,
	exec func(context.Context) error,
) error {
	// A retry around an inbound handler can execute business logic multiple times,
	// which is unsafe after any response bytes have been written.
	retry := cfg.Retry
	if httpWriter != nil || disableRetry {
		retry = nil
	}

	_, err := Decorate(func(ctx context.Context) (interface{}, error) {
		if err := exec(ctx); err != nil {
			return nil, err
		}
		return nil, nil
	}).
		WithRateLimiter(cfg.RateLimiter).
		WithBulkhead(cfg.Bulkhead).
		WithCircuitBreaker(cfg.CircuitBreaker).
		WithRetry(retry).
		Call(ctx)
	return err
}

func writeHTTPResilienceError(w *statusCapturingResponseWriter, err error) {
	if w == nil || w.wroteHeader {
		return
	}
	status, message := mapResilienceError(err)
	http.Error(w, message, status)
}

func abortGinResilienceError(c *gin.Context, err error) {
	if c.Writer.Written() {
		return
	}
	status, message := mapResilienceError(err)
	c.AbortWithStatusJSON(status, gin.H{"error": message})
}

func mapResilienceError(err error) (status int, message string) {
	switch {
	case errors.Is(err, ErrCircuitOpen):
		return http.StatusServiceUnavailable, "circuit open"
	case errors.Is(err, ErrBulkheadFull):
		return http.StatusTooManyRequests, "bulkhead full"
	case errors.Is(err, ErrRateLimitExceeded):
		return http.StatusTooManyRequests, "rate limit exceeded"
	default:
		return http.StatusInternalServerError, "internal error"
	}
}

type statusCapturingResponseWriter struct {
	http.ResponseWriter
	statusCode   int
	wroteHeader  bool
	writtenBytes int
}

func (w *statusCapturingResponseWriter) WriteHeader(statusCode int) {
	if w.wroteHeader {
		return
	}
	w.statusCode = statusCode
	w.wroteHeader = true
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *statusCapturingResponseWriter) Write(p []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(p)
	w.writtenBytes += n
	return n, err
}
