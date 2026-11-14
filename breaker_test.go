package idempotency

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCircuitBreaker_StateTransitions(t *testing.T) {
	errRedis := errors.New("redis connection refused")
	cb := NewCircuitBreaker(3, 50*time.Millisecond)

	assert.True(t, cb.Allow())

	cb.RecordResult(errRedis)
	cb.RecordResult(errRedis)
	cb.RecordResult(nil)
	cb.RecordResult(errRedis)
	cb.RecordResult(errRedis)
	assert.True(t, cb.Allow())

	cb.RecordResult(errRedis)
	assert.False(t, cb.Allow())

	time.Sleep(60 * time.Millisecond)

	assert.True(t, cb.Allow())
	assert.False(t, cb.Allow())

	cb.RecordResult(errRedis)
	assert.False(t, cb.Allow())

	time.Sleep(60 * time.Millisecond)
	assert.True(t, cb.Allow())

	cb.RecordResult(nil)
	assert.True(t, cb.Allow())
	assert.True(t, cb.Allow())
}

func TestCircuitBreaker_IgnoreContextErrors(t *testing.T) {
	errRedis := errors.New("redis timeout")
	cb := NewCircuitBreaker(2, 50*time.Millisecond)

	cb.RecordResult(errRedis)
	cb.RecordResult(context.Canceled)
	cb.RecordResult(context.DeadlineExceeded)
	assert.True(t, cb.Allow())

	cb.RecordResult(errRedis)
	assert.False(t, cb.Allow())
}

func TestCircuitBreaker_DefaultValues(t *testing.T) {
	cb := NewCircuitBreaker(0, -5*time.Second)
	assert.Equal(t, DefaultCircuitBreakerThreshold, cb.failureThreshold)
	assert.Equal(t, DefaultCircuitBreakerCooldown, cb.cooldownTimeout)
}

func TestCircuitBreaker_ConcurrentHalfOpen(t *testing.T) {
	errRedis := errors.New("redis down")
	cb := NewCircuitBreaker(1, 30*time.Millisecond)

	cb.RecordResult(errRedis)
	assert.False(t, cb.Allow())

	time.Sleep(40 * time.Millisecond)

	const workers = 50
	var allowedCount atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})

	for range workers {
		wg.Go(func() {
			<-start
			if cb.Allow() {
				allowedCount.Add(1)
			}
		})
	}

	close(start)
	wg.Wait()

	assert.Equal(t, int32(1), allowedCount.Load())
}
