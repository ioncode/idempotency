package idempotency

import (
	"context"
	"errors"
	"sync"
	"time"
)

// circuitBreaker реализует конечный автомат защиты Go-процесса от сетевых деградаций Redis.
type circuitBreaker struct {
	mu           sync.RWMutex
	state        string
	failureCount int
	lastStateMod time.Time
}

// Allow проверяет, открыт ли предохранитель цепи. Поддерживает автоматический cooldown (30с).
func (cb *circuitBreaker) Allow() bool {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	if cb.state == "OPEN" {
		return time.Since(cb.lastStateMod) > 30*time.Second
	}
	return true
}

// RecordResult фиксирует результат сетевой операции. 5 ошибок подряд размыкают цепь.
func (cb *circuitBreaker) RecordResult(err error) {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		cb.mu.Lock()
		cb.failureCount = 0
		cb.state = "CLOSED"
		cb.mu.Unlock()
		return
	}
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failureCount++
	if cb.failureCount >= 5 {
		cb.state = "OPEN"
		cb.lastStateMod = time.Now()
	}
}
