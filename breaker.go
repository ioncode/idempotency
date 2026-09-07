package idempotency

import (
	"context"
	"errors"
	"sync"
	"time"
)

const (
	// StateClosed означает, что цепь замкнута, Redis здоров, и запросы проходят стандартную проверку.
	StateClosed = "CLOSED"
	// StateOpen означает, что цепь разомкнута из-за сбоев, и активирована стратегия Fail-Open.
	StateOpen = "OPEN"
)

// circuitBreaker реализует конечный автомат защиты Go-процесса от сетевых деградаций Redis.
type circuitBreaker struct {
	mu           sync.RWMutex
	state        string
	failureCount int
	lastStateMod time.Time
}

// NewCircuitBreaker гарантирует корректное стартовое состояние CLOSED для каждого инстанса middleware.
func NewCircuitBreaker() *circuitBreaker {
	return &circuitBreaker{
		state: StateClosed,
	}
}

// Allow проверяет, открыт ли предохранитель цепи. Поддерживает автоматический cooldown (30с).
func (cb *circuitBreaker) Allow() bool {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	if cb.state == StateOpen {
		// Cooldown таймаут 30 секунд для попытки авто-восстановления
		return time.Since(cb.lastStateMod) > 30*time.Second
	}
	return true
}

// RecordResult фиксирует результат сетевой операции. 5 ошибок подряд размыкают цепь.
// Клиентские отмены контекста (Canceled/DeadlineExceeded) автоматически игнорируются.
func (cb *circuitBreaker) RecordResult(err error) {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		cb.mu.Lock()
		cb.failureCount = 0
		cb.state = StateClosed
		cb.mu.Unlock()
		return
	}
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failureCount++
	if cb.failureCount >= 5 {
		cb.state = StateOpen
		cb.lastStateMod = time.Now()
	}
}
