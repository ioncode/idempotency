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
	// StateHalfOpen означает, что истек cooldown-таймаут, и цепь пропускает один пробный запрос для проверки восстановления Redis.
	StateHalfOpen = "HALF_OPEN"
)

const (
	// DefaultCircuitBreakerThreshold — количество последовательных сбоев Redis, после которого цепь размыкается.
	DefaultCircuitBreakerThreshold = 5
	// DefaultCircuitBreakerCooldown — время охлаждения, после которого предохранитель переходит в Half-Open.
	DefaultCircuitBreakerCooldown = 30 * time.Second
)

// circuitBreaker реализует конечный автомат защиты Go-процесса от сетевых деградаций Redis.
type circuitBreaker struct {
	mu               sync.RWMutex
	state            string
	failureCount     int
	failureThreshold int
	cooldownTimeout  time.Duration
	lastStateMod     time.Time
}

// NewCircuitBreaker инициализирует предохранитель в состоянии CLOSED с заданным порогом сбоев и временем восстановления.
func NewCircuitBreaker(threshold int, cooldown time.Duration) *circuitBreaker {
	if threshold <= 0 {
		threshold = DefaultCircuitBreakerThreshold
	}
	if cooldown <= 0 {
		cooldown = DefaultCircuitBreakerCooldown
	}
	return &circuitBreaker{
		state:            StateClosed,
		failureThreshold: threshold,
		cooldownTimeout:  cooldown,
	}
}

// Allow проверяет состояние предохранителя и управляет переходом в HALF_OPEN по истечении cooldown-таймаута.
func (cb *circuitBreaker) Allow() bool {
	cb.mu.RLock()
	state := cb.state
	expired := state == StateOpen && time.Since(cb.lastStateMod) > cb.cooldownTimeout
	cb.mu.RUnlock()

	if state == StateClosed {
		return true
	}

	if expired {
		cb.mu.Lock()
		defer cb.mu.Unlock()
		// Повторная проверка под эксклюзивной блокировкой (Double-Checked Locking)
		// гарантирует пропуск ровно одного пробного запроса.
		if cb.state == StateOpen && time.Since(cb.lastStateMod) > cb.cooldownTimeout {
			cb.state = StateHalfOpen
			return true
		}
	}

	return false
}

// RecordResult фиксирует результат сетевой операции. Достижение порога ошибок размыкает цепь.
// Клиентские отмены контекста (Canceled/DeadlineExceeded) игнорируются и не влияют на счетчик сбоев.
func (cb *circuitBreaker) RecordResult(err error) {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()

	if err == nil {
		cb.failureCount = 0
		cb.state = StateClosed
		return
	}

	// Неудачный пробный запрос в состоянии HALF_OPEN немедленно возвращает цепь в OPEN на новый цикл ожидания.
	if cb.state == StateHalfOpen {
		cb.state = StateOpen
		cb.lastStateMod = time.Now()
		return
	}

	cb.failureCount++
	if cb.failureCount >= cb.failureThreshold {
		cb.state = StateOpen
		cb.lastStateMod = time.Now()
	}
}
