package idempotency

import (
	"context"
	"errors"
	"sync"
	"time"
)

const (
	// StateClosed означает, что цепь замкнута, хранилище доступно,
	// и запросы проходят стандартную проверку идемпотентности.
	StateClosed = "CLOSED"

	// StateOpen означает, что цепь разомкнута из-за серии сбоев хранилища,
	// и активирована выбранная стратегия обработки отказов (Fail-Open или Fail-Closed).
	StateOpen = "OPEN"

	// StateHalfOpen означает, что истек cooldown-таймаут, и цепь пропускает
	// один пробный запрос для проверки восстановления хранилища.
	StateHalfOpen = "HALF_OPEN"
)

const (
	// DefaultCircuitBreakerThreshold — количество последовательных сбоев хранилища
	// по умолчанию, после которого цепь размыкается.
	DefaultCircuitBreakerThreshold = 5

	// DefaultCircuitBreakerCooldown — время охлаждения по умолчанию,
	// по истечении которого предохранитель переходит в состояние StateHalfOpen.
	DefaultCircuitBreakerCooldown = 30 * time.Second
)

// CircuitBreaker реализует потокобезопасный конечный автомат (CLOSED -> OPEN -> HALF_OPEN)
// для изоляции приложения от сетевых деградаций и таймаутов хранилища идемпотентности.
type CircuitBreaker struct {
	mu               sync.RWMutex
	state            string
	failureCount     int
	failureThreshold int
	cooldownTimeout  time.Duration
	lastStateMod     time.Time
}

// NewCircuitBreaker инициализирует предохранитель в состоянии StateClosed
// с заданным порогом последовательных сбоев и временем восстановления.
// Если переданы неположительные значения, применяются DefaultCircuitBreakerThreshold
// и DefaultCircuitBreakerCooldown.
func NewCircuitBreaker(threshold int, cooldown time.Duration) *CircuitBreaker {
	if threshold <= 0 {
		threshold = DefaultCircuitBreakerThreshold
	}
	if cooldown <= 0 {
		cooldown = DefaultCircuitBreakerCooldown
	}
	return &CircuitBreaker{
		state:            StateClosed,
		failureThreshold: threshold,
		cooldownTimeout:  cooldown,
	}
}

// State потокобезопасно возвращает текущее состояние предохранителя
// (StateClosed, StateOpen или StateHalfOpen).
func (cb *CircuitBreaker) State() string {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// Allow проверяет, разрешено ли выполнение операции с хранилищем идемпотентности.
// В состоянии StateClosed возвращает true. В состоянии StateOpen по истечении
// cooldownTimeout переводит предохранитель в StateHalfOpen по паттерну
// Double-Checked Locking, гарантируя пропуск ровно одного пробного запроса.
func (cb *CircuitBreaker) Allow() bool {
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

// RecordResult фиксирует исход операции взаимодействия с хранилищем идемпотентности.
// При отсутствии ошибки сбрасывает счетчик сбоев и замыкает цепь (StateClosed).
// При достижении failureThreshold переводит цепь в состояние StateOpen.
// Клиентские отмены контекста (context.Canceled и context.DeadlineExceeded)
// игнорируются и не влияют на состояние предохранителя.
func (cb *CircuitBreaker) RecordResult(err error) {
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

	if cb.state == StateOpen {
		return
	}

	// Неудачный пробный запрос в состоянии StateHalfOpen немедленно возвращает цепь в StateOpen.
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
