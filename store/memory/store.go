// Package memory предоставляет потокобезопасную in-memory реализацию хранилища
// и распределенных блокировок для пакета idempotency.
// Подходит для модульного и интеграционного тестирования, а также для single-instance развертываний.
package memory

import (
	"sync"
	"time"

	"github.com/ioncode/idempotency"
)

const defaultCleanupInterval = time.Minute

type lockEntry struct {
	ownerToken string
	expiresAt  time.Time
}

type recordEntry struct {
	record    *idempotency.IdempotentRecord
	expiresAt time.Time
}

// Store реализует интерфейсы idempotency.Locker, idempotency.Saver и idempotency.Store в оперативной памяти.
type Store struct {
	mu              sync.RWMutex
	locks           map[string]lockEntry
	records         map[string]recordEntry
	cleanupInterval time.Duration
	done            chan struct{}
	closeOnce       sync.Once
}

// Option определяет функциональную опцию настройки in-memory хранилища.
type Option func(*Store)

// WithCleanupInterval задает интервал фоновой очистки истекших блокировок и записей.
// Передайте 0 или отрицательное значение, чтобы отключить фоновую горутину очистки
// (очистка все равно будет выполняться лениво при обращении к ключам).
func WithCleanupInterval(d time.Duration) Option {
	return func(s *Store) {
		s.cleanupInterval = d
	}
}

// New создает новый экземпляр потокобезопасного in-memory хранилища.
func New(opts ...Option) *Store {
	s := &Store{
		locks:           make(map[string]lockEntry),
		records:         make(map[string]recordEntry),
		cleanupInterval: defaultCleanupInterval,
		done:            make(chan struct{}),
	}

	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}

	if s.cleanupInterval > 0 {
		go s.cleanupLoop()
	}

	return s
}

// Close останавливает фоновую горутину очистки и освобождает внутренние мапы.
func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		close(s.done)
		s.mu.Lock()
		clear(s.locks)
		clear(s.records)
		s.mu.Unlock()
	})
	return nil
}

func (s *Store) cleanupLoop() {
	ticker := time.NewTicker(s.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.evictExpired(time.Now())
		case <-s.done:
			return
		}
	}
}

func (s *Store) evictExpired(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for k, l := range s.locks {
		if !now.Before(l.expiresAt) {
			delete(s.locks, k)
		}
	}
	for k, r := range s.records {
		if !now.Before(r.expiresAt) {
			delete(s.records, k)
		}
	}
}

var (
	_ idempotency.Locker = (*Store)(nil)
	_ idempotency.Saver  = (*Store)(nil)
	_ idempotency.Store  = (*Store)(nil)
)
