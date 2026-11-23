package memory

import (
	"bytes"
	"context"
	"net/http"
	"slices"
	"time"

	"github.com/ioncode/idempotency"
)

// Evaluate атомарно проверяет наличие активной блокировки или кэшированного ответа по ключу.
// Если ключ свободен и отсутствует в кэше, захватывает блокировку на время ttl с привязкой к ownerToken.
func (s *Store) Evaluate(ctx context.Context, key string, ownerToken string, ttl time.Duration) (idempotency.EvaluationStatus, *idempotency.IdempotentRecord, error) {
	if err := ctx.Err(); err != nil {
		return "", nil, err
	}

	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	if lock, ok := s.locks[key]; ok {
		if now.Before(lock.expiresAt) {
			return idempotency.EvalLocked, nil, nil
		}
		delete(s.locks, key)
	}

	if entry, ok := s.records[key]; ok {
		if now.Before(entry.expiresAt) {
			return idempotency.EvalHit, cloneRecord(entry.record), nil
		}
		delete(s.records, key)
	}

	s.locks[key] = lockEntry{
		ownerToken: ownerToken,
		expiresAt:  now.Add(ttl),
	}

	return idempotency.EvalMiss, nil, nil
}

// ExtendLock атомарно продлевает время жизни активной блокировки на период ttl,
// если она существует, не истекла и принадлежит ownerToken.
func (s *Store) ExtendLock(ctx context.Context, key string, ownerToken string, ttl time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}

	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	lock, ok := s.locks[key]
	if !ok || !now.Before(lock.expiresAt) {
		delete(s.locks, key)
		return false, nil
	}

	if lock.ownerToken != ownerToken {
		return false, nil
	}

	lock.expiresAt = now.Add(ttl)
	s.locks[key] = lock
	return true, nil
}

// Unlock снимает блокировку по ключу, если она принадлежит указанному ownerToken.
func (s *Store) Unlock(ctx context.Context, key string, ownerToken string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if lock, ok := s.locks[key]; ok && lock.ownerToken == ownerToken {
		delete(s.locks, key)
	}

	return nil
}

// Save сохраняет глубокую копию записи ответа в оперативной памяти с заданным временем жизни ttl.
func (s *Store) Save(ctx context.Context, record *idempotency.IdempotentRecord, ttl time.Duration) error {
	if record == nil {
		return idempotency.ErrNilRecord
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	cloned := cloneRecord(record)
	if cloned.CreatedAt.IsZero() {
		cloned.CreatedAt = time.Now().UTC()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.records[cloned.Key] = recordEntry{
		record:    cloned,
		expiresAt: time.Now().Add(ttl),
	}

	return nil
}

// cloneRecord создает глубокую копию IdempotentRecord для изоляции внутреннего кэша от внешних мутаций.
func cloneRecord(src *idempotency.IdempotentRecord) *idempotency.IdempotentRecord {
	if src == nil {
		return nil
	}

	var headersCopy http.Header
	if src.ResponseHeaders != nil {
		headersCopy = make(http.Header, len(src.ResponseHeaders))
		for k, v := range src.ResponseHeaders {
			headersCopy[k] = slices.Clone(v)
		}
	}

	return &idempotency.IdempotentRecord{
		Key:             src.Key,
		PayloadHash:     src.PayloadHash,
		StatusCode:      src.StatusCode,
		ResponseBody:    bytes.Clone(src.ResponseBody),
		ResponseHeaders: headersCopy,
		CreatedAt:       src.CreatedAt,
	}
}
