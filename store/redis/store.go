// Package redis предоставляет реализацию хранилища и распределенных блокировок
// для пакета idempotency на базе Redis (поддерживаются standalone, Sentinel и Cluster).
package redis

import (
	"github.com/ioncode/idempotency"
	"github.com/redis/go-redis/v9"
)

const (
	defaultLockPrefix = "lock:idempotency:"
	defaultDataPrefix = "data:idempotency:"
)

// Store реализует интерфейсы idempotency.Locker, idempotency.Saver и idempotency.Store
// поверх клиента go-redis.
type Store struct {
	rdb        redis.UniversalClient
	lockPrefix string
	dataPrefix string
}

// Option определяет функциональную опцию настройки Redis-хранилища.
type Option func(*Store)

// WithKeyPrefix задает пользовательский префикс для ключей блокировок и данных в Redis
// (по умолчанию используются "lock:idempotency:" и "data:idempotency:").
func WithKeyPrefix(prefix string) Option {
	return func(s *Store) {
		if prefix != "" {
			s.lockPrefix = prefix + ":lock:"
			s.dataPrefix = prefix + ":data:"
		}
	}
}

// New создает новый экземпляр Redis-хранилища, реализующий idempotency.Store.
func New(rdb redis.UniversalClient, opts ...Option) *Store {
	s := &Store{
		rdb:        rdb,
		lockPrefix: defaultLockPrefix,
		dataPrefix: defaultDataPrefix,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}
	return s
}

// Проверка на этапе компиляции, что *Store реализует все контракты ядра.
var (
	_ idempotency.Locker = (*Store)(nil)
	_ idempotency.Saver  = (*Store)(nil)
	_ idempotency.Store  = (*Store)(nil)
)

func (s *Store) lockKey(key string) string {
	return s.lockPrefix + key
}

func (s *Store) dataKey(key string) string {
	return s.dataPrefix + key
}
