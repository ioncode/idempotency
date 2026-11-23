package idempotency

import (
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/otel/trace"
)

const (
	// DefaultHeaderName — имя HTTP-заголовка по умолчанию для передачи ключа идемпотентности.
	DefaultHeaderName = HeaderIdempotencyKey

	// DefaultLockTTL — время жизни распределенной блокировки по умолчанию на время выполнения запроса.
	DefaultLockTTL = 10 * time.Second

	// DefaultResponseTTL — время хранения кэшированного ответа по умолчанию (24 часа).
	DefaultResponseTTL = 24 * time.Hour

	// DefaultMaxCacheBody — максимальный размер тела ответа для кэширования по умолчанию (2 МБ).
	DefaultMaxCacheBody = 2 * 1024 * 1024

	// DefaultMaxRequestPayload — максимальный размер входящего тела запроса для хеширования по умолчанию (10 МБ).
	DefaultMaxRequestPayload = 10 * 1024 * 1024

	// DefaultCoalesceTimeout — максимальное время ожидания завершения параллельного запроса с тем же ключом.
	DefaultCoalesceTimeout = 3 * time.Second

	// DefaultCoalesceInterval — базовый интервал опроса статуса блокировки при ожидании.
	DefaultCoalesceInterval = 150 * time.Millisecond

	// DefaultRetryAfter — значение заголовка Retry-After по умолчанию при возврате 425 Too Early / 409 Conflict.
	DefaultRetryAfter = 2 * time.Second
)

// FailureStrategy определяет поведение middleware при отказе или недоступности хранилища (Locker/Saver).
type FailureStrategy int

const (
	// FailOpen пропускает запросы напрямую в бизнес-логику при недоступности хранилища (приоритет доступности).
	FailOpen FailureStrategy = iota

	// FailClosed отклоняет запросы с кодом 503 Service Unavailable при недоступности хранилища (приоритет консистентности).
	FailClosed
)

// Config содержит настройки работы middleware идемпотентности.
type Config struct {
	KeyExtractor            KeyExtractor
	ResponseStatusHeader    string
	LockTTL                 time.Duration
	ResponseTTL             time.Duration
	MaxCacheBodySize        int
	MaxRequestPayloadLimit  int64
	CoalesceTimeout         time.Duration
	CoalesceInterval        time.Duration
	Tracer                  trace.Tracer
	RetryAfter              time.Duration
	CircuitBreakerThreshold int
	CircuitBreakerCooldown  time.Duration
	FailureStrategy         FailureStrategy
	ConflictStatusCode      int
}

// Option определяет функциональную опцию для настройки Config.
type Option func(*Config)

// WithKeyExtractor задает пользовательскую функцию извлечения ключа идемпотентности из запроса.
func WithKeyExtractor(extractor KeyExtractor) Option {
	return func(c *Config) {
		if extractor != nil {
			c.KeyExtractor = extractor
		}
	}
}

// WithResponseStatusHeader задает имя HTTP-заголовка для возврата статуса обработки (например, X-Idempotency-Status).
// Передайте пустую строку, чтобы отключить добавление заголовка.
func WithResponseStatusHeader(headerName string) Option {
	return func(c *Config) { c.ResponseStatusHeader = headerName }
}

// WithLockTTL задает время жизни распределенной блокировки на период выполнения запроса.
func WithLockTTL(ttl time.Duration) Option {
	return func(c *Config) {
		if ttl > 0 {
			c.LockTTL = ttl
		}
	}
}

// WithResponseTTL задает время жизни кэшированного результата в хранилище.
func WithResponseTTL(ttl time.Duration) Option {
	return func(c *Config) {
		if ttl > 0 {
			c.ResponseTTL = ttl
		}
	}
}

// WithMaxCacheBodySize задает максимальный размер тела ответа (в байтах), который будет сохранен в кэш.
func WithMaxCacheBodySize(bytes int) Option {
	return func(c *Config) {
		if bytes > 0 {
			c.MaxCacheBodySize = bytes
		}
	}
}

// WithMaxRequestPayloadLimit задает лимит чтения тела запроса (в байтах) для расчета контрольной суммы.
func WithMaxRequestPayloadLimit(bytes int64) Option {
	return func(c *Config) {
		if bytes > 0 {
			c.MaxRequestPayloadLimit = bytes
		}
	}
}

// WithCoalesceTimeout задает максимальное время ожидания параллельных запросов с идентичным ключом.
func WithCoalesceTimeout(d time.Duration) Option {
	return func(c *Config) {
		if d > 0 {
			c.CoalesceTimeout = d
		}
	}
}

// WithCoalesceInterval задает интервал проверки готовности результата при ожидании параллельного запроса.
func WithCoalesceInterval(d time.Duration) Option {
	return func(c *Config) {
		if d > 0 {
			c.CoalesceInterval = d
		}
	}
}

// WithTracer задает экземпляр OpenTelemetry Tracer для инструментирования запросов.
func WithTracer(t trace.Tracer) Option {
	return func(c *Config) {
		if t != nil {
			c.Tracer = t
		}
	}
}

// WithRetryAfter задает значение заголовка Retry-After при превышении таймаута коалесцирования.
func WithRetryAfter(d time.Duration) Option {
	return func(c *Config) {
		if d >= 0 {
			c.RetryAfter = d
		}
	}
}

// WithCircuitBreakerThreshold задает порог последовательных ошибок хранилища для размыкания Circuit Breaker.
func WithCircuitBreakerThreshold(threshold int) Option {
	return func(c *Config) {
		if threshold > 0 {
			c.CircuitBreakerThreshold = threshold
		}
	}
}

// WithCircuitBreakerCooldown задает время восстановления (half-open) для Circuit Breaker.
func WithCircuitBreakerCooldown(cooldown time.Duration) Option {
	return func(c *Config) {
		if cooldown > 0 {
			c.CircuitBreakerCooldown = cooldown
		}
	}
}

// WithFailureStrategy задает стратегию поведения при сбое хранилища (FailOpen или FailClosed).
func WithFailureStrategy(strategy FailureStrategy) Option {
	return func(c *Config) { c.FailureStrategy = strategy }
}

// WithConflictStatusCode позволяет переопределить HTTP-код при несовпадении тела запроса (по умолчанию 422 Unprocessable Entity).
func WithConflictStatusCode(code int) Option {
	return func(c *Config) {
		if code >= 400 && code < 500 {
			c.ConflictStatusCode = code
		}
	}
}

// DefaultHeaderExtractor возвращает стандартный экстрактор ключа из указанного HTTP-заголовка,
// очищая значение от обрамляющих кавычек (согласно спецификации Structured Field Values RFC 8941).
func DefaultHeaderExtractor(headerName string) KeyExtractor {
	return func(r *http.Request) string {
		return strings.TrimSpace(strings.Trim(r.Header.Get(headerName), "\""))
	}
}
