// Package idempotency предоставляет высокопроизводительный Chi-совместимый middleware транспортного уровня.
// Модуль реализует паттерн «Идемпотентность» для High-Load микросервисов, защищая распределенное ядро
// от повторных списаний средств и дублирования сущностей при сетевых сбоях.
package idempotency

import (
	"context"
	"net/http"
	"time"

	"github.com/ioncode/ulog"
)

// Status определяет строковый тип для внутренних состояний обработки идемпотентности.
type Status string

const (
	// StatusHit означает, что ответ успешно взят и воспроизведен из распределенного кэша.
	StatusHit Status = "HIT"

	// StatusMiss означает, что это первый уникальный запрос, который ушел на выполнение в бизнес-логику.
	StatusMiss Status = "MISS"

	// StatusConflict означает, что пришел тот же ключ, но с другим телом запроса (изменение payload).
	StatusConflict Status = "CONFLICT"

	// StatusProcessing означает, что идентичный параллельный запрос уже обрабатывается другой горутиной.
	StatusProcessing Status = "PROCESSING"

	// StatusBypassFailOpen означает, что хранилище (Redis) деградировало, и запрос пропущен без проверки.
	StatusBypassFailOpen Status = "BYPASS_FAIL_OPEN"

	// StatusBypassTooLarge означает, что размер ответа превысил лимит RAM, и кэширование пропущено во избежание OOM.
	StatusBypassTooLarge Status = "BYPASS_TOO_LARGE"
)

// String возвращает строковое представление статуса.
func (s Status) String() string {
	return string(s)
}

// KeyExtractor определяет сигнатуру функции для динамического извлечения ключа идемпотентности из HTTP-запроса.
type KeyExtractor func(r *http.Request) string

// LoggerEvent описывает событие логирования, совместимое с Fluent API библиотеки ulog.
type LoggerEvent interface {
	Str(key string, val string) LoggerEvent
	Int(key string, val int) LoggerEvent
	Err(err error) LoggerEvent
	Msg(msg string)
}

// Logger описывает методы логирования на уровне интерфейса приложения для изоляции слоев.
type Logger interface {
	Info() ulog.LoggerEvent
	Warn() ulog.LoggerEvent
	Error() ulog.LoggerEvent
}

// IdempotentRecord описывает структуру сохраненного HTTP-ответа в кэше.
type IdempotentRecord struct {
	Key             string
	PayloadHash     string
	StatusCode      int
	ResponseBody    []byte
	ResponseHeaders map[string][]string
	CreatedAt       time.Time
}

// Repository — атомарный контракт для хранения данных идемпотентности и управления блокировками.
type Repository interface {
	// EvaluateIdempotency атомарно проверяет лок и кэш данных в Redis с помощью Lua-скрипта.
	EvaluateIdempotency(ctx context.Context, key string, payloadHash string, ttl time.Duration) (string, *IdempotentRecord, error)

	// ExtendLock атомарно продлевает время жизни блокировки (Heartbeat) во время долгой работы хэндлера.
	ExtendLock(ctx context.Context, key string, payloadHash string, ttl time.Duration) (bool, error)

	// Unlock снимает распределенную блокировку.
	Unlock(ctx context.Context, key string) error

	// Save персистирует успешный HTTP-ответ в хранилище.
	Save(ctx context.Context, record *IdempotentRecord, ttl time.Duration) error
}
