package idempotency

import "errors"

var (
	// ErrNilRecord возвращается при попытке сохранить nil-запись в хранилище.
	ErrNilRecord = errors.New("idempotency: record cannot be nil")

	// ErrMissingKey возвращается, если в HTTP-запросе отсутствует обязательный ключ идемпотентности.
	ErrMissingKey = errors.New("idempotency: key is missing")

	// ErrPayloadMismatch возвращается, когда повторный запрос с тем же ключом содержит другое тело запроса.
	ErrPayloadMismatch = errors.New("idempotency: payload hash mismatch")

	// ErrConcurrentTimeout возвращается при истечении времени ожидания параллельного запроса с тем же ключом.
	ErrConcurrentTimeout = errors.New("idempotency: concurrent processing timeout")

	// ErrStoreUnavailable возвращается в режиме FailClosed при недоступности хранилища или разомкнутом Circuit Breaker.
	ErrStoreUnavailable = errors.New("idempotency: storage unavailable")

	// ErrDeadlinePriorToLock возвращается, если дедлайн контекста запроса истек еще до захвата блокировки.
	ErrDeadlinePriorToLock = errors.New("idempotency: context deadline exceeded prior to locking")
)
