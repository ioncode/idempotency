// Package idempotency предоставляет высокопроизводительный HTTP-middleware
// для обеспечения идемпотентности мутирующих запросов в распределенных системах.
package idempotency

import (
	"context"
	"net/http"
	"time"

	"github.com/ioncode/ulog/v3"
)

const (
	HeaderIdempotencyKey  = "Idempotency-Key"
	HeaderIdempotencyStat = "X-Idempotency-Status"
)

type Status string

const (
	StatusHit            Status = "HIT"
	StatusMiss           Status = "MISS"
	StatusConflict       Status = "CONFLICT"
	StatusProcessing     Status = "PROCESSING"
	StatusBypassFailOpen Status = "BYPASS_FAIL_OPEN"
	StatusFailClosed     Status = "FAIL_CLOSED"
	StatusBypassTooLarge Status = "BYPASS_TOO_LARGE"
)

func (s Status) String() string {
	return string(s)
}

type EvaluationStatus string

const (
	EvalMiss   EvaluationStatus = "MISS"
	EvalHit    EvaluationStatus = "HIT"
	EvalLocked EvaluationStatus = "LOCKED"
)

type KeyExtractor func(r *http.Request) string
type Logger = ulog.Logger

type IdempotentRecord struct {
	Key             string      `json:"key"`
	PayloadHash     string      `json:"payload_hash"`
	StatusCode      int         `json:"status_code"`
	ResponseBody    []byte      `json:"response_body,omitempty"`
	ResponseHeaders http.Header `json:"response_headers,omitempty"`
	CreatedAt       time.Time   `json:"created_at"`
}

type Locker interface {
	Evaluate(ctx context.Context, key string, ownerToken string, ttl time.Duration) (EvaluationStatus, *IdempotentRecord, error)
	ExtendLock(ctx context.Context, key string, ownerToken string, ttl time.Duration) (bool, error)
	Unlock(ctx context.Context, key string, ownerToken string) error
}

type Saver interface {
	Save(ctx context.Context, record *IdempotentRecord, ttl time.Duration) error
}

type Store interface {
	Locker
	Saver
}
