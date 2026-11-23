package idempotency

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/ioncode/ulog/v3"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// middleware инкапсулирует состояние и зависимости HTTP-перехватчика идемпотентности.
type middleware struct {
	locker     Locker
	saver      Saver
	log        Logger
	cfg        *Config
	cb         *CircuitBreaker
	bufferPool *sync.Pool
	sf         singleflight.Group
}

// NewMiddleware возвращает Chi-совместимый HTTP middleware для дедупликации мутирующих запросов.
//
// Параметры:
//   - locker: реализация распределенных блокировок для предотвращения гонок при параллельных запросах.
//   - saver: хранилище для сохранения и последующего воспроизведения успешных HTTP-ответов.
//   - log: структурированный логгер (ulog/v3); может быть nil для отключения логирования.
//   - opts: функциональные опции для переопределения конфигурации по умолчанию.
func NewMiddleware(locker Locker, saver Saver, log Logger, opts ...Option) func(next http.Handler) http.Handler {
	cfg := &Config{
		KeyExtractor:           DefaultHeaderExtractor(DefaultHeaderName),
		ResponseStatusHeader:   HeaderIdempotencyStat,
		LockTTL:                DefaultLockTTL,
		ResponseTTL:            DefaultResponseTTL,
		MaxCacheBodySize:       DefaultMaxCacheBody,
		MaxRequestPayloadLimit: DefaultMaxRequestPayload,
		CoalesceTimeout:        DefaultCoalesceTimeout,
		CoalesceInterval:       DefaultCoalesceInterval,
		Tracer:                 otel.Tracer("github.com/ioncode/idempotency"),
		RetryAfter:             DefaultRetryAfter,
		ConflictStatusCode:     http.StatusUnprocessableEntity,
	}

	for _, opt := range opts {
		if opt != nil {
			opt(cfg)
		}
	}

	m := &middleware{
		locker: locker,
		saver:  saver,
		log:    log,
		cfg:    cfg,
		cb:     NewCircuitBreaker(cfg.CircuitBreakerThreshold, cfg.CircuitBreakerCooldown),
		bufferPool: &sync.Pool{
			New: func() any {
				return bytes.NewBuffer(make([]byte, 0, 4096))
			},
		},
	}

	return m.handler
}

// isMutatingMethod проверяет, относится ли HTTP-метод к мутирующим операциям,
// требующим проверки идемпотентности (POST, PUT, PATCH).
func isMutatingMethod(method string) bool {
	return method == http.MethodPost || method == http.MethodPut || method == http.MethodPatch
}

// handler оборачивает следующий http.Handler в слой дедупликации и защиты от гонок.
func (m *middleware) handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !isMutatingMethod(r.Method) {
			next.ServeHTTP(w, r)
			return
		}

		requestPath := r.URL.Path
		traceID := ulog.GetTraceID(r.Context())
		idempotencyKey := m.cfg.KeyExtractor(r)

		if idempotencyKey == "" {
			http.Error(w, "Idempotency key is missing", http.StatusBadRequest)
			return
		}

		ctx, span := m.cfg.Tracer.Start(r.Context(), "idempotency.middleware",
			trace.WithSpanKind(trace.SpanKindInternal),
			trace.WithAttributes(
				attribute.String("idempotency.key", idempotencyKey),
				attribute.String("idempotency.path", requestPath),
				attribute.String("ulog.trace_id", traceID),
			),
		)
		defer span.End()

		span.SetAttributes(attribute.String("circuit_breaker.state", m.cb.State()))

		if !m.cb.Allow() {
			if m.cfg.FailureStrategy == FailClosed {
				m.setStatusHeader(w, StatusFailClosed)
				globalMetrics.IncRequests(StatusFailClosed, requestPath)
				span.SetAttributes(attribute.String("idempotency.status", StatusFailClosed.String()))
				if m.log != nil {
					m.log.Info("Circuit Breaker is OPEN. Request rejected (Fail-Closed).",
						ulog.String("idempotency_key", idempotencyKey),
					)
				}
				http.Error(w, "Idempotency store unavailable", http.StatusServiceUnavailable)
				return
			}

			m.setStatusHeader(w, StatusBypassFailOpen)
			globalMetrics.IncRequests(StatusBypassFailOpen, requestPath)
			span.SetAttributes(attribute.String("idempotency.status", StatusBypassFailOpen.String()))
			if m.log != nil {
				m.log.Info("Circuit Breaker is OPEN. Idempotency layer bypassed (Fail-Open).",
					ulog.String("idempotency_key", idempotencyKey),
				)
			}
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}

		coalesceTimeout := m.cfg.CoalesceTimeout
		if deadline, ok := ctx.Deadline(); ok {
			if clientTimeout := time.Until(deadline) - 50*time.Millisecond; clientTimeout < coalesceTimeout {
				coalesceTimeout = clientTimeout
				if coalesceTimeout <= 0 {
					span.RecordError(ErrDeadlinePriorToLock)
					http.Error(w, ErrDeadlinePriorToLock.Error(), http.StatusGatewayTimeout)
					return
				}
			}
		}

		hashReader := newHashingReader(r.Body, m.cfg.MaxRequestPayloadLimit)
		r.Body = hashReader

		ownerToken := rand.Text()
		isStoreAvailable := true
		startTime := time.Now()
		var record *IdempotentRecord

		for {
			evalStatus, rec, err := m.locker.Evaluate(ctx, idempotencyKey, ownerToken, m.cfg.LockTTL)
			m.cb.RecordResult(err)
			if err != nil {
				span.RecordError(err)
				if m.cfg.FailureStrategy == FailClosed {
					m.setStatusHeader(w, StatusFailClosed)
					globalMetrics.IncRequests(StatusFailClosed, requestPath)
					span.SetAttributes(attribute.String("idempotency.status", StatusFailClosed.String()))
					if m.log != nil {
						m.log.Error("Idempotency store error. Request rejected (Fail-Closed).",
							err,
							ulog.String("idempotency_key", idempotencyKey),
						)
					}
					http.Error(w, "Idempotency store unavailable", http.StatusServiceUnavailable)
					return
				}
				isStoreAvailable = false
				break
			}

			if evalStatus == EvalMiss {
				break
			}
			if evalStatus == EvalHit {
				record = rec
				break
			}

			if time.Since(startTime) > coalesceTimeout {
				if m.cfg.RetryAfter > 0 {
					w.Header().Set("Retry-After", strconv.Itoa(max(1, int(m.cfg.RetryAfter.Seconds()))))
				}
				m.setStatusHeader(w, StatusProcessing)
				globalMetrics.IncRequests(StatusProcessing, requestPath)
				span.SetAttributes(attribute.String("idempotency.status", StatusProcessing.String()))
				http.Error(w, "Concurrent processing timeout", http.StatusTooEarly)
				return
			}

			select {
			case <-ctx.Done():
				return
			case <-time.After(m.cfg.CoalesceInterval):
			}
		}

		if record != nil {
			if record.PayloadHash != "" {
				_, _ = io.Copy(io.Discard, hashReader)
				incomingHash := hashReader.Sum()

				if hashReader.TooLarge || incomingHash != record.PayloadHash {
					m.setStatusHeader(w, StatusConflict)
					globalMetrics.IncRequests(StatusConflict, requestPath)
					span.SetAttributes(attribute.String("idempotency.status", StatusConflict.String()))
					http.Error(w, "Idempotency Key payload mismatch", m.cfg.ConflictStatusCode)
					return
				}
			}

			copyCachedHeaders(w.Header(), record.ResponseHeaders)
			if !record.CreatedAt.IsZero() {
				w.Header().Set(HeaderOriginalDate, record.CreatedAt.UTC().Format(http.TimeFormat))
			}
			m.setStatusHeader(w, StatusHit)
			globalMetrics.IncRequests(StatusHit, requestPath)
			span.SetAttributes(attribute.String("idempotency.status", StatusHit.String()))
			w.WriteHeader(record.StatusCode)
			_, _ = w.Write(record.ResponseBody)
			return
		}

		stopRenewal := make(chan struct{})
		if isStoreAvailable {
			go func() {
				ticker := time.NewTicker(m.cfg.LockTTL / 3)
				defer ticker.Stop()
				for {
					select {
					case <-ticker.C:
						if extended, _ := m.locker.ExtendLock(ctx, idempotencyKey, ownerToken, m.cfg.LockTTL); !extended {
							return
						}
					case <-ctx.Done():
						unlockCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
						_ = m.locker.Unlock(unlockCtx, idempotencyKey, ownerToken)
						cancel()
						return
					case <-stopRenewal:
						return
					}
				}
			}()
			defer func() {
				close(stopRenewal)
				unlockCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
				_ = m.locker.Unlock(unlockCtx, idempotencyKey, ownerToken)
				cancel()
			}()
		}

		buf := m.bufferPool.Get().(*bytes.Buffer)
		buf.Reset()
		defer m.bufferPool.Put(buf)

		crw := NewProductionResponseWriter(w, buf, m.cfg.MaxCacheBodySize, m.cfg.ResponseStatusHeader)
		statusTag := StatusMiss
		if !isStoreAvailable {
			statusTag = StatusBypassFailOpen
		}

		next.ServeHTTP(crw, r.WithContext(ctx))

		computedHash := hashReader.Sum()
		if hashReader.TooLarge || crw.TooLarge {
			statusTag = StatusBypassTooLarge
		}

		if isStoreAvailable && crw.StatusCode < 500 && statusTag == StatusMiss && ctx.Err() == nil {
			savedRecord := &IdempotentRecord{
				Key:             idempotencyKey,
				PayloadHash:     computedHash,
				StatusCode:      crw.StatusCode,
				ResponseBody:    bytes.Clone(crw.Body.Bytes()),
				ResponseHeaders: filterHeaders(crw.Header(), m.cfg.ResponseStatusHeader),
				CreatedAt:       time.Now().UTC(),
			}

			saveErr := m.saver.Save(ctx, savedRecord, m.cfg.ResponseTTL)
			m.cb.RecordResult(saveErr)
			if saveErr != nil {
				span.RecordError(saveErr)
				if m.log != nil {
					m.log.Error("Failed to save idempotent response record.",
						saveErr,
						ulog.String("idempotency_key", idempotencyKey),
					)
				}
			}
		}

		globalMetrics.IncRequests(statusTag, requestPath)
		span.SetAttributes(attribute.String("idempotency.status", statusTag.String()))
		crw.Commit(statusTag.String())
	})
}

// setStatusHeader записывает диагностический статус идемпотентности в заголовок ответа,
// если имя заголовка не пустое.
func (m *middleware) setStatusHeader(w http.ResponseWriter, status Status) {
	if m.cfg.ResponseStatusHeader != "" {
		w.Header().Set(m.cfg.ResponseStatusHeader, status.String())
	}
}
