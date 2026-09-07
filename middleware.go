package idempotency

import (
	"bytes"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ioncode/ulog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const (
	DefaultHeaderName        = "Idempotency-Key"
	DefaultLockTTL           = 10 * time.Second
	DefaultResponseTTL       = 24 * time.Hour
	DefaultMaxCacheBody      = 2 * 1024 * 1024
	DefaultCoalesceTimeout   = 3 * time.Second
	DefaultCoalesceInterval  = 150 * time.Millisecond
	DefaultMaxRequestPayload = 10 * 1024 * 1024

	HeaderOriginalDate    = "Original-Request-Date"
	HeaderIdempotencyStat = "X-Idempotency-Status"
)

var hopByHopHeaders = map[string]bool{
	"connection": true, "keep-alive": true, "proxy-authenticate": true,
	"proxy-authorization": true, "te": true, "trailer": true, "transfer-encoding": true,
	"upgrade": true, "content-length": true, "date": true, "server": true,
}

type Config struct {
	KeyExtractor           KeyExtractor
	ResponseStatusHeader   string
	LockTTL                time.Duration
	ResponseTTL            time.Duration
	MaxCacheBodySize       int
	MaxRequestPayloadLimit int64
	CoalesceTimeout        time.Duration
	Tracer                 trace.Tracer
}

// inMemoryItem описывает элемент локального L1-кэша в оперативной памяти.
type inMemoryItem struct {
	payloadHash string
	expiresAt   time.Time
}

type Option func(*Config)

func WithKeyExtractor(extractor KeyExtractor) Option {
	return func(c *Config) { c.KeyExtractor = extractor }
}
func WithResponseStatusHeader(headerName string) Option {
	return func(c *Config) { c.ResponseStatusHeader = headerName }
}
func WithLockTTL(ttl time.Duration) Option     { return func(c *Config) { c.LockTTL = ttl } }
func WithResponseTTL(ttl time.Duration) Option { return func(c *Config) { c.ResponseTTL = ttl } }
func WithMaxCacheBodySize(bytes int) Option    { return func(c *Config) { c.MaxCacheBodySize = bytes } }
func WithMaxRequestPayloadLimit(bytes int64) Option {
	return func(c *Config) { c.MaxRequestPayloadLimit = bytes }
}
func WithCoalesceTimeout(d time.Duration) Option { return func(c *Config) { c.CoalesceTimeout = d } }
func WithTracer(t trace.Tracer) Option           { return func(c *Config) { c.Tracer = t } }

func DefaultHeaderExtractor(headerName string) KeyExtractor {
	return func(r *http.Request) string { return r.Header.Get(headerName) }
}

// NewMiddleware возвращает Chi-совместимый middleware дедупликации HTTP-запросов.
func NewMiddleware(repo Repository, log Logger, opts ...Option) func(next http.Handler) http.Handler {
	cfg := &Config{
		KeyExtractor:           DefaultHeaderExtractor(DefaultHeaderName),
		ResponseStatusHeader:   HeaderIdempotencyStat,
		LockTTL:                DefaultLockTTL,
		ResponseTTL:            DefaultResponseTTL,
		MaxCacheBodySize:       DefaultMaxCacheBody,
		MaxRequestPayloadLimit: DefaultMaxRequestPayload,
		CoalesceTimeout:        DefaultCoalesceTimeout,
		Tracer:                 otel.Tracer("github.com/ioncode/idempotency"),
	}

	for _, opt := range opts {
		opt(cfg)
	}

	var localCache sync.Map
	cb := NewCircuitBreaker()
	bufferPool := &sync.Pool{New: func() interface{} { return bytes.NewBuffer(make([]byte, 0, 4096)) }}

	go func() {
		for {
			time.Sleep(1 * time.Minute)
			localCache.Range(func(key, value interface{}) bool {
				if item, ok := value.(inMemoryItem); ok && time.Now().After(item.expiresAt) {
					localCache.Delete(key)
				}
				return true
			})
		}
	}()

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost && r.Method != http.MethodPatch && r.Method != http.MethodPut {
				next.ServeHTTP(w, r)
				return
			}

			requestPath := r.URL.Path
			traceID := ulog.GetTraceID(r.Context())
			idempotencyKey := cfg.KeyExtractor(r)

			if idempotencyKey == "" {
				http.Error(w, "Idempotency key is missing", http.StatusBadRequest)
				return
			}

			ctx, span := cfg.Tracer.Start(r.Context(), "idempotency.middleware",
				trace.WithSpanKind(trace.SpanKindInternal),
				trace.WithAttributes(
					attribute.String("idempotency.key", idempotencyKey),
					attribute.String("idempotency.path", requestPath),
					attribute.String("ulog.trace_id", traceID),
				),
			)
			defer span.End()

			cb.mu.RLock()
			cbState := cb.state
			cb.mu.RUnlock()
			span.SetAttributes(attribute.String("circuit_breaker.state", cbState))

			if !cb.Allow() {
				if cfg.ResponseStatusHeader != "" {
					w.Header().Set(cfg.ResponseStatusHeader, StatusBypassFailOpen.String())
				}
				globalMetrics.IncRequests(StatusBypassFailOpen, requestPath)
				span.SetAttributes(attribute.String("idempotency.status", StatusBypassFailOpen.String()))
				if log != nil {
					log.Warn().
						Str(ulog.LogKeyTraceID, traceID).
						Str("idempotency_key", idempotencyKey).
						Msg("Circuit Breaker is OPEN. Idempotency layer bypassed (Fail-Open).")
				}
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}

			coalesceTimeout := cfg.CoalesceTimeout
			if deadline, ok := ctx.Deadline(); ok {
				if clientTimeout := time.Until(deadline) - 50*time.Millisecond; clientTimeout < coalesceTimeout {
					coalesceTimeout = clientTimeout
					if coalesceTimeout <= 0 {
						span.RecordError(errors.New("deadline exceeded prior to locking"))
						http.Error(w, "Context deadline exceeded prior to locking", http.StatusGatewayTimeout)
						return
					}
				}
			}

			hashReader := newHashingReader(r.Body, cfg.MaxRequestPayloadLimit)
			r.Body = hashReader

			isRedisAvailable := true
			startTime := time.Now()
			var record *IdempotentRecord

			for {
				status, rec, err := repo.EvaluateIdempotency(ctx, idempotencyKey, "", cfg.LockTTL)
				cb.RecordResult(err)
				if err != nil {
					span.RecordError(err)
					isRedisAvailable = false
					break
				}

				if status == "MISS" {
					break
				}
				if status == "HIT" {
					record = rec
					break
				}

				if time.Since(startTime) > coalesceTimeout {
					w.Header().Set("Retry-After", "2")
					if cfg.ResponseStatusHeader != "" {
						w.Header().Set(cfg.ResponseStatusHeader, StatusProcessing.String())
					}
					globalMetrics.IncRequests(StatusProcessing, requestPath)
					span.SetAttributes(attribute.String("idempotency.status", StatusProcessing.String()))
					http.Error(w, "Concurrent processing timeout", http.StatusTooEarly)
					return
				}

				select {
				case <-ctx.Done():
					return
				case <-time.After(DefaultCoalesceInterval):
				}
			}

			if record != nil {
				if val, ok := localCache.Load(idempotencyKey); ok {
					if item, ok := val.(inMemoryItem); ok && time.Now().Before(item.expiresAt) {
						if record.PayloadHash != "" && item.payloadHash != "" && record.PayloadHash != item.payloadHash {
							if cfg.ResponseStatusHeader != "" {
								w.Header().Set(cfg.ResponseStatusHeader, StatusConflict.String())
							}
							globalMetrics.IncRequests(StatusConflict, requestPath)
							span.SetAttributes(attribute.String("idempotency.status", StatusConflict.String()))
							http.Error(w, "Idempotency Key payload mismatch", http.StatusConflict)
							return
						}
					}
				}

				if cfg.ResponseStatusHeader != "" {
					w.Header().Set(cfg.ResponseStatusHeader, StatusHit.String())
				}
				for key, values := range record.ResponseHeaders {
					for _, value := range values {
						w.Header().Add(key, value)
					}
				}
				span.SetAttributes(attribute.String("idempotency.status", StatusHit.String()))
				w.WriteHeader(record.StatusCode)
				_, _ = w.Write(record.ResponseBody)
				return
			}

			stopRenewal := make(chan struct{})
			if isRedisAvailable {
				go func() {
					ticker := time.NewTicker(cfg.LockTTL / 3)
					defer ticker.Stop()
					for {
						select {
						case <-ticker.C:
							if extended, _ := repo.ExtendLock(ctx, idempotencyKey, "", cfg.LockTTL); !extended {
								return
							}
						case <-ctx.Done():
							_ = repo.Unlock(ctx, idempotencyKey)
							return
						case <-stopRenewal:
							return
						}
					}
				}()
				defer func() {
					close(stopRenewal)
					_ = repo.Unlock(ctx, idempotencyKey)
				}()
			}

			buf := bufferPool.Get().(*bytes.Buffer)
			buf.Reset()
			defer bufferPool.Put(buf)

			crw := NewProductionResponseWriter(buf, cfg.MaxCacheBodySize)
			statusTag := StatusMiss
			if !isRedisAvailable {
				statusTag = StatusBypassFailOpen
			}

			next.ServeHTTP(crw, r.WithContext(ctx))

			computedHash := hashReader.Sum()
			if hashReader.TooLarge || crw.TooLarge {
				statusTag = StatusBypassTooLarge
			}

			if isRedisAvailable && crw.StatusCode < 500 && statusTag == StatusMiss && ctx.Err() == nil {
				rawHeaders := crw.Header()
				filteredHeaders := make(map[string][]string)
				for key, values := range rawHeaders {
					if hopByHopHeaders[strings.ToLower(key)] {
						continue
					}
					filteredHeaders[key] = values
				}

				savedRecord := &IdempotentRecord{
					Key:             idempotencyKey,
					PayloadHash:     computedHash,
					StatusCode:      crw.StatusCode,
					ResponseBody:    getBufferBytesCopy(crw.Body),
					ResponseHeaders: filteredHeaders,
					CreatedAt:       time.Now(),
				}

				saveErr := repo.Save(ctx, savedRecord, cfg.ResponseTTL)
				cb.RecordResult(saveErr)

				if saveErr == nil {
					localCache.Store(idempotencyKey, inMemoryItem{
						payloadHash: computedHash,
						expiresAt:   time.Now().Add(5 * time.Second),
					})
				}
			}

			if cfg.ResponseStatusHeader != "" {
				w.Header().Set(cfg.ResponseStatusHeader, statusTag.String())
			}
			for key, values := range crw.Header() {
				for _, value := range values {
					w.Header().Add(key, value)
				}
			}
			span.SetAttributes(attribute.String("idempotency.status", statusTag.String()))
			w.WriteHeader(crw.StatusCode)
			_, _ = w.Write(crw.Body.Bytes())
		})
	}
}

func getBufferBytesCopy(b *bytes.Buffer) []byte {
	cb := make([]byte, b.Len())
	copy(cb, b.Bytes())
	return cb
}
