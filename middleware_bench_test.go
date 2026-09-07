package idempotency_test

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ioncode/idempotency"
	"github.com/ioncode/ulog"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
)

func BenchmarkMiddleware_Processing(b *testing.B) {
	baseLog := zerolog.New(io.Discard)
	logAdapter := ulog.NewZerologAdapter(baseLog)

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	repo := idempotency.NewRedisRepository(rdb)

	middleware := idempotency.NewMiddleware(repo, logAdapter)
	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"success"}`))
	})
	router := middleware(nextHandler)

	reqBody := []byte(`{"order_id":100052,"amount":450.50,"currency":"USD"}`)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			req := httptest.NewRequest(http.MethodPost, "/v1/charge", bytes.NewBuffer(reqBody))
			req.Header.Set("Idempotency-Key", "bench-key-id")
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)
		}
	})
}
