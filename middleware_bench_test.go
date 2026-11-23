package idempotency_test

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/ioncode/idempotency"
	"github.com/ioncode/idempotency/store/memory"
)

func BenchmarkMiddleware_CacheHit(b *testing.B) {
	store := memory.New(memory.WithCleanupInterval(0))
	defer store.Close()

	mw := idempotency.NewMiddleware(store, store, nil)
	router := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"success"}`))
	}))

	reqBody := []byte(`{"order_id":100052,"amount":450.50,"currency":"USD"}`)

	// Прогреваем кэш первым запросом (MISS -> сохранение в кэш)
	warmReq := httptest.NewRequest(http.MethodPost, "/v1/charge", bytes.NewReader(reqBody))
	warmReq.Header.Set("Idempotency-Key", "bench-hit-key")
	router.ServeHTTP(httptest.NewRecorder(), warmReq)

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			req := httptest.NewRequest(http.MethodPost, "/v1/charge", bytes.NewReader(reqBody))
			req.Header.Set("Idempotency-Key", "bench-hit-key")
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
		}
	})
}

func BenchmarkMiddleware_CacheMiss(b *testing.B) {
	store := memory.New(memory.WithCleanupInterval(0))
	defer store.Close()

	mw := idempotency.NewMiddleware(store, store, nil)
	router := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"success"}`))
	}))

	reqBody := []byte(`{"order_id":100052,"amount":450.50,"currency":"USD"}`)
	var seq atomic.Uint64

	b.ReportAllocs()
	for b.Loop() {
		key := "bench-miss-" + strconv.FormatUint(seq.Add(1), 10)
		req := httptest.NewRequest(http.MethodPost, "/v1/charge", bytes.NewReader(reqBody))
		req.Header.Set("Idempotency-Key", key)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
	}
}
