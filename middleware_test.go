package idempotency

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockStore struct {
	mu             sync.Mutex
	records        map[string]*IdempotentRecord
	locks          map[string]string
	evalErr        error
	saveErr        error
	extendFalse    bool
	unlocked       atomic.Int32
	extended       atomic.Int32
	lastOwnerToken string
}

func newMockStore() *mockStore {
	return &mockStore{
		records: make(map[string]*IdempotentRecord),
		locks:   make(map[string]string),
	}
}

func (m *mockStore) Evaluate(_ context.Context, key string, ownerToken string, _ time.Duration) (EvaluationStatus, *IdempotentRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.evalErr != nil {
		return "", nil, m.evalErr
	}
	if rec, ok := m.records[key]; ok {
		return EvalHit, rec, nil
	}
	if _, locked := m.locks[key]; locked {
		return EvalLocked, nil, nil
	}

	m.locks[key] = ownerToken
	m.lastOwnerToken = ownerToken
	return EvalMiss, nil, nil
}

func (m *mockStore) ExtendLock(_ context.Context, _ string, _ string, _ time.Duration) (bool, error) {
	m.extended.Add(1)
	if m.extendFalse {
		return false, nil
	}
	return true, nil
}

func (m *mockStore) Unlock(_ context.Context, key string, ownerToken string) error {
	m.mu.Lock()
	if cur, ok := m.locks[key]; ok && cur == ownerToken {
		delete(m.locks, key)
	}
	m.mu.Unlock()

	m.unlocked.Add(1)
	return nil
}

func (m *mockStore) Save(_ context.Context, record *IdempotentRecord, _ time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.saveErr != nil {
		return m.saveErr
	}
	m.records[record.Key] = record
	return nil
}

func TestMiddleware_MethodFilteringAndMissingKey(t *testing.T) {
	store := newMockStore()
	var calls atomic.Int32

	handler := NewMiddleware(store, store, nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))

	// 1. Безопасные методы (GET, DELETE) пропускаются без проверки Idempotency-Key
	for _, method := range []string{http.MethodGet, http.MethodDelete} {
		req := httptest.NewRequest(method, "/items", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
	}
	assert.Equal(t, int32(2), calls.Load())

	// 2. Мутирующий метод (POST) без ключа -> 400 Bad Request
	reqNoKey := httptest.NewRequest(http.MethodPost, "/items", bytes.NewBufferString(`{}`))
	recNoKey := httptest.NewRecorder()
	handler.ServeHTTP(recNoKey, reqNoKey)
	assert.Equal(t, http.StatusBadRequest, recNoKey.Code)
	assert.Equal(t, int32(2), calls.Load(), "бизнес-логика не должна вызываться без ключа")
}

func TestMiddleware_HitMissAndConflict(t *testing.T) {
	store := newMockStore()
	var calls atomic.Int32

	handler := NewMiddleware(
		store,
		store,
		nil,
		WithResponseStatusHeader("X-Custom-Idemp"),
		WithResponseTTL(time.Hour),
	)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Connection", "keep-alive")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("created-ok"))
	}))

	// 1. Первый запрос -> MISS
	req1 := httptest.NewRequest(http.MethodPost, "/orders", bytes.NewBufferString("payload-1"))
	req1.Header.Set("Idempotency-Key", `"key-1"`)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	assert.Equal(t, http.StatusCreated, rec1.Code)
	assert.Equal(t, StatusMiss.String(), rec1.Header().Get("X-Custom-Idemp"))
	assert.Equal(t, "created-ok", rec1.Body.String())
	assert.Equal(t, int32(1), calls.Load())
	assert.Equal(t, int32(1), store.unlocked.Load())
	assert.NotEmpty(t, store.lastOwnerToken)
	require.Contains(t, store.records, "key-1")
	assert.NotContains(t, store.records["key-1"].ResponseHeaders, "Connection")
	// 2. Повторный запрос с тем же ключом и телом -> HIT без вызова хендлера
	req2 := httptest.NewRequest(http.MethodPost, "/orders", bytes.NewBufferString("payload-1"))
	req2.Header.Set("Idempotency-Key", "key-1")
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	assert.Equal(t, http.StatusCreated, rec2.Code)
	assert.Equal(t, StatusHit.String(), rec2.Header().Get("X-Custom-Idemp"))
	assert.Equal(t, "application/json", rec2.Header().Get("Content-Type"))
	assert.NotEmpty(t, rec2.Header().Get(HeaderOriginalDate))
	assert.Equal(t, "created-ok", rec2.Body.String())
	assert.Equal(t, int32(1), calls.Load())

	// 3. Повторный запрос с тем же ключом, но другим телом -> 422 Unprocessable Entity
	req3 := httptest.NewRequest(http.MethodPost, "/orders", bytes.NewBufferString("payload-2"))
	req3.Header.Set("Idempotency-Key", "key-1")
	rec3 := httptest.NewRecorder()
	handler.ServeHTTP(rec3, req3)

	assert.Equal(t, http.StatusUnprocessableEntity, rec3.Code)
	assert.Equal(t, StatusConflict.String(), rec3.Header().Get("X-Custom-Idemp"))
	assert.Equal(t, int32(1), calls.Load())
}

func TestMiddleware_ProcessingTimeoutAndDeadline(t *testing.T) {
	store := newMockStore()
	store.locks["busy-key"] = "another-owner"

	handler := NewMiddleware(
		store,
		store,
		nil,
		WithCoalesceTimeout(60*time.Millisecond),
		WithCoalesceInterval(15*time.Millisecond),
		WithRetryAfter(3*time.Second),
	)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// 1. Истечение CoalesceTimeout -> 425 Too Early + Retry-After
	req := httptest.NewRequest(http.MethodPatch, "/orders/1", bytes.NewBufferString("body"))
	req.Header.Set("Idempotency-Key", "busy-key")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusTooEarly, rec.Code)
	assert.Equal(t, StatusProcessing.String(), rec.Header().Get(HeaderIdempotencyStat))
	assert.Equal(t, "3", rec.Header().Get("Retry-After"))

	// 2. Слишком короткий Deadline контекста (< 50ms) -> 504 Gateway Timeout до попытки блокировки
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	reqDeadline := httptest.NewRequest(http.MethodPut, "/orders/1", bytes.NewBufferString("body")).WithContext(ctx)
	reqDeadline.Header.Set("Idempotency-Key", "deadline-key")
	recDeadline := httptest.NewRecorder()
	handler.ServeHTTP(recDeadline, reqDeadline)

	assert.Equal(t, http.StatusGatewayTimeout, recDeadline.Code)
}

func TestMiddleware_BypassTooLarge(t *testing.T) {
	store := newMockStore()

	handler := NewMiddleware(
		store,
		store,
		nil,
		WithMaxRequestPayloadLimit(5),
		WithMaxCacheBodySize(10),
	)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))

	reqLarge := httptest.NewRequest(http.MethodPost, "/upload", bytes.NewBufferString("123456789"))
	reqLarge.Header.Set("Idempotency-Key", "large-req-key")
	recLarge := httptest.NewRecorder()
	handler.ServeHTTP(recLarge, reqLarge)

	assert.Equal(t, http.StatusOK, recLarge.Code)
	assert.Equal(t, StatusBypassTooLarge.String(), recLarge.Header().Get(HeaderIdempotencyStat))
	assert.NotContains(t, store.records, "large-req-key")
}

func TestMiddleware_ServerErrorsNotCached(t *testing.T) {
	store := newMockStore()

	handler := NewMiddleware(store, store, nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("internal error"))
	}))

	req500 := httptest.NewRequest(http.MethodPost, "/upload", bytes.NewBufferString("ok"))
	req500.Header.Set("Idempotency-Key", "err-500-key")
	rec500 := httptest.NewRecorder()
	handler.ServeHTTP(rec500, req500)

	assert.Equal(t, http.StatusInternalServerError, rec500.Code)
	assert.NotContains(t, store.records, "err-500-key")
}

func TestMiddleware_LockHeartbeatAndFailOpen(t *testing.T) {
	store := newMockStore()

	handler := NewMiddleware(
		store,
		store,
		nil,
		WithLockTTL(60*time.Millisecond),
		WithCircuitBreakerThreshold(2),
	)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		time.Sleep(55 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodPost, "/slow", bytes.NewBufferString("body"))
	req.Header.Set("Idempotency-Key", "slow-key")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.GreaterOrEqual(t, store.extended.Load(), int32(1))

	store.evalErr = errors.New("store unreachable")

	reqFailOpen := httptest.NewRequest(http.MethodPost, "/slow", bytes.NewBufferString("body"))
	reqFailOpen.Header.Set("Idempotency-Key", "fail-open-key")
	recFailOpen := httptest.NewRecorder()
	handler.ServeHTTP(recFailOpen, reqFailOpen)

	assert.Equal(t, http.StatusOK, recFailOpen.Code)
	assert.Equal(t, StatusBypassFailOpen.String(), recFailOpen.Header().Get(HeaderIdempotencyStat))
}

func TestMiddleware_FailClosed(t *testing.T) {
	store := newMockStore()
	store.evalErr = errors.New("store unreachable")

	var calls atomic.Int32
	handler := NewMiddleware(
		store,
		store,
		nil,
		WithFailureStrategy(FailClosed),
		WithCircuitBreakerThreshold(1),
	)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))

	// 1. Первая ошибка хранилища размыкает Circuit Breaker и отклоняет запрос (503 + FAIL_CLOSED)
	req1 := httptest.NewRequest(http.MethodPost, "/payments", bytes.NewBufferString("body"))
	req1.Header.Set("Idempotency-Key", "fc-key-1")
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	assert.Equal(t, http.StatusServiceUnavailable, rec1.Code)
	assert.Equal(t, StatusFailClosed.String(), rec1.Header().Get(HeaderIdempotencyStat))
	assert.Equal(t, int32(0), calls.Load())

	// 2. Повторный запрос при уже разомкнутом Circuit Breaker также отклоняется без вызова бизнес-логики
	req2 := httptest.NewRequest(http.MethodPost, "/payments", bytes.NewBufferString("body"))
	req2.Header.Set("Idempotency-Key", "fc-key-2")
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	assert.Equal(t, http.StatusServiceUnavailable, rec2.Code)
	assert.Equal(t, StatusFailClosed.String(), rec2.Header().Get(HeaderIdempotencyStat))
	assert.Equal(t, int32(0), calls.Load())
}
