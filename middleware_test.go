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

// memoryRepo — потокобезопасный in-memory мок Repository для быстрых юнит- и мутационных тестов.
type memoryRepo struct {
	mu          sync.Mutex
	records     map[string]*IdempotentRecord
	locks       map[string]bool
	evalErr     error
	saveErr     error
	extendFalse bool
	unlocked    int32
	extended    int32
}

func newMemoryRepo() *memoryRepo {
	return &memoryRepo{
		records: make(map[string]*IdempotentRecord),
		locks:   make(map[string]bool),
	}
}

func (m *memoryRepo) EvaluateIdempotency(_ context.Context, key string, _ string, _ time.Duration) (string, *IdempotentRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.evalErr != nil {
		return "", nil, m.evalErr
	}
	if rec, ok := m.records[key]; ok {
		return "HIT", rec, nil
	}
	if m.locks[key] {
		return "PROCESSING", nil, nil
	}
	m.locks[key] = true
	return "MISS", nil, nil
}

func (m *memoryRepo) ExtendLock(_ context.Context, _ string, _ string, _ time.Duration) (bool, error) {
	atomic.AddInt32(&m.extended, 1)
	if m.extendFalse {
		return false, nil
	}
	return true, nil
}

func (m *memoryRepo) Unlock(_ context.Context, key string) error {
	m.mu.Lock()
	delete(m.locks, key)
	m.mu.Unlock()
	atomic.AddInt32(&m.unlocked, 1)
	return nil
}

func (m *memoryRepo) Save(_ context.Context, record *IdempotentRecord, _ time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.saveErr != nil {
		return m.saveErr
	}
	m.records[record.Key] = record
	return nil
}

func TestMiddleware_MethodFilteringAndMissingKey(t *testing.T) {
	repo := newMemoryRepo()
	var calls atomic.Int32
	handler := NewMiddleware(repo, nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	repo := newMemoryRepo()
	var calls atomic.Int32

	handler := NewMiddleware(
		repo,
		nil,
		WithResponseStatusHeader("X-Custom-Idemp"),
		WithResponseTTL(time.Hour),
	)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Connection", "keep-alive") // hop-by-hop должен отфильтроваться
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"created":true}`))
	}))

	// 1. Первый запрос -> MISS, сохранение в кэш и снятие блокировки
	req1 := httptest.NewRequest(http.MethodPost, "/orders", bytes.NewBufferString(`{"amount":100}`))
	req1.Header.Set("Idempotency-Key", `"key-1"`) // проверка очистки кавычек IETF
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	assert.Equal(t, http.StatusCreated, rec1.Code)
	assert.Equal(t, StatusMiss.String(), rec1.Header().Get("X-Custom-Idemp"))
	assert.Equal(t, `{"created":true}`, rec1.Body.String())
	assert.Equal(t, int32(1), calls.Load())
	assert.Equal(t, int32(1), atomic.LoadInt32(&repo.unlocked))

	// Проверяем, что hop-by-hop заголовок Connection не попал в кэш
	require.Contains(t, repo.records, "key-1")
	assert.NotContains(t, repo.records["key-1"].ResponseHeaders, "Connection")

	// 2. Повторный запрос с тем же ключом и телом -> HIT без вызова хендлера
	req2 := httptest.NewRequest(http.MethodPost, "/orders", bytes.NewBufferString(`{"amount":100}`))
	req2.Header.Set("Idempotency-Key", "key-1")
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	assert.Equal(t, http.StatusCreated, rec2.Code)
	assert.Equal(t, StatusHit.String(), rec2.Header().Get("X-Custom-Idemp"))
	assert.Equal(t, "application/json", rec2.Header().Get("Content-Type"))
	assert.Equal(t, `{"created":true}`, rec2.Body.String())
	assert.Equal(t, int32(1), calls.Load())

	// 3. Повторный запрос с тем же ключом, но другим телом -> 409 Conflict
	req3 := httptest.NewRequest(http.MethodPost, "/orders", bytes.NewBufferString(`{"amount":999}`))
	req3.Header.Set("Idempotency-Key", "key-1")
	rec3 := httptest.NewRecorder()
	handler.ServeHTTP(rec3, req3)

	assert.Equal(t, http.StatusConflict, rec3.Code)
	assert.Equal(t, StatusConflict.String(), rec3.Header().Get("X-Custom-Idemp"))
	assert.Equal(t, int32(1), calls.Load())
}

func TestMiddleware_ProcessingTimeoutAndDeadline(t *testing.T) {
	repo := newMemoryRepo()
	repo.locks["busy-key"] = true // имитируем активную блокировку другой горутиной

	handler := NewMiddleware(
		repo,
		nil,
		WithCoalesceTimeout(60*time.Millisecond),
		WithRetryAfter(3*time.Second),
	)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// 1. Истечение CoalesceTimeout -> 425 Too Early + Retry-After
	req := httptest.NewRequest(http.MethodPatch, "/orders/1", bytes.NewBufferString(`{}`))
	req.Header.Set("Idempotency-Key", "busy-key")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusTooEarly, rec.Code)
	assert.Equal(t, StatusProcessing.String(), rec.Header().Get(HeaderIdempotencyStat))
	assert.Equal(t, "3", rec.Header().Get("Retry-After"))

	// 2. Слишком короткий Deadline контекста (< 50ms) -> 504 Gateway Timeout до попытки блокировки
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	reqDeadline := httptest.NewRequest(http.MethodPut, "/orders/1", bytes.NewBufferString(`{}`)).WithContext(ctx)
	reqDeadline.Header.Set("Idempotency-Key", "deadline-key")
	recDeadline := httptest.NewRecorder()
	handler.ServeHTTP(recDeadline, reqDeadline)

	assert.Equal(t, http.StatusGatewayTimeout, recDeadline.Code)
}

func TestMiddleware_BypassTooLargeAnd5xxNotCached(t *testing.T) {
	repo := newMemoryRepo()

	handler := NewMiddleware(
		repo,
		nil,
		WithMaxRequestPayloadLimit(5),
		WithMaxCacheBodySize(10),
	)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		if r.Header.Get("X-Fail") == "1" {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("internal error"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))

	// 1. Входящий payload > 5 байт -> BYPASS_TOO_LARGE, ответ не кэшируется
	reqLarge := httptest.NewRequest(http.MethodPost, "/upload", bytes.NewBufferString("123456789"))
	reqLarge.Header.Set("Idempotency-Key", "large-req-key")
	recLarge := httptest.NewRecorder()
	handler.ServeHTTP(recLarge, reqLarge)

	assert.Equal(t, http.StatusOK, recLarge.Code)
	assert.Equal(t, StatusBypassTooLarge.String(), recLarge.Header().Get(HeaderIdempotencyStat))
	assert.NotContains(t, repo.records, "large-req-key")

	// 2. Ошибка сервера 500 -> ответ не сохраняется в кэш
	req500 := httptest.NewRequest(http.MethodPost, "/upload", bytes.NewBufferString("ok"))
	req500.Header.Set("Idempotency-Key", "err-500-key")
	req500.Header.Set("X-Fail", "1")
	rec500 := httptest.NewRecorder()
	handler.ServeHTTP(rec500, req500)

	assert.Equal(t, http.StatusInternalServerError, rec500.Code)
	assert.NotContains(t, repo.records, "err-500-key")
}

func TestMiddleware_LockHeartbeatAndFailOpen(t *testing.T) {
	repo := newMemoryRepo()

	// Проверяем фоновое продление лока (LockTTL = 60ms -> тикер сработает каждые 20ms)
	handler := NewMiddleware(
		repo,
		nil,
		WithLockTTL(60*time.Millisecond),
		WithCircuitBreakerThreshold(2),
	)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		time.Sleep(55 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodPost, "/slow", bytes.NewBufferString(`{}`))
	req.Header.Set("Idempotency-Key", "slow-key")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.GreaterOrEqual(t, atomic.LoadInt32(&repo.extended), int32(1), "фоновый воркер должен был продлить блокировку")

	// Имитируем ошибку Redis -> Fail-Open пропускает запрос со статусом BYPASS_FAIL_OPEN
	repo.evalErr = errors.New("redis unreachable")
	reqFailOpen := httptest.NewRequest(http.MethodPost, "/slow", bytes.NewBufferString(`{}`))
	reqFailOpen.Header.Set("Idempotency-Key", "fail-open-key")
	recFailOpen := httptest.NewRecorder()
	handler.ServeHTTP(recFailOpen, reqFailOpen)

	assert.Equal(t, http.StatusOK, recFailOpen.Code)
	assert.Equal(t, StatusBypassFailOpen.String(), recFailOpen.Header().Get(HeaderIdempotencyStat))
}
