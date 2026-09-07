package idempotency_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ioncode/idempotency"
	"github.com/ioncode/ulog"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"

	toxiClient "github.com/Shopify/toxiproxy/v2/client"
	redisClient "github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

// TestIdempotency_ChaosNetwork_CircuitBreaker тестирует поведение модуля в условиях
// искусственной сетевой деградации (Chaos Engineering) с помощью Toxiproxy.
func TestIdempotency_ChaosNetwork_CircuitBreaker(t *testing.T) {
	ctx := context.Background()
	baseLog := zerolog.New(io.Discard)
	//use console logger for debug
	//baseLog := zerolog.New(os.Stdout).With().Timestamp().Logger()
	logAdapter := ulog.NewZerologAdapter(baseLog)

	net, err := network.New(ctx, network.WithDriver("bridge"))
	if err != nil {
		t.Fatalf("failed to create docker network using network.New: %s", err)
	}
	defer func() { _ = net.Remove(ctx) }()
	netName := net.Name

	// Запуск контейнера Redis внутри созданной сети
	redisReq := testcontainers.ContainerRequest{
		Image:        "redis:7-alpine",
		ExposedPorts: []string{"6379/tcp"},
		Networks:     []string{netName},
		NetworkAliases: map[string][]string{
			netName: {"redis-service"},
		},
		WaitingFor: wait.ForLog("Ready to accept connections"),
	}
	redisC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: redisReq,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("failed to start redis container: %s", err)
	}
	defer func() { _ = redisC.Terminate(ctx) }()

	redisContainerName, err := redisC.Name(ctx)
	if err != nil {
		t.Fatalf("failed to get redis container name: %s", err)
	}
	redisContainerName = strings.TrimPrefix(redisContainerName, "/")
	log.Println("Created redis container with name", redisContainerName)

	// Запуск контейнера Toxiproxy в той же сети
	toxiReq := testcontainers.ContainerRequest{
		Image:        "ghcr.io/shopify/toxiproxy:2.5.0",
		ExposedPorts: []string{"8474/tcp", "26379/tcp"},
		Networks:     []string{netName},
		WaitingFor:   wait.ForHTTP("/version").WithPort("8474/tcp"),
	}
	toxiC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: toxiReq,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("failed to start toxiproxy container: %s", err)
	}
	defer func() { _ = toxiC.Terminate(ctx) }()

	toxiHost, _ := toxiC.Host(ctx)
	toxiAPIPort, _ := toxiC.MappedPort(ctx, "8474")
	toxiRedisPort, _ := toxiC.MappedPort(ctx, "26379")

	// Настройка клиента Toxiproxy
	cli := toxiClient.NewClient(toxiHost + ":" + toxiAPIPort.Port())

	proxy, err := cli.CreateProxy("redis_proxy", "0.0.0.0:26379", "redis-service:6379")
	if err != nil {
		t.Fatalf("failed to create toxiproxy proxy: %s", err)
	}
	rdb := redisClient.NewClient(&redisClient.Options{
		Addr:         toxiHost + ":" + toxiRedisPort.Port(),
		DialTimeout:  1 * time.Second,
		ReadTimeout:  1 * time.Second,
		WriteTimeout: 1 * time.Second,
		PoolSize:     5,
		MaxRetries:   0,
	})
	defer rdb.Close()

	repo := idempotency.NewRedisRepository(rdb)
	middleware := idempotency.NewMiddleware(repo, logAdapter, idempotency.WithCoalesceTimeout(2*time.Second))

	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	router := middleware(nextHandler)

	t.Run("Внедрение хаоса - Сетевой таймаут вызывает размыкание Circuit Breaker", func(t *testing.T) {
		// ------------------------------------------------------------------
		// ГАРАНТИРОВАННЫЙ ПРОГРЕВ И ИЗОЛЯЦИЯ СЕТИ (Ждем готовность линка)
		// ------------------------------------------------------------------
		// Делаем явный Пинг через драйвер. Это заставит Toxiproxy лениво поднять
		// и стабилизировать upstream-соединение до Redis-контейнера.
		var pingErr error
		for check := 0; check < 10; check++ {
			log.Println("Waiting for Redis", check)
			if pingErr = rdb.Ping(ctx).Err(); pingErr == nil {
				break
			}
			time.Sleep(100 * time.Millisecond) // Даем Docker время поднять сетевые интерфейсы
		}
		if pingErr != nil {
			t.Fatalf("failed to warm up redis connection pool through proxy: %s", pingErr)
		}

		// ------------------------------------------------------------------
		// ШАГ 1: ВЕРИФИКАЦИЯ ЗАКРЫТОГО ПРЕДОХРАНИТЕЛЯ (CLOSED)
		// ------------------------------------------------------------------
		// Теперь сеть гарантированно чистая и прогретая.
		reqCheck := httptest.NewRequest(http.MethodPost, "/api/v1/payments", bytes.NewBuffer([]byte(`{}`)))
		reqCheck.Header.Set("Idempotency-Key", "verified-closed-key")
		recCheck := httptest.NewRecorder()
		router.ServeHTTP(recCheck, reqCheck)

		log.Println(recCheck.Header())
		// Контракт закрытой цепи: первый запрос обязан вернуть честный MISS!
		assert.Equal(t, idempotency.StatusMiss.String(), recCheck.Header().Get(idempotency.HeaderIdempotencyStat))

		// ------------------------------------------------------------------
		// ШАГ 2: ВКЛЮЧАЕМ ХАОС И НАКАПЛИВАЕМ ОШИБКИ
		// ------------------------------------------------------------------
		// Внедряем деградацию: задержка downstream-пакетов на 1500мс
		_, err = proxy.AddToxic("latency_downstream", "latency", "downstream", 1.0, toxiClient.Attributes{
			"latency": 1500,
		})
		assert.NoError(t, err)

		// Делаем серию запросов для гарантированного переполнения счетчика сбоев
		for i := 0; i < 5; i++ {
			req := httptest.NewRequest(http.MethodPost, "/api/v1/payments", bytes.NewBuffer([]byte(`{}`)))
			req.Header.Set("Idempotency-Key", fmt.Sprintf("chaos-trigger-key-%d", i))
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
		}

		// ------------------------------------------------------------------
		// ШАГ 3: ВЕРИФИКАЦИЯ РАЗОМКНУТОГО ПРЕДОХРАНИТЕЛЯ (OPEN)
		// ------------------------------------------------------------------
		// Предохранитель обязан быть в состоянии OPEN. Наш запрос пойдет по Fail-Open
		finalReq := httptest.NewRequest(http.MethodPost, "/api/v1/payments", bytes.NewBuffer([]byte(`{}`)))
		finalReq.Header.Set("Idempotency-Key", "verified-open-key")
		finalRec := httptest.NewRecorder()
		router.ServeHTTP(finalRec, finalReq)

		// Проверяем железобетонный контракт Fail-Open стратегии
		assert.Equal(t, idempotency.StatusBypassFailOpen.String(), finalRec.Header().Get(idempotency.HeaderIdempotencyStat))
		assert.Equal(t, http.StatusOK, finalRec.Code)

		// Восстанавливаем сеть
		_ = proxy.RemoveToxic("latency_downstream")
	})
}
