package idempotency_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
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
		DialTimeout:  100 * time.Millisecond,
		ReadTimeout:  100 * time.Millisecond,
		WriteTimeout: 100 * time.Millisecond,
		MaxRetries:   0,
	})
	defer rdb.Close()

	repo := idempotency.NewRedisRepository(rdb)
	middleware := idempotency.NewMiddleware(repo, logAdapter, idempotency.WithCoalesceTimeout(400*time.Millisecond))

	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	router := middleware(nextHandler)

	t.Run("Внедрение хаоса - Сетевой таймаут вызывает размыкание Circuit Breaker", func(t *testing.T) {
		_, err := proxy.AddToxic("latency_downstream", "latency", "downstream", 1.0, toxiClient.Attributes{
			"latency": 1500,
		})
		assert.NoError(t, err)

		for i := 0; i < 6; i++ {
			req := httptest.NewRequest(http.MethodPost, "/api/v1/payments", bytes.NewBuffer([]byte(`{}`)))
			req.Header.Set("Idempotency-Key", "chaos-test-key-id")
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			if i >= 5 {
				assert.Equal(t, idempotency.StatusBypassFailOpen.String(), rec.Header().Get(idempotency.HeaderIdempotencyStat))
				assert.Equal(t, http.StatusOK, rec.Code)
			}
		}

		_ = proxy.RemoveToxic("latency_downstream")
	})
}
