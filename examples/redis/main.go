package main

import (
	"log"
	"net/http"
	"os"
	"time"

	redisstore "github.com/ioncode/idempotency/store/redis"
	"github.com/ioncode/ulog/v3/adapters/uzerolog"

	"github.com/go-chi/chi/v5"
	"github.com/ioncode/idempotency"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"

	"go.opentelemetry.io/otel"
)

func main() {
	baseLog := zerolog.New(os.Stdout).With().Timestamp().Logger()
	logger := uzerolog.NewZerologAdapter(baseLog)

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()

	store := redisstore.New(rdb, redisstore.WithKeyPrefix("billing"))

	// В данном примере мы извлекаем ключ идемпотентности из URL Query параметров (?idemp_key=xxx),
	// вместо классического чтения из заголовков HTTP.
	customExtractor := func(r *http.Request) string {
		return r.URL.Query().Get("idemp_key")
	}

	idempMiddleware := idempotency.NewMiddleware(
		store,
		store,
		logger,
		idempotency.WithFailureStrategy(idempotency.FailClosed),
		idempotency.WithTracer(otel.Tracer("payment-gateway-service")),
		idempotency.WithKeyExtractor(customExtractor),                        // Кастомный поиск ключа
		idempotency.WithResponseStatusHeader("X-Billing-Idempotency-Status"), // Кастомный диагностический заголовок
		idempotency.WithLockTTL(5*time.Second),                               // TTL атомарной Lua-блокировки
		idempotency.WithResponseTTL(12*time.Hour),                            // Время удержания кэша успешного ответа
		idempotency.WithMaxCacheBodySize(1*1024*1024),                        // Макс. размер ответа 1 МБ (Защита от OOM)
		idempotency.WithMaxRequestPayloadLimit(5*1024*1024),                  // Мягкий лимит входящего тела 5 МБ (Защита CPU)
	)

	r := chi.NewRouter()
	r.Use(idempMiddleware)
	r.Post("/charge", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"status":"success"}`))
	})

	log.Println("Production server listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", r))
}
