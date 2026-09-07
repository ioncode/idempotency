package idempotency_test

import (
	"net/http"
	"os"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/ioncode/idempotency"
	"github.com/ioncode/ulog"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel"
)

// ExampleNewMiddleware демонстрирует комплексный сценарий инициализации публичного модуля
// в High-Load окружении с поддержкой кастомных экстракторов, OpenTelemetry и лимитов памяти.
func ExampleNewMiddleware() {
	// 1. Инициализация базовой инфраструктуры логирования (ulog-совместимой)
	baseLog := zerolog.New(os.Stdout).With().Timestamp().Logger()
	logAdapter := ulog.NewZerologAdapter(baseLog)

	// 2. Инициализация клиента распределенной базы данных Redis
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	repo := idempotency.NewRedisRepository(rdb)

	// 3. Создание кастомного экстрактора ключа.
	// В данном примере мы извлекаем ключ идемпотентности из URL Query параметров (?idemp_key=xxx),
	// вместо классического чтения из заголовков HTTP.
	customExtractor := func(r *http.Request) string {
		return r.URL.Query().Get("idemp_key")
	}

	// 4. Инициализация глобального провайдера трассировки OpenTelemetry
	tracer := otel.Tracer("payment-gateway-service")

	// 5. Конфигурация и сборка middleware с использованием Functional Options
	idempMiddleware := idempotency.NewMiddleware(
		repo,
		logAdapter,
		idempotency.WithKeyExtractor(customExtractor),                        // Кастомный поиск ключа
		idempotency.WithResponseStatusHeader("X-Billing-Idempotency-Status"), // Кастомный диагностический заголовок
		idempotency.WithLockTTL(5*time.Second),                               // TTL атомарной Lua-блокировки
		idempotency.WithResponseTTL(12*time.Hour),                            // Время удержания кэша успешного ответа
		idempotency.WithMaxCacheBodySize(1*1024*1024),                        // Макс. размер ответа 1 МБ (Защита от OOM)
		idempotency.WithMaxRequestPayloadLimit(5*1024*1024),                  // Мягкий лимит входящего тела 5 МБ (Защита CPU)
		idempotency.WithTracer(tracer),                                       // Интеграция APM распределенных трейсов
	)

	// 6. Подключение слоя дедупликации к Chi-роутеру микросервиса
	r := chi.NewRouter()

	r.Route("/api/v3", func(api chi.Router) {
		api.Use(idempMiddleware)

		api.Post("/charge", func(w http.ResponseWriter, r *http.Request) {
			// Логика обработки платежа...
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"status":"success","billing_id":"bill_777"}`))
		})
	})

	// Сервер готов к запуску:
	// _ = http.ListenAndServe(":8080", r)
}
