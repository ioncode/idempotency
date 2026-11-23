# Go Idempotency Middleware

Высокопроизводительный Go-middleware (`net/http` и `chi`) транспортного уровня для реализации паттерна **«Идемпотентность»** в распределенных системах, платежных шлюзах и биллинговых сервисах. Спроектирован с опорой на спецификацию [**IETF `draft-ietf-httpapi-idempotency-key-header`**](https://ietf.org).

Модуль защищает мутирующие HTTP-эндпоинты (`POST`, `PUT`, `PATCH`) от повторных списаний, дублирования заказов и гонок данных (Race Conditions) при сетевых сбоях и конкурентных ретраях, сохраняя минимальный оверхед по CPU и памяти.

---

## ⚡ Ключевые архитектурные решения

1. **Разделение интерфейсов и хранилищ (`Locker`/`Saver`):** ISP-контракты с адаптерами `store/redis` (standalone, Sentinel, Cluster) и потокобезопасным `store/memory` для тестов.
2. **Атомарные Lua-скрипты и Fencing Tokens:** Операции в Redis выполняются через `EVALSHA` за один round-trip с защитой `ownerToken` (`crypto/rand.Text()`).
3. **Потоковый SHA-256 без буферизации:** Хэширование «на лету» без загрузки тела в RAM. При дубле — возврат **`422 Unprocessable Entity`** (`WithConflictStatusCode`).
4. **Гибридный перехватчик ответа (`sync.Pool`):** Буферизация ответа в памяти с защитой от OOM и сбросом в сокет при превышении лимита или использовании `http.Flusher` (SSE).
5. **Защита CPU:** При превышении `WithMaxRequestPayloadLimit` расчет SHA-256 прерывается (`BYPASS_TOO_LARGE`).
6. **Автопродление и безопасный `Unlock`:** Фоновое продление TTL и снятие лока через `context.WithoutCancel` при отмене клиентского контекста.
7. **Circuit Breaker:** Трехступенчатый предохранитель (`CLOSED` → `OPEN` → `HALF_OPEN`) со стратегиями `FailOpen` (`BYPASS_FAIL_OPEN`) или **`503 Service Unavailable`** (`FailClosed`).
8. **Наблюдаемость:** Интеграция с OpenTelemetry (спаны `idempotency.middleware` с Trace ID), Prometheus и `ulog/v3`.
9. **`KeyExtractor` и IETF:** Очистка обрамляющих кавычек (RFC 8941), заголовок `Original-Request-Date` при `HIT` и составные ключи.

---

## 🚀 Быстрый старт

### 1. Установка пакета
```bash
go get github.com/ioncode/idempotency
```

### 2. Интеграция в Chi Router с поддержкой OpenTelemetry и zerolog
```go
package main

import (
	"net/http"
	"os"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/ioncode/idempotency"
	"github.com/ioncode/ulog/v3"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel"
	redisstore "github.com/ioncode/idempotency/store/redis"
)

func main() {
	// 1. Инициализация корпоративного логера ulog
	baseLog := zerolog.New(os.Stdout).With().Timestamp().Logger()
	logger := uzerolog.NewZerologAdapter(baseLog)

	// 2. Инициализация инфраструктуры Redis
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()
	store := redisstore.New(rdb, redisstore.WithKeyPrefix("billing"))

	// 3. Настройка кастомного извлечения ключа (например, из Query-параметров)
	queryExtractor := func(r *http.Request) string {
		return r.URL.Query().Get("idemp_key")
	}

	// 4. Создание высокопроизводительного middleware
	idempMiddleware := idempotency.NewMiddleware(
		store,
		store,
		logger,
		idempotency.WithKeyExtractor(queryExtractor),                           // Наш экстрактор
		idempotency.WithResponseStatusHeader("X-Billing-Idempotency-Status"),    // Кастомный заголовок статуса
		idempotency.WithLockTTL(5 * time.Second),                               // TTL блокировки в Redis
		idempotency.WithMaxCacheBodySize(2 * 1024 * 1024),                      // Лимит ответа 2 МБ (Anti-OOM)
		idempotency.WithMaxRequestPayloadLimit(10 * 1024 * 1024),               // Лимит запроса 10 МБ (Anti-CPU-Attack)
		idempotency.WithTracer(otel.Tracer("billing-idempotency")),             // Интеграция OpenTelemetry Tracing
	)

	r := chi.NewRouter()
	r.Use(idempMiddleware)
	
	r.Post("/charge", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"status":"success"}`))
	})

	http.ListenAndServe(":8080", r)
}
```

---

## 📈 HTTP Статусы Идемпотентности (`X-Idempotency-Status`)

Каждый мутирующий запрос (`POST`, `PUT`, `PATCH`) помечается диагностическим заголовком ответа (имя которого можно переопределить через `WithResponseStatusHeader` или отключить, передав пустую строку):

| Статус | Описание | Действие модуля |
| :--- | :--- | :--- |
| **`MISS`** | Первый уникальный запрос. | Пропускает в бизнес-логику, удерживает блокировку (`ownerToken`), сохраняет результат в кэш. |
| **`HIT`** | Повторный вызов с тем же ключом и телом запроса. | Мгновенно воспроизводит исходный HTTP-статус, тело, заголовки и добавляет `Original-Request-Date`. |
| **`CONFLICT`** | Ключ совпал, но тело запроса (SHA-256) изменилось. | **`422 Unprocessable Entity`** (настраивается через `WithConflictStatusCode`). Операция отклоняется во избежание порчи данных. |
| **`PROCESSING`** | Предыдущий запрос с тем же ключом еще выполняется, истек `WithCoalesceTimeout`. | **`425 Too Early`** с заголовком `Retry-After`. Предлагает клиенту повторить попытку позже. |
| **`BYPASS_FAIL_OPEN`** | Хранилище недоступно, активна стратегия `FailOpen` (по умолчанию). | Circuit Breaker пропускает запрос напрямую в бизнес-логику без дедупликации. |
| **`FAIL_CLOSED`** | Хранилище недоступно, активна стратегия `WithFailureStrategy(FailClosed)`. | **`503 Service Unavailable`**. Запрос отклоняется без вызова бизнес-логики для защиты от дублирования. |
| **`BYPASS_TOO_LARGE`** | Размер входящего запроса или исходящего ответа превысил лимиты. | Кэширование отменяется для защиты RAM/CPU, но бизнес-логика выполняется штатно. |

---

## 🚨 Правила алертинга (Prometheus Alerting Rules)

Разверните следующие правила алертинга в вашем Prometheus / VictoriaMetrics кластере для мониторинга стабильности слоя дедупликации:

```yaml
groups:
  - name: idempotency_infra_alerts
    rules:
      # 1. ALERT: Авария хранилища идемпотентности (Размыкание Circuit Breaker)
      - alert: IdempotencyCircuitBreakerOpen
        expr: sum(rate(idempotency_requests_total{status=~"BYPASS_FAIL_OPEN|FAIL_CLOSED"}[2m])) > 5
        for: 1m
        labels:
          severity: critical
          tier: platform
        annotations:
          summary: "Предохранитель слоя идемпотентности РАЗОМКНУТ (хранилище недоступно)"
          description: "За последние 2 минуты зафиксирован всплеск ошибок хранилища идемпотентности. В режиме Fail-Open запросы идут в бизнес-логику без дедупликации (риск дублей), в режиме Fail-Closed клиенты получают 503 Service Unavailable."

      # 2. ALERT: Высокий уровень конфликтов данных payload (Попытка фрода или баг клиента)
      - alert: IdempotencyHighConflictRate
        expr: sum(increase(idempotency_requests_total{status="CONFLICT"}[5m])) > 20
        for: 3m
        labels:
          severity: warning
          tier: application
        annotations:
          summary: "Обнаружен высокий уровень бизнес-конфликтов payload (422 Unprocessable Entity)"
          description: "Зафиксировано более 20 конфликтов за последние 5 минут. Клиенты отправляют одинаковые Idempotency-Key с разными телами запроса. Возможна ошибка интеграции на стороне клиента или попытка подбора ключей."
```


---

## 📝 OpenAPI / Swagger Аннотации

Чтобы спецификация вашего API автоматически генерировала корректную документацию заголовков и кодов ответов для сторонних интеграторов, используйте следующие Swaggo-декораторы над вашими хэндлерами:

```go
// ChargePaymentHandler обрабатывает списание денежных средств
// @Summary      Атомарное списание средств
// @Param        Idempotency-Key  header    string  true  "Уникальный ключ операции (UUID) для дедупликации"
// @Success      201              {object}  ResponseStructure
// @Header       201              {string}  X-Idempotency-Status  "Результат обработки слоя дедупликации (MISS, HIT, BYPASS_FAIL_OPEN, BYPASS_TOO_LARGE)"
// @Header       201              {string}  Original-Request-Date "Дата и время первоначальной обработки транзакции в формате RFC 7231 (только при HIT)"
// @Failure      400              {string}  string                "Отсутствует обязательный заголовок Idempotency-Key"
// @Failure      422              {string}  string                "Конфликт payload: повторный запрос с тем же ключом, но другим телом (CONFLICT)"
// @Failure      425              {string}  string                "Предыдущий запрос с тем же ключом еще выполняется (PROCESSING)"
// @Header       425              {string}  Retry-After           "Рекомендуемое время ожидания в секундах перед повторным запросом"
// @Failure      503              {string}  string                "Хранилище идемпотентности недоступно при активной стратегии FailClosed (FAIL_CLOSED)"
// @Router       /api/v3/charge [post]
```


---

## 🧪 Тестирование и контроль качества (Unit, Chaos, Mutation & Benchmarks)

Пакет включает многоуровневую систему проверки надежности, устойчивости к сетевым сбоям и контроля регрессии производительности:

```bash
# 1. Запуск быстрых модульных тестов с детектором гонок (без внешних зависимостей)
go test -v -race ./...

# 2. Запуск Chaos Engineering тестов (автоматическое развертывание Redis и Toxiproxy в Docker)
# Проверяет полный цикл Circuit Breaker (CLOSED -> OPEN -> HALF_OPEN -> CLOSED) при задержке сети 1.5с
go test -v -race -tags=chaos -timeout 5m ./...

# 3. Запуск мутационного тестирования (проверка качества тестового покрытия)
go install github.com/avito-tech/go-mutesting/cmd/go-mutesting@latest
go-mutesting github.com/ioncode/idempotency

# 4. Запуск бенчмарков производительности с профилированием аллокаций памяти
go test -benchmem -run=^\$ -bench=. ./...
```
