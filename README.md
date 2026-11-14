# Go Idempotency Middleware

Высокопроизводительный Go-middleware (`net/http` и `chi`) транспортного уровня для реализации паттерна **«Идемпотентность»** в распределенных системах, платежных шлюзах и биллинговых сервисах. Спроектирован с опорой на спецификацию [**IETF `draft-ietf-httpapi-idempotency-key-header`**](https://ietf.org).

Модуль защищает мутирующие HTTP-эндпоинты (`POST`, `PUT`, `PATCH`) от повторных списаний, дублирования заказов и гонок данных (Race Conditions) при сетевых сбоях и конкурентных ретраях, сохраняя минимальный оверхед по CPU и памяти.

---

## ⚡ Ключевые архитектурные решения

1. **Атомарные Lua-скрипты (`EVALSHA`):** Проверка распределенной блокировки (`TryLock`) и извлечение сохраненного ответа (`Get`) объединены в один атомарный Lua-скрипт на стороне Redis. Скрипт загружается и вызывается по SHA-хэшу за один сетевой round-trip.
2. **Потоковый SHA-256 без буферизации тела запроса (`io.TeeReader`):** Хэш входящего payload вычисляется «на лету» по мере чтения потока бизнес-логикой, не загружая всё тело запроса в оперативную память, а при повторных запросах (`HIT`) проверяется на совпадение с возвратом `409 Conflict`.
3. **Гибридный перехватчик ответа (`ProductionResponseWriter` + `sync.Pool`):** Внутренние буферы арендуются из пула `sync.Pool` для снижения нагрузки на GC. Пока ответ укладывается в `WithMaxCacheBodySize`, он накапливается в буфере; при превышении лимита или вызове `http.Flusher` (SSE) буфер мгновенно очищается, а данные передаются клиенту напрямую в сокет (Anti-OOM).
4. **Защита CPU при тяжелых входящих запросах:** Если размер входящего тела превышает `WithMaxRequestPayloadLimit`, хэширование останавливается для защиты CPU, а запрос пропускается в бизнес-логику со статусом `BYPASS_TOO_LARGE` без кэширования.
5. **Автопродление блокировок (Lock Renewal / Heartbeat):** Фоновая горутина периодически продлевает TTL блокировки в Redis, если обработка транзакции занимает больше базового таймаута, защищая от преждевременного снятия лока при долгих операциях.
6. **Трехступенчатый Circuit Breaker (`CLOSED` → `OPEN` → `HALF_OPEN`):** Встроенный предохранитель изолирует сетевые сбои и таймауты Redis. При достижении порога ошибок (`WithCircuitBreakerThreshold`) цепь размыкается, и модуль переходит в режим `Fail-Open`, а по истечении `WithCircuitBreakerCooldown` пропускает ровно один пробный запрос (`Double-Checked Locking`) для автовосстановления.
7. **Сквозная наблюдаемость (OpenTelemetry, Prometheus и `ulog`):** Модуль создает спаны `idempotency.middleware` с привязкой к `Trace ID` и экспортирует счетчики запросов и ошибок хранилища для Prometheus.
8. **Гибкий `KeyExtractor` и составные ключи:** Поддерживается очистка кавычек по спецификации IETF, а также передача кастомной функции извлечения ключа (например, для формирования составного ключа `user_id + path + idempotency_key` в соответствии с разделом *Security Considerations* стандарта IETF).

---

## 🚀 Быстрый старт

### 1. Установка пакета
```bash
go get github.com/ioncode/idempotency
```

### 2. Интеграция в Chi Router с поддержкой OpenTelemetry и slog
```go
package main

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

func main() {
	// 1. Инициализация корпоративного логера ulog
	baseLog := zerolog.New(os.Stdout).With().Timestamp().Logger()
	logAdapter := ulog.NewZerologAdapter(baseLog)

	// 2. Инициализация инфраструктуры Redis
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	repo := idempotency.NewRedisRepository(rdb)

	// 3. Настройка кастомного извлечения ключа (например, из Query-параметров)
	queryExtractor := func(r *http.Request) string {
		return r.URL.Query().Get("idemp_key")
	}

	// 4. Создание высокопроизводительного middleware
	idempMiddleware := idempotency.NewMiddleware(
		repo,
		logAdapter,
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

Каждый мутирующий запрос (`POST`, `PUT`, `PATCH`) помечается диагностическим заголовком ответа (имя которого можно переопределить или отключить):

| Статус | Описание | Действие модуля |
| :--- | :--- | :--- |
| **`MISS`** | Первый уникальный запрос. | Пропускает в бизнес-логику, блокирует конкурентов, сохраняет результат. |
| **`HIT`** | Повторный вызов с тем же ключом. | Воспроизводит исходный HTTP-статус, тело и заголовки из кэша. |
| **`CONFLICT`** | Ключ совпал, но тело запроса изменилось. | **409 Conflict**. Операция отклоняется во избежание порчи данных. |
| **`PROCESSING`** | Предыдущий запрос еще выполняется. | **425 Too Early**. Предлагает клиенту повторить попытку позже. |
| **`BYPASS_FAIL_OPEN`** | Кластер Redis деградировал или недоступен. | Стратегия Fail-Open: Circuit Breaker пускает запрос мимо Redis. |
| **`BYPASS_TOO_LARGE`** | Размер запроса или ответа превысил лимиты. | Кэширование отменяется для защиты RAM/CPU, но API продолжает работу. |

---

## 🚨 Правила алертинга (Prometheus Alerting Rules)

Разверните следующие правила алертинга в вашем Prometheus / VictoriaMetrics кластере для мониторинга стабильности слоя дедупликации:

```yaml
groups:
  - name: idempotency_infra_alerts
    rules:
      # 1. ALERT: Авария распределенного хранилища (Размыкание Circuit Breaker)
      - alert: IdempotencyCircuitBreakerOpen
        expr: sum(rate(idempotency_requests_total{status="BYPASS_FAIL_OPEN"}[2m])) > 5
        for: 1m
        labels:
          severity: critical
          tier: platform
        annotations:
          summary: "Предохранитель слоя идемпотентности РАЗОМКНУТ (Redis недоступен)"
          description: "За последние 2 минуты зафиксировано лавинообразное количество ошибок взаимодействия с Redis. Слой идемпотентности аварийно отключен (Fail-Open), транзакции идут напрямую в базу без проверки дубликатов. Риск двойных списаний денежных средств!"

      # 2. ALERT: Высокий уровень конфликтов данных payload (Попытка фрода или баг фронтенда)
      - alert: IdempotencyHighConflictRate
        expr: sum(rate(idempotency_requests_total{status="CONFLICT"}[5m])) > 20
        for: 3m
        labels:
          severity: warning
          tier: application
        annotations:
          summary: "Обнаружен высокий уровень бизнес-конфликтов payload"
          description: "Зафиксировано более 20 конфликтов за последние 5 минут. Клиенты отправляют одинаковые Idempotency-Key с разными телами запроса. Возможна интеграционная ошибка на стороне фронтенда или попытка мошеннического подбора ключей."
```

---

## 📝 OpenAPI / Swagger Аннотации

Чтобы спецификация вашего API автоматически генерировала корректную документацию заголовков для сторонних интеграторов, используйте следующие Swaggo-декораторы над вашими хэндлерами:

```go
// ChargePaymentHandler обрабатывает списание денежных средств
// @Summary      Атомарное списание средств
// @Param        Idempotency-Key  header    string  true  "Уникальный UUID операции для дедупликации"
// @Success      201              {object}  ResponseStructure
// @Header       201              {string}  X-Idempotency-Status  "Результат обработки слоя дедупликации (HIT, MISS, CONFLICT)"
// @Header       201              {string}  Original-Request-Date "Дата первоначальной обработки транзакции (только для статуса HIT)"
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
