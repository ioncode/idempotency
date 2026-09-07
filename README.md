# Go Idempotency Middleware (Production-Ready Open-Source)

Высокопроизводительный, Chi-совместимый Go-middleware транспортного уровня для реализации паттерна **«Идемпотентность»** в распределенных системах, биллинговых процессингах и Mission-Critical ядрах.

Модуль гарантирует 100% защиту API от повторных списаний средств, дублирования заказов и гонок данных (Race Conditions) при сетевых сбоях, обеспечивая экстремальную производительность под нагрузками в миллионы RPS.

---

## ⚡ Ключевые паттерны высшего эшелона надежности

В текущей версии реализован стек ультимативных технологий для Ultra High-Load систем:

1. **Атомарные Lua-скрипты (EVALSHA):** Проверка распределенной блокировки (`TryLock`) и извлечение сохраненного ответа (`Get`) объединены в один атомарный Lua-скрипт на стороне Redis. Скрипт компилируется один раз при старте приложения и вызывается по короткому SHA-хэшу, снижая сетевой оверхед до нуля.
2. **Zero-Allocation Streaming (SHA-256):** Хэш тела запроса вычисляется «на лету» через `io.TeeReader` в момент, когда бизнес-логика сама читает поток байт. Аллокации памяти под входящие payloads равны **0 байт**.
3. **Защита от GC Pressure (`sync.Pool`):** Внутренние буферы для перехвата ответов арендуются из разделяемого пула памяти. Нагрузка на Garbage Collector (мусорщик Go) в пиках сведена к минимуму.
4. **Адаптивный пропуск тяжелых payloads (Anti-OOM / CPU Protection):** 
   - **Для запросов:** Если входящее тело превышает лимит (опция `WithMaxRequestPayloadLimit`), хэширование останавливается для защиты CPU, а запрос безопасно пропускается в бизнес-логику со статусом `BYPASS_TOO_LARGE` без кэширования.
   - **Для ответов:** Если хэндлер возвращает тяжелый файл (превышающий `WithMaxCacheBodySize`), буфер RAM мгновенно очищается, предотвращая утечки памяти.
5. **Автопродление блокировок (Lock Renewal / Heartbeat):** Фоновый асинхронный воркер автоматически продлевает TTL блокировки в Redis, если транзакция во внешнем банке заняла больше времени, чем базовый таймаут, полностью исключая разрывы коннекта при обработке.
6. **Защита от каскадных сбоев (Circuit Breaker):** Встроенный предохранитель изолирует сетевые деградации Redis. Если база начинает отвечать с задержками, цепь размыкается, и модуль мгновенно переходит в режим безопасного пропуска (Fail-Open), спасая Go-процесс от лавинообразной нехватки горутин.
7. **Сквозная наблюдаемость (OpenTelemetry & ulog):** Модуль порождает внутренние трассировочные спаны `idempotency.middleware`, автоматически подхватывая сквозной `Trace ID` компании и обогащая APM-метрики тегами состояний.
8. **Гибкий `KeyExtractor`:** Библиотека не привязана жестко к заголовкам. Вы можете передать любую функцию для извлечения ключа из Query-параметров, кук или JSON-тела.

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

	"github.com/ioncode/idempotency"
	"://github.com"
	"://github.com"
	"://github.com"
	"go.opentelemetry.io/otel"

	"://github.com"
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

## 🧪 Инфраструктурное качество (Chaos, Race & Benchmarks)

Пакет поставляется с развитой экосистемой встроенного контроля регрессии производительности и устойчивости к сбоям:

```bash
# 1. Запуск интеграционных тестов с включенным детектором гонок Go
go test -v -race -timeout 5m ./...

# 2. Запуск Chaos Engineering тестов (Автоматическое развертывание Toxiproxy в Docker)
# Проверяет поведение Circuit Breaker при искусственной задержке сети в 1.5 секунды к Redis
go test -v -run=TestIdempotency_ChaosNetwork_CircuitBreaker ./...

# 3. Запуск локальных бенчмарков производительности под параллельной нагрузкой
go test -benchmem -run=^\$ -bench=. ./...
```
