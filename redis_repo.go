package idempotency

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// Инкапсулируем Lua-скрипты на уровне пакета через redis.NewScript для поддержки EVALSHA.
// Атомарное выполнение гарантирует защиту отRace Conditions (состояний гонки данных).
var (
	// evaluateIdempotencyScript совмещает в себе фазы атомарной проверки лока и кэша.
	// KEYS[1] - ключ распределенной блокировки (lock)
	// KEYS[2] - ключ хэш-карты с кэшированными данными HTTP-ответа (data)
	// ARGV[1] - SHA-256 хэш (цифровой отпечаток) тела запроса
	// ARGV[2] - время жизни блокировки (TTL) в миллисекундах
	evaluateIdempotencyScript = redis.NewScript(`
		if redis.call("EXISTS", KEYS[1]) == 1 then
			return {"LOCKED", ""}
		end
		
		local data = redis.call("HGETALL", KEYS[2])
		if #data > 0 then
			return {"HIT", "FOUND"}
		end
		
		redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[2])
		return {"MISS", ""}
	`)

	// extendLockScript атомарно продлевает блокировку во время работы долгой бизнес-логики,
	// проверяя, что текущий поток выполнения по-прежнему является владельцем этого замка.
	extendLockScript = redis.NewScript(`
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("pexpire", KEYS[1], ARGV[2])
		else
			return 0
		end
	`)
)

type redisRepository struct {
	rdb *redis.Client
}

// NewRedisRepository создает новую инфраструктурную реализацию репозитория на базе Redis.
func NewRedisRepository(rdb *redis.Client) Repository {
	return &redisRepository{rdb: rdb}
}

// EvaluateIdempotency осуществляет атомарную валидацию контракта идемпотентности за один сетевой запрос.
// Использует go-redis метод StringSlice() для гарантированного безопасного разбора Lua-массивов без рантайм-паник.
func (r *redisRepository) EvaluateIdempotency(ctx context.Context, key string, payloadHash string, ttl time.Duration) (string, *IdempotentRecord, error) {
	if err := ctx.Err(); err != nil {
		return "", nil, err
	}

	lockKey := "lock:idempotency:" + key
	dataKey := "data:idempotency:" + key

	// Выполняем скрипт через EVALSHA. Драйвер сам загрузит скрипт в кэш Redis при первом вызове.
	res, err := evaluateIdempotencyScript.Run(ctx, r.rdb, []string{lockKey, dataKey}, payloadHash, int64(ttl/time.Millisecond)).StringSlice()
	if err != nil {
		return "", nil, err
	}

	if len(res) < 1 {
		return "MISS", nil, nil
	}

	status := res[0]

	if status == "HIT" {
		// Если зафиксирован HIT, атомарно вычитываем хэш-карту ответа стандартной командой HGetAll
		dataMap, err := r.rdb.HGetAll(ctx, dataKey).Result()
		if err != nil {
			return "", nil, err
		}
		if len(dataMap) == 0 {
			return "MISS", nil, nil
		}

		var headers map[string][]string
		if err := json.Unmarshal([]byte(dataMap["headers"]), &headers); err != nil {
			headers = make(map[string][]string)
		}

		statusCode, _ := strconv.Atoi(dataMap["status"])
		var createdAt time.Time
		if t, err := strconv.ParseInt(dataMap["created_at"], 10, 64); err == nil {
			createdAt = time.Unix(t, 0)
		}

		return status, &IdempotentRecord{
			Key:             key,
			PayloadHash:     dataMap["hash"],
			StatusCode:      statusCode,
			ResponseBody:    []byte(dataMap["body"]),
			ResponseHeaders: headers,
			CreatedAt:       createdAt,
		}, nil
	}

	return status, nil, nil
}

// ExtendLock продлевает блокировку в Redis, защищая долгие транзакции от перехвата по таймауту.
func (r *redisRepository) ExtendLock(ctx context.Context, key string, payloadHash string, ttl time.Duration) (bool, error) {
	lockKey := "lock:idempotency:" + key
	res, err := extendLockScript.Run(ctx, r.rdb, []string{lockKey}, payloadHash, int64(ttl/time.Millisecond)).Result()
	if err != nil {
		return false, err
	}

	val, ok := res.(int64)
	if !ok {
		return false, nil
	}
	return val == 1, nil
}

// Unlock удаляет ключ распределенной блокировки из Redis.
func (r *redisRepository) Unlock(ctx context.Context, key string) error {
	return r.rdb.Del(context.Background(), "lock:idempotency:"+key).Err()
}

// Save сохраняет сериализованный успешный HTTP-ответ в Redis в виде хэш-карты с ограничением по времени (TTL).
func (r *redisRepository) Save(ctx context.Context, record *IdempotentRecord, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	dataKey := "data:idempotency:" + record.Key

	headersJSON, err := json.Marshal(record.ResponseHeaders)
	if err != nil {
		return err
	}

	err = r.rdb.HSet(ctx, dataKey, map[string]interface{}{
		"body":       string(record.ResponseBody),
		"hash":       record.PayloadHash,
		"status":     strconv.Itoa(record.StatusCode),
		"headers":    string(headersJSON),
		"created_at": record.CreatedAt.Unix(),
	}).Err()
	if err != nil {
		return err
	}
	return r.rdb.Expire(ctx, dataKey, ttl).Err()
}

// ProductionResponseWriter расширяет http.ResponseWriter, перехватывая поток байт ответа
// и защищая оперативную память сервера от утечек (OOM) при обработке тяжелых payloads.
type ProductionResponseWriter struct {
	headers     http.Header
	Body        *bytes.Buffer
	StatusCode  int
	MaxBodySize int
	TooLarge    bool
}

// NewProductionResponseWriter инициализирует легковесный продакшен-враппер ответа.
func NewProductionResponseWriter(buf *bytes.Buffer, maxSize int) *ProductionResponseWriter {
	return &ProductionResponseWriter{
		headers:     make(http.Header),
		Body:        buf,
		StatusCode:  http.StatusOK,
		MaxBodySize: maxSize,
	}
}

// Header возвращает внутреннюю карту HTTP-заголовков.
func (w *ProductionResponseWriter) Header() http.Header { return w.headers }

// WriteHeader фиксирует HTTP-код ответа бизнес-логики.
func (w *ProductionResponseWriter) WriteHeader(statusCode int) { w.StatusCode = statusCode }

// Write буферизирует байты ответа. При превышении MaxBodySize память мгновенно сбрасывается через Reset().
func (w *ProductionResponseWriter) Write(b []byte) (int, error) {
	if w.TooLarge {
		return len(b), nil
	}
	if w.MaxBodySize > 0 && w.Body.Len()+len(b) > w.MaxBodySize {
		w.TooLarge = true
		w.Body.Reset()
	} else {
		w.Body.Write(b)
	}
	return len(b), nil
}
