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
var (
	evaluateIdempotencyScript = redis.NewScript(`
		if redis.call("EXISTS", KEYS) == 1 then
			return {"LOCKED", ""}
		end
		
		local data = redis.call("HGETALL", KEYS)
		if #data > 0 then
			return {"HIT", data}
		end
		
		redis.call("SET", KEYS, ARGV, "PX", ARGV)
		return {"MISS", ""}
	`)

	extendLockScript = redis.NewScript(`
		if redis.call("get", KEYS) == ARGV then
			return redis.call("pexpire", KEYS, ARGV)
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

// EvaluateIdempotency совмещает TryLock и Get в едином атомарном Lua-скрипте на стороне Redis по протоколу EVALSHA.
func (r *redisRepository) EvaluateIdempotency(ctx context.Context, key string, payloadHash string, ttl time.Duration) (string, *IdempotentRecord, error) {
	if err := ctx.Err(); err != nil {
		return "", nil, err
	}

	lockKey := "lock:idempotency:" + key
	dataKey := "data:idempotency:" + key

	res, err := evaluateIdempotencyScript.Run(ctx, r.rdb, []string{lockKey, dataKey}, payloadHash, int64(ttl/time.Millisecond)).Result()
	if err != nil {
		return "", nil, err
	}

	slice, ok := res.([]interface{})
	if !ok || len(slice) < 2 {
		return "MISS", nil, nil
	}

	status := slice[0].(string)

	if status == "HIT" {
		rawData := slice[1].([]interface{})
		dataMap := make(map[string]string)
		for i := 0; i < len(rawData); i += 2 {
			dataMap[rawData[i].(string)] = rawData[i+1].(string)
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

// ExtendLock атомарно продлевает блокировку через Lua-скрипт, проверяя владельца ключа.
func (r *redisRepository) ExtendLock(ctx context.Context, key string, payloadHash string, ttl time.Duration) (bool, error) {
	lockKey := "lock:idempotency:" + key
	res, err := extendLockScript.Run(ctx, r.rdb, []string{lockKey}, payloadHash, int64(ttl/time.Millisecond)).Result()
	if err != nil {
		return false, err
	}
	return res.(int64) == 1, nil
}

// Unlock удаляет замок из Redis.
func (r *redisRepository) Unlock(ctx context.Context, key string) error {
	return r.rdb.Del(context.Background(), "lock:idempotency:"+key).Err()
}

// Save персистирует сериализованный HTTP-ответ в Redis в виде хэш-таблицы.
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

// ProductionResponseWriter осуществляет проксирование ответов без лишних аллокаций памяти.
type ProductionResponseWriter struct {
	headers     http.Header
	Body        *bytes.Buffer
	StatusCode  int
	MaxBodySize int
	TooLarge    bool
}

func NewProductionResponseWriter(buf *bytes.Buffer, maxSize int) *ProductionResponseWriter {
	return &ProductionResponseWriter{headers: make(http.Header), Body: buf, StatusCode: http.StatusOK, MaxBodySize: maxSize}
}
func (w *ProductionResponseWriter) Header() http.Header        { return w.headers }
func (w *ProductionResponseWriter) WriteHeader(statusCode int) { w.StatusCode = statusCode }
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
