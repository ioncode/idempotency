package redis

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/ioncode/idempotency"
	"github.com/redis/go-redis/v9"
)

// evaluateScript атомарно проверяет наличие активной блокировки и сохраненного ответа в Redis.
var evaluateScript = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 1 then
	return {"LOCKED"}
end

local data = redis.call("HGETALL", KEYS[2])
if #data > 0 then
	return {"HIT", unpack(data)}
end

redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[2])
return {"MISS"}
`)

// Evaluate атомарно проверяет состояние ключа идемпотентности в Redis и захватывает блокировку при отсутствии.
func (s *Store) Evaluate(ctx context.Context, key string, ownerToken string, ttl time.Duration) (idempotency.EvaluationStatus, *idempotency.IdempotentRecord, error) {
	if err := ctx.Err(); err != nil {
		return "", nil, err
	}

	res, err := evaluateScript.Run(ctx, s.rdb, []string{s.lockKey(key), s.dataKey(key)}, ownerToken, ttl.Milliseconds()).StringSlice()
	if err != nil {
		return "", nil, err
	}

	if len(res) == 0 {
		return idempotency.EvalMiss, nil, nil
	}

	switch idempotency.EvaluationStatus(res[0]) {
	case idempotency.EvalLocked:
		return idempotency.EvalLocked, nil, nil
	case idempotency.EvalHit:
		record, ok := parseRedisRecord(key, res[1:])
		if !ok {
			return idempotency.EvalMiss, nil, nil
		}
		return idempotency.EvalHit, record, nil
	default:
		return idempotency.EvalMiss, nil, nil
	}
}

// parseRedisRecord десериализует плоский список пар ключ-значение из результата HGETALL в IdempotentRecord.
func parseRedisRecord(key string, kv []string) (*idempotency.IdempotentRecord, bool) {
	if len(kv) < 2 {
		return nil, false
	}

	fields := make(map[string]string, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		fields[kv[i]] = kv[i+1]
	}

	var headers http.Header
	if rawHeaders, ok := fields["headers"]; ok && rawHeaders != "" {
		if err := json.Unmarshal([]byte(rawHeaders), &headers); err != nil {
			headers = make(http.Header)
		}
	}

	statusCode, _ := strconv.Atoi(fields["status"])

	var createdAt time.Time
	if rawTS, ok := fields["created_at"]; ok {
		if ts, err := strconv.ParseInt(rawTS, 10, 64); err == nil {
			createdAt = time.Unix(ts, 0).UTC()
		}
	}

	return &idempotency.IdempotentRecord{
		Key:             key,
		PayloadHash:     fields["hash"],
		StatusCode:      statusCode,
		ResponseBody:    []byte(fields["body"]),
		ResponseHeaders: headers,
		CreatedAt:       createdAt,
	}, true
}
