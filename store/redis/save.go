package redis

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/ioncode/idempotency"
)

// Save атомарно сохраняет сериализованный HTTP-ответ в Redis в виде хэш-карты
// и устанавливает время жизни записи (TTL) за один сетевой round-trip с помощью транзакции.
func (s *Store) Save(ctx context.Context, record *idempotency.IdempotentRecord, ttl time.Duration) error {
	if record == nil {
		return idempotency.ErrNilRecord
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	var headersJSON []byte
	if len(record.ResponseHeaders) > 0 {
		var err error
		headersJSON, err = json.Marshal(record.ResponseHeaders)
		if err != nil {
			return err
		}
	}

	createdAt := record.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}

	dataKey := s.dataKey(record.Key)

	pipe := s.rdb.TxPipeline()
	pipe.HSet(ctx, dataKey, map[string]any{
		"body":       record.ResponseBody,
		"hash":       record.PayloadHash,
		"status":     strconv.Itoa(record.StatusCode),
		"headers":    string(headersJSON),
		"created_at": createdAt.Unix(),
	})
	pipe.PExpire(ctx, dataKey, ttl)

	_, err := pipe.Exec(ctx)
	return err
}
