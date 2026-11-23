package redis

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// extendLockScript атомарно продлевает время жизни распределенной блокировки в Redis,
// предварительно проверяя, что блокировка все еще принадлежит текущему владельцу (ownerToken).
//
// KEYS[1] — ключ блокировки.
// ARGV[1] — токен владельца блокировки (ownerToken).
// ARGV[2] — новое время жизни блокировки (TTL) в миллисекундах.
var extendLockScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("PEXPIRE", KEYS[1], ARGV[2])
else
	return 0
end
`)

// ExtendLock атомарно продлевает TTL распределенной блокировки на указанный период ttl,
// если ключ блокировки существует и его значение совпадает с ownerToken.
// Возвращает true, если блокировка успешно продлена, и false, если лок уже истек
// или был перехвачен другим обработчиком.
func (s *Store) ExtendLock(ctx context.Context, key string, ownerToken string, ttl time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}

	res, err := extendLockScript.Run(ctx, s.rdb, []string{s.lockKey(key)}, ownerToken, ttl.Milliseconds()).Int64()
	if err != nil {
		return false, err
	}

	return res == 1, nil
}
