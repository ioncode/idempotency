package redis

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// unlockScript атомарно снимает распределенную блокировку в Redis только в том случае,
// если она все еще принадлежит текущему владельцу (ownerToken).
//
// KEYS[1] — ключ блокировки.
// ARGV[1] — токен владельца блокировки (ownerToken).
var unlockScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
else
	return 0
end
`)

// Unlock атомарно удаляет ключ распределенной блокировки из Redis,
// если его текущее значение совпадает с ownerToken.
// Если блокировка уже истекла или принадлежит другому владельцу, операция завершается без ошибки.
func (s *Store) Unlock(ctx context.Context, key string, ownerToken string) error {
	return unlockScript.Run(ctx, s.rdb, []string{s.lockKey(key)}, ownerToken).Err()
}
