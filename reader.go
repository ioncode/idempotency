package idempotency

import (
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"
)

// hashingReader реализует потоковое вычисление SHA-256 для входящего тела запроса.
// Если размер входящих данных превышает maxBodySize, хэширование прекращается
// для защиты CPU от атак, но оригинальный поток данных НЕ обрывается.
type hashingReader struct {
	rc          io.ReadCloser
	hasher      hash.Hash
	readBytes   int64
	maxBodySize int64
	TooLarge    bool
}

// newHashingReader инициализирует потоковый ридер с контролем лимита байт.
func newHashingReader(rc io.ReadCloser, maxRequestSize int64) *hashingReader {
	return &hashingReader{
		rc:          rc,
		hasher:      sha256.New(),
		maxBodySize: maxRequestSize,
	}
}

// Read перехватывает байты из сетевого потока и параллельно отправляет их в криптографический хэшер.
func (hr *hashingReader) Read(p []byte) (n int, err error) {
	n, err = hr.rc.Read(p)
	if n > 0 && !hr.TooLarge {
		hr.readBytes += int64(n)

		if hr.maxBodySize > 0 && hr.readBytes > hr.maxBodySize {
			hr.TooLarge = true
			hr.hasher.Reset()
		} else {
			hr.hasher.Write(p[:n])
		}
	}
	return n, err
}

// Close закрывает нижележащий сетевой поток чтения тела запроса.
func (hr *hashingReader) Close() error { return hr.rc.Close() }

// Sum возвращает финальный строковый SHA-256 хэш, если лимит памяти не был превышен.
func (hr *hashingReader) Sum() string {
	if hr.TooLarge {
		return ""
	}
	return hex.EncodeToString(hr.hasher.Sum(nil))
}
