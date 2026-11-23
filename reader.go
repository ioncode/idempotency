package idempotency

import (
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"
	"net/http"
)

// hashingReader реализует потоковое вычисление SHA-256 для входящего тела HTTP-запроса
// без предварительной буферизации всего payload в оперативную память.
//
// При превышении лимита maxBodySize криптографическое хеширование прекращается
// и сбрасывается для защиты CPU от DoS-атак, однако чтение исходного потока
// для нижележащего обработчика продолжается в штатном режиме.
type hashingReader struct {
	rc          io.ReadCloser
	hasher      hash.Hash
	readBytes   int64
	maxBodySize int64
	TooLarge    bool
}

// newHashingReader инициализирует потоковый ридер с контролем максимального размера payload.
// Если rc равен nil, используется http.NoBody.
func newHashingReader(rc io.ReadCloser, maxRequestSize int64) *hashingReader {
	if rc == nil {
		rc = http.NoBody
	}
	return &hashingReader{
		rc:          rc,
		hasher:      sha256.New(),
		maxBodySize: maxRequestSize,
	}
}

// Read читает данные из нижележащего потока и параллельно передает их в SHA-256 хешер,
// пока суммарный объем прочитанных данных не превысит maxBodySize.
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

// Close закрывает нижележащий поток чтения тела запроса.
func (hr *hashingReader) Close() error {
	return hr.rc.Close()
}

// Sum возвращает hex-представление вычисленной контрольной суммы SHA-256.
// Если во время чтения был превышен лимит maxBodySize (TooLarge == true), возвращается пустая строка.
func (hr *hashingReader) Sum() string {
	if hr.TooLarge {
		return ""
	}
	var sum [sha256.Size]byte
	return hex.EncodeToString(hr.hasher.Sum(sum[:0]))
}
