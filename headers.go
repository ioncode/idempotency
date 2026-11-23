package idempotency

import (
	"maps"
	"net/http"
	"slices"
	"strings"
)

const (
	// HeaderIdempotencyKey — стандартное имя HTTP-заголовка для передачи ключа идемпотентности (IETF draft).
	HeaderIdempotencyKey = "Idempotency-Key"

	// HeaderIdempotencyStat — стандартное имя диагностического HTTP-заголовка со статусом обработки запроса.
	HeaderIdempotencyStat = "X-Idempotency-Status"

	// HeaderOriginalDate — имя HTTP-заголовка с датой и временем первоначального выполнения транзакции (RFC 7231).
	HeaderOriginalDate = "Original-Request-Date"
)

// hopByHopHeaders содержит список заголовков уровня соединения (RFC 7230),
// а также транспортных заголовков, которые не должны сохраняться в кэш
// и воспроизводиться при повторных запросах.
var hopByHopHeaders = map[string]bool{
	"connection":          true,
	"keep-alive":          true,
	"proxy-authenticate":  true,
	"proxy-authorization": true,
	"te":                  true,
	"trailer":             true,
	"transfer-encoding":   true,
	"upgrade":             true,
	"content-length":      true,
	"date":                true,
	"server":              true,
}

// isHopByHopHeader проверяет, относится ли имя заголовка к hop-by-hop
// или транспортным заголовкам, исключаемым из кэширования.
func isHopByHopHeader(name string) bool {
	return hopByHopHeaders[strings.ToLower(name)]
}

// filterHeaders возвращает глубокую копию HTTP-заголовков ответа,
// исключая hop-by-hop заголовки и заголовок статуса идемпотентности.
func filterHeaders(raw http.Header, statusHeader string) http.Header {
	if len(raw) == 0 {
		return nil
	}

	filtered := make(http.Header, len(raw))
	statusHeaderLower := strings.ToLower(statusHeader)

	for key, values := range raw {
		lowerKey := strings.ToLower(key)
		if hopByHopHeaders[lowerKey] || (statusHeaderLower != "" && lowerKey == statusHeaderLower) {
			continue
		}
		filtered[key] = slices.Clone(values)
	}

	return filtered
}

// copyCachedHeaders копирует сохраненные заголовки из IdempotentRecord в http.ResponseWriter,
// дополнительно защищая от случайной перезаписи hop-by-hop заголовков.
func copyCachedHeaders(dst http.Header, src http.Header) {
	if len(src) == 0 {
		return
	}

	for key, values := range maps.All(src) {
		if isHopByHopHeader(key) {
			continue
		}
		for _, v := range values {
			dst.Add(key, v)
		}
	}
}
