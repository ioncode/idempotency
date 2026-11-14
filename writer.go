package idempotency

import (
	"bytes"
	"net/http"
)

// ProductionResponseWriter реализует гибридный перехватчик HTTP-ответа.
// Пока объем ответа не превышает MaxBodySize, данные накапливаются в буфере (что позволяет
// выставить точный X-Idempotency-Status до отправки заголовков в сеть). При превышении лимита
// буфер сбрасывается клиенту, и writer переключается в режим прямого стриминга.
type ProductionResponseWriter struct {
	w             http.ResponseWriter
	Body          *bytes.Buffer
	StatusCode    int
	MaxBodySize   int
	statusHeader  string
	TooLarge      bool
	headerFlushed bool
}

// NewProductionResponseWriter инициализирует перехватчик ответа с поддержкой отложенной отправки заголовков.
func NewProductionResponseWriter(w http.ResponseWriter, buf *bytes.Buffer, maxSize int, statusHeader string) *ProductionResponseWriter {
	return &ProductionResponseWriter{
		w:            w,
		Body:         buf,
		StatusCode:   http.StatusOK,
		MaxBodySize:  maxSize,
		statusHeader: statusHeader,
	}
}

// Header возвращает карту HTTP-заголовков оригинального http.ResponseWriter.
func (w *ProductionResponseWriter) Header() http.Header {
	return w.w.Header()
}

// WriteHeader запоминает HTTP статус-код ответа, откладывая реальную отправку заголовков
// до завершения запроса или превышения лимита буферизации.
func (w *ProductionResponseWriter) WriteHeader(statusCode int) {
	if w.headerFlushed {
		return
	}
	w.StatusCode = statusCode
}

// Write буферизирует ответ в пределах MaxBodySize. При выходе за лимит переводит writer
// в режим прямого стриминга в сетевой сокет с выставлением статуса BYPASS_TOO_LARGE.
func (w *ProductionResponseWriter) Write(b []byte) (int, error) {
	// Если уже в режиме стриминга — пишем напрямую в сокет
	if w.TooLarge {
		return w.w.Write(b)
	}

	// Проверяем, не превысит ли новый чанк лимит кэширования в RAM
	if w.MaxBodySize > 0 && w.Body.Len()+len(b) > w.MaxBodySize {
		w.TooLarge = true
		w.flushToStream(StatusBypassTooLarge.String())
		return w.w.Write(b)
	}

	return w.Body.Write(b)
}

// flushToStream отправляет заголовки, статус-код и накопленный буфер в реальный http.ResponseWriter,
// освобождая буфер памяти для защиты от OOM.
func (w *ProductionResponseWriter) flushToStream(statusValue string) {
	if w.headerFlushed {
		return
	}
	if w.statusHeader != "" && statusValue != "" {
		w.w.Header().Set(w.statusHeader, statusValue)
	}
	w.w.WriteHeader(w.StatusCode)
	w.headerFlushed = true

	if w.Body.Len() > 0 {
		_, _ = w.w.Write(w.Body.Bytes())
		w.Body.Reset()
	}
}

// Commit завершает отправку ответа клиенту, если хендлер уложился в лимит кэширования
// и данные еще не были сброшены в потоковый режим.
func (w *ProductionResponseWriter) Commit(statusValue string) {
	if w.headerFlushed {
		return
	}
	if w.statusHeader != "" && statusValue != "" {
		w.w.Header().Set(w.statusHeader, statusValue)
	}
	w.w.WriteHeader(w.StatusCode)
	w.headerFlushed = true

	if w.Body.Len() > 0 {
		_, _ = w.w.Write(w.Body.Bytes())
	}
}

// Flush принудительно переводит ответ в потоковый режим (если он еще в буфере)
// и делегирует сброс данных интерфейсу http.Flusher (для SSE и стриминга).
func (w *ProductionResponseWriter) Flush() {
	if !w.headerFlushed {
		w.TooLarge = true
		w.flushToStream(StatusBypassTooLarge.String())
	}
	if f, ok := w.w.(http.Flusher); ok {
		f.Flush()
	}
}
