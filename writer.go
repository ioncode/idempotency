package idempotency

import (
	"bytes"
	"net/http"
)

// ProductionResponseWriter реализует гибридный перехватчик HTTP-ответа с буферизацией в памяти до MaxBodySize
// и автоматическим переходом на стриминг при превышении лимита или принудительном вызове Flush.
type ProductionResponseWriter struct {
	w             http.ResponseWriter
	Body          *bytes.Buffer
	StatusCode    int
	MaxBodySize   int
	statusHeader  string
	TooLarge      bool
	headerFlushed bool
	wroteHeader   bool
}

// NewProductionResponseWriter создает новый экземпляр ProductionResponseWriter.
func NewProductionResponseWriter(w http.ResponseWriter, buf *bytes.Buffer, maxSize int, statusHeader string) *ProductionResponseWriter {
	return &ProductionResponseWriter{
		w:            w,
		Body:         buf,
		StatusCode:   http.StatusOK,
		MaxBodySize:  maxSize,
		statusHeader: statusHeader,
	}
}

// Unwrap возвращает оригинальный http.ResponseWriter для поддержки http.ResponseController.
func (w *ProductionResponseWriter) Unwrap() http.ResponseWriter {
	return w.w
}

// Header делегирует получение заголовков оригинальному ResponseWriter.
func (w *ProductionResponseWriter) Header() http.Header {
	return w.w.Header()
}

// WriteHeader сохраняет статус-код, игнорируя повторные вызовы.
func (w *ProductionResponseWriter) WriteHeader(statusCode int) {
	if w.headerFlushed || w.wroteHeader {
		return
	}
	w.StatusCode = statusCode
	w.wroteHeader = true
}

// Write записывает данные в буфер или переключается на прямой стриминг при превышении MaxBodySize.
func (w *ProductionResponseWriter) Write(b []byte) (int, error) {
	w.wroteHeader = true

	if w.TooLarge {
		return w.w.Write(b)
	}

	if w.MaxBodySize > 0 && w.Body.Len()+len(b) > w.MaxBodySize {
		w.TooLarge = true
		w.flushToStream(StatusBypassTooLarge.String())
		return w.w.Write(b)
	}

	return w.Body.Write(b)
}

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

// Commit фиксирует ответ и отправляет накопленный буфер клиенту.
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

// Flush переводит writer в потоковый режим и вызывает Flush у нижележащего хэндлера при его наличии.
func (w *ProductionResponseWriter) Flush() {
	if !w.headerFlushed {
		w.TooLarge = true
		w.flushToStream(StatusBypassTooLarge.String())
	}
	if f, ok := w.w.(http.Flusher); ok {
		f.Flush()
	}
}
