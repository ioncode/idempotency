package idempotency

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProductionResponseWriter_BufferedCommit(t *testing.T) {
	rec := httptest.NewRecorder()
	buf := new(bytes.Buffer)
	crw := NewProductionResponseWriter(rec, buf, 1024, HeaderIdempotencyStat)

	crw.Header().Set("Content-Type", "application/json")
	crw.WriteHeader(http.StatusCreated)
	n, err := crw.Write([]byte(`{"id":42}`))

	require.NoError(t, err)
	assert.Equal(t, 9, n)
	assert.False(t, crw.TooLarge)
	assert.False(t, crw.headerFlushed, "заголовки не должны уйти до Commit")
	assert.Equal(t, `{"id":42}`, buf.String())

	// Повторный WriteHeader после Commit не должен менять статус
	crw.Commit(StatusMiss.String())
	assert.True(t, crw.headerFlushed)

	// Повторный Commit и WriteHeader после отправки не должны паниковать или менять ответ
	crw.WriteHeader(http.StatusBadRequest)
	crw.Commit(StatusHit.String())

	assert.Equal(t, http.StatusCreated, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	assert.Equal(t, StatusMiss.String(), rec.Header().Get(HeaderIdempotencyStat))
	assert.Equal(t, `{"id":42}`, rec.Body.String())
}

func TestProductionResponseWriter_ExceedMaxBodySize(t *testing.T) {
	rec := httptest.NewRecorder()
	buf := new(bytes.Buffer)
	// Лимит кэширования — 10 байт
	crw := NewProductionResponseWriter(rec, buf, 10, HeaderIdempotencyStat)

	crw.WriteHeader(http.StatusAccepted)

	// 1. Первый чанк (6 байт) укладывается в лимит 10 байт -> идет в буфер
	_, err := crw.Write([]byte("chunk1"))
	require.NoError(t, err)
	assert.False(t, crw.TooLarge)
	assert.Equal(t, 6, buf.Len())

	// 2. Второй чанк (6 байт) превышает лимит (6 + 6 > 10) -> сброс в сокет и переход в стриминг
	_, err = crw.Write([]byte("chunk2"))
	require.NoError(t, err)
	assert.True(t, crw.TooLarge)
	assert.True(t, crw.headerFlushed)
	assert.Equal(t, 0, buf.Len(), "буфер RAM должен очиститься при превышении лимита")

	// 3. Третий чанк (6 байт) в режиме TooLarge -> пишется напрямую в rec, минуя buf
	_, err = crw.Write([]byte("chunk3"))
	require.NoError(t, err)
	assert.Equal(t, 0, buf.Len())

	// Вызов Commit в конце хендлера не должен перезаписать заголовок статуса
	crw.Commit(StatusMiss.String())

	assert.Equal(t, http.StatusAccepted, rec.Code)
	assert.Equal(t, StatusBypassTooLarge.String(), rec.Header().Get(HeaderIdempotencyStat))
	assert.Equal(t, "chunk1chunk2chunk3", rec.Body.String(), "клиент должен получить все 3 чанка без потерь")
}

func TestProductionResponseWriter_Flush(t *testing.T) {
	rec := httptest.NewRecorder()
	buf := new(bytes.Buffer)
	crw := NewProductionResponseWriter(rec, buf, 1024, HeaderIdempotencyStat)

	_, err := crw.Write([]byte("event: ping\n\n"))
	require.NoError(t, err)

	// Flush должен перевести writer в потоковый режим, сбросить буфер и вызвать Flusher
	crw.Flush()

	assert.True(t, crw.TooLarge)
	assert.True(t, crw.headerFlushed)
	assert.True(t, rec.Flushed)
	assert.Equal(t, 0, buf.Len())
	assert.Equal(t, StatusBypassTooLarge.String(), rec.Header().Get(HeaderIdempotencyStat))
	assert.Equal(t, "event: ping\n\n", rec.Body.String())

	// Повторный Flush не должен ломать состояние
	crw.Flush()
	assert.Equal(t, "event: ping\n\n", rec.Body.String())
}

func TestProductionResponseWriter_EmptyBodyDefaultStatus(t *testing.T) {
	rec := httptest.NewRecorder()
	buf := new(bytes.Buffer)
	crw := NewProductionResponseWriter(rec, buf, 1024, "")

	// Если WriteHeader не вызывался, должен отправиться 200 OK без паники при пустом statusHeader
	crw.Commit(StatusMiss.String())

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Empty(t, rec.Header().Get(HeaderIdempotencyStat))
	assert.Empty(t, rec.Body.String())
}
