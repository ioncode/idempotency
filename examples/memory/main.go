// Package main демонстрирует минимальный локальный запуск HTTP-сервера
// с использованием idempotency middleware и in-memory хранилища.
package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/ioncode/idempotency"
	"github.com/ioncode/idempotency/store/memory"
)

func main() {
	// 1. Инициализируем потокобезопасное in-memory хранилище
	store := memory.New()
	defer store.Close()

	// 2. Настраиваем middleware с использованием разделенных контрактов Locker и Saver
	mw := idempotency.NewMiddleware(
		store,
		store,
		nil,
		idempotency.WithLockTTL(5*time.Second),
		idempotency.WithResponseTTL(10*time.Minute),
	)

	mux := http.NewServeMux()
	mux.Handle("POST /orders", mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = fmt.Fprintln(w, `{"status":"created"}`)
	})))

	log.Println("Server listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", mux))
}
