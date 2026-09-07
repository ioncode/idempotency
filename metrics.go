package idempotency

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics инкапсулирует коллекторы Prometheus для мониторинга модуля идемпотентности.
type Metrics struct {
	RequestsTotal *prometheus.CounterVec
	ErrorsTotal   *prometheus.CounterVec
}

var globalMetrics *Metrics

func init() {
	globalMetrics = &Metrics{
		RequestsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "idempotency_requests_total",
				Help: "Total number of requests handled by the idempotency middleware.",
			},
			[]string{"status", "path"},
		),
		ErrorsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "idempotency_redis_errors_total",
				Help: "Total number of errors encountered while interacting with the idempotency storage.",
			},
			[]string{"operation"},
		),
	}
}

// IncRequests увеличивает счетчик обработанных запросов с разбивкой по статусам и путям.
func (m *Metrics) IncRequests(status Status, path string) {
	if m != nil && m.RequestsTotal != nil {
		m.RequestsTotal.WithLabelValues(status.String(), path).Inc()
	}
}

// IncErrors увеличивает счетчик ошибок инфраструктурного слоя (хранилища).
func (m *Metrics) IncErrors(operation string) {
	if m != nil && m.ErrorsTotal != nil {
		m.ErrorsTotal.WithLabelValues(operation).Inc()
	}
}
