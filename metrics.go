package idempotency

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics инкапсулирует счетчики Prometheus для мониторинга слоя идемпотентности.
type Metrics struct {
	RequestsTotal *prometheus.CounterVec
	ErrorsTotal   *prometheus.CounterVec
}

// NewMetrics регистрирует и возвращает набор Prometheus-метрик в переданном реестре reg.
// Если reg равен nil, используется prometheus.DefaultRegisterer.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}

	factory := promauto.With(reg)

	return &Metrics{
		RequestsTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "idempotency_requests_total",
				Help: "Total number of requests handled by the idempotency middleware.",
			},
			[]string{"status", "path"},
		),
		ErrorsTotal: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "idempotency_storage_errors_total",
				Help: "Total number of errors encountered while interacting with the idempotency storage.",
			},
			[]string{"operation"},
		),
	}
}

var globalMetrics = NewMetrics(prometheus.DefaultRegisterer)

// IncRequests увеличивает счетчик обработанных запросов с разбивкой по статусам и HTTP-путям.
func (m *Metrics) IncRequests(status Status, path string) {
	if m != nil && m.RequestsTotal != nil {
		m.RequestsTotal.WithLabelValues(status.String(), path).Inc()
	}
}

// IncErrors увеличивает счетчик инфраструктурных ошибок хранилища по типу операции.
func (m *Metrics) IncErrors(operation string) {
	if m != nil && m.ErrorsTotal != nil {
		m.ErrorsTotal.WithLabelValues(operation).Inc()
	}
}
