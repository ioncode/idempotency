package idempotency

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

func TestMetrics_Counters(t *testing.T) {
	globalMetrics.IncRequests(StatusHit, "/api/test")
	assert.GreaterOrEqual(t, testutil.ToFloat64(globalMetrics.RequestsTotal.WithLabelValues(StatusHit.String(), "/api/test")), 1.0)

	globalMetrics.IncErrors("eval_lua")
	assert.GreaterOrEqual(t, testutil.ToFloat64(globalMetrics.ErrorsTotal.WithLabelValues("eval_lua")), 1.0)
}
