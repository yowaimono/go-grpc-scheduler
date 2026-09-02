package observability

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetricsRegistryExportsAllCriticalMetrics(t *testing.T) {
	m := NewMetrics()
	m.TasksSubmitted.Inc()
	m.HTTPRequests.WithLabelValues("GET", "/healthz", "200").Inc()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()
	for _, metric := range []string{"scheduler_tasks_submitted_total", "scheduler_http_requests_total", "scheduler_schedule_lag_seconds"} {
		if !strings.Contains(body, metric) {
			t.Fatalf("metric %q missing", metric)
		}
	}
}
