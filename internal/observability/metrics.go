package observability

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	Registry            *prometheus.Registry
	TasksSubmitted      prometheus.Counter
	TasksAssigned       prometheus.Counter
	TasksSucceeded      prometheus.Counter
	TasksFailed         prometheus.Counter
	TasksRequeued       prometheus.Counter
	QueueDepth          prometheus.Gauge
	WorkersOnline       prometheus.Gauge
	LeaderState         prometheus.Gauge
	ScheduleLag         prometheus.Histogram
	DBOperationDuration prometheus.Histogram
	HTTPRequests        *prometheus.CounterVec
	HTTPDuration        *prometheus.HistogramVec
}

func NewMetrics() *Metrics {
	m := &Metrics{Registry: prometheus.NewRegistry(),
		TasksSubmitted:      prometheus.NewCounter(prometheus.CounterOpts{Name: "scheduler_tasks_submitted_total", Help: "Tasks accepted by the scheduler."}),
		TasksAssigned:       prometheus.NewCounter(prometheus.CounterOpts{Name: "scheduler_tasks_assigned_total", Help: "Tasks assigned to workers."}),
		TasksSucceeded:      prometheus.NewCounter(prometheus.CounterOpts{Name: "scheduler_tasks_succeeded_total", Help: "Tasks completed successfully."}),
		TasksFailed:         prometheus.NewCounter(prometheus.CounterOpts{Name: "scheduler_tasks_failed_total", Help: "Tasks completed with failure."}),
		TasksRequeued:       prometheus.NewCounter(prometheus.CounterOpts{Name: "scheduler_tasks_requeued_total", Help: "Tasks requeued after lease expiry or failure."}),
		QueueDepth:          prometheus.NewGauge(prometheus.GaugeOpts{Name: "scheduler_queue_depth", Help: "Tasks ready for dispatch."}),
		WorkersOnline:       prometheus.NewGauge(prometheus.GaugeOpts{Name: "scheduler_workers_online", Help: "Registered worker instances."}),
		LeaderState:         prometheus.NewGauge(prometheus.GaugeOpts{Name: "scheduler_leader_state", Help: "1 when this node is master, 0 otherwise."}),
		ScheduleLag:         prometheus.NewHistogram(prometheus.HistogramOpts{Name: "scheduler_schedule_lag_seconds", Help: "Delay between scheduled and actual dispatch."}),
		DBOperationDuration: prometheus.NewHistogram(prometheus.HistogramOpts{Name: "scheduler_db_operation_duration_seconds", Help: "Database operation latency."}),
		HTTPRequests:        prometheus.NewCounterVec(prometheus.CounterOpts{Name: "scheduler_http_requests_total", Help: "HTTP requests by method, path and status."}, []string{"method", "path", "status"}),
		HTTPDuration:        prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "scheduler_http_request_duration_seconds", Help: "HTTP request latency."}, []string{"method", "path"}),
	}
	for _, collector := range []prometheus.Collector{m.TasksSubmitted, m.TasksAssigned, m.TasksSucceeded, m.TasksFailed, m.TasksRequeued, m.QueueDepth, m.WorkersOnline, m.LeaderState, m.ScheduleLag, m.DBOperationDuration, m.HTTPRequests, m.HTTPDuration} {
		m.Registry.MustRegister(collector)
	}
	return m
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{EnableOpenMetrics: true})
}
func ObserveDuration(h prometheus.Observer, start time.Time) { h.Observe(time.Since(start).Seconds()) }
