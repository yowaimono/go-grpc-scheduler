package httpapi

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	schedulerv1 "github.com/yowaimono/go-grpc-scheduler/api/gen"
	"github.com/yowaimono/go-grpc-scheduler/internal/auth"
	"github.com/yowaimono/go-grpc-scheduler/internal/observability"
	"github.com/yowaimono/go-grpc-scheduler/internal/scheduler"
	"github.com/yowaimono/go-grpc-scheduler/internal/transport/grpcserver"
	"go.opentelemetry.io/otel"
)

type Server struct {
	Engine          *scheduler.Engine
	NodeID          string
	Role            string
	grpc            *grpcserver.Server
	Metrics         *observability.Metrics
	Auth            *auth.Authenticator
	AllowedOrigins  map[string]struct{}
	MaxPayloadBytes int64
	MaxTaskTimeout  time.Duration
}

type submitRequest struct {
	TaskID       string                     `json:"task_id,omitempty"`
	TaskName     string                     `json:"task_name"`
	Role         string                     `json:"role"`
	Params       map[string]json.RawMessage `json:"params,omitempty"`
	Payload      []byte                     `json:"payload,omitempty"`
	RunAtUnixMS  int64                      `json:"run_at_unix_ms,omitempty"`
	DelayMS      int64                      `json:"delay_ms,omitempty"`
	ScheduleType string                     `json:"schedule_type,omitempty"`
	IntervalMS   int64                      `json:"interval_ms,omitempty"`
	Priority     int                        `json:"priority"`
	TimeoutMS    int64                      `json:"timeout_ms,omitempty"`
	TenantID     string                     `json:"tenant_id,omitempty"`
	CronExpr     string                     `json:"cron_expr,omitempty"`
	Timezone     string                     `json:"timezone,omitempty"`
}

func tenantFromRequest(r *http.Request) string {
	if p, ok := auth.PrincipalFromContext(r.Context()); ok {
		return p.Tenant
	}
	return "local"
}
func actorID(r *http.Request) string {
	if p, ok := auth.PrincipalFromContext(r.Context()); ok {
		return p.Subject
	}
	return "local"
}

func New(engine *scheduler.Engine, nodeID, role string, metrics *observability.Metrics) *Server {
	return &Server{Engine: engine, NodeID: nodeID, Role: role, Metrics: metrics, Auth: auth.New(nil), AllowedOrigins: map[string]struct{}{"http://localhost:8780": {}, "http://127.0.0.1:8780": {}}, MaxPayloadBytes: 1 << 20, MaxTaskTimeout: 24 * time.Hour, grpc: &grpcserver.Server{Engine: engine}}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.health)
	mux.HandleFunc("/readyz", s.ready)
	mux.HandleFunc("/api/v1/discovery", s.discovery)
	mux.HandleFunc("/api/v1/workers", s.workers)
	mux.HandleFunc("/api/v1/tasks", s.tasks)
	mux.HandleFunc("/api/v1/tasks/bulk", s.bulkTasks)
	mux.HandleFunc("/api/v1/tasks/", s.taskAction)
	mux.HandleFunc("/api/v1/events", s.events)
	mux.HandleFunc("/api/v1/observability/overview", s.overview)
	mux.HandleFunc("/api/v1/observability/dashboard", s.dashboard)
	mux.Handle("/metrics", s.metricsHandler())
	return s.Auth.RequireHTTP("viewer", requestID(requestMetrics(cors(mux, s.AllowedOrigins), s.Metrics)))
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}
func (s *Server) ready(w http.ResponseWriter, _ *http.Request) {
	if s.Engine.IsDraining() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"ready": false, "role": "draining"})
		return
	}
	role := "slave"
	if s.Engine.Role() == scheduler.RoleMaster {
		role = "master"
	}
	writeJSON(w, http.StatusOK, map[string]any{"ready": true, "role": role})
}
func (s *Server) discovery(w http.ResponseWriter, _ *http.Request) {
	role := "slave"
	if s.Engine.Role() == scheduler.RoleMaster {
		role = "master"
	}
	writeJSON(w, http.StatusOK, map[string]any{"node_id": s.NodeID, "role": role, "leader_id": s.NodeID, "epoch": 1, "ready": true, "api_base": ""})
}
func (s *Server) workers(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"items": s.Engine.SnapshotWorkers()})
}
func (s *Server) tasks(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		tenant := tenantFromRequest(r)
		writeJSON(w, http.StatusOK, map[string]any{"items": s.Engine.SnapshotTasksForTenant(tenant)})
		return
	}
	if r.Method == http.MethodDelete {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if p, ok := auth.PrincipalFromContext(r.Context()); ok && p.Role != "admin" && p.Role != "operator" {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "operator role required"})
		return
	}
	if r.ContentLength > s.MaxPayloadBytes {
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "task payload too large"})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, s.MaxPayloadBytes)
	var req submitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if req.TimeoutMS < 0 || (req.TimeoutMS > 0 && time.Duration(req.TimeoutMS)*time.Millisecond > s.MaxTaskTimeout) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid task timeout"})
		return
	}
	params := make(map[string][]byte, len(req.Params))
	for key, value := range req.Params {
		params[key] = append([]byte(nil), value...)
	}
	req.TenantID = tenantFromRequest(r)
	result, err := s.grpc.SubmitTask(r.Context(), &schedulerv1.SubmitTaskRequest{TaskId: req.TaskID, TaskName: req.TaskName, Role: req.Role, Params: params, Payload: req.Payload, RunAtUnixMs: req.RunAtUnixMS, DelayMs: req.DelayMS, ScheduleType: req.ScheduleType, IntervalMs: req.IntervalMS, Priority: int32(req.Priority), TimeoutMs: req.TimeoutMS, TenantId: req.TenantID, CronExpr: req.CronExpr, Timezone: req.Timezone})
	if err != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"code": "NOT_LEADER", "error": err.Error()})
		return
	}
	_ = s.Engine.Audit(r.Context(), scheduler.AuditRecord{TenantID: req.TenantID, ActorID: actorID(r), Action: "task.create", ResourceType: "task", ResourceID: result.TaskId, RequestID: r.Header.Get("X-Request-ID"), TaskID: result.TaskId})
	writeJSON(w, http.StatusAccepted, result)
}

func (s *Server) taskAction(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/v1/tasks/"), "/"), "/")
	if len(parts) == 1 && r.Method == http.MethodGet {
		s.taskDetail(w, r)
		return
	}
	if len(parts) != 2 {
		http.NotFound(w, r)
		return
	}
	taskID, action := parts[0], parts[1]
	if tenant := tenantFromRequest(r); tenant != "" {
		if owner, ok := s.Engine.TaskTenant(taskID); !ok || owner != tenant {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "task not found"})
			return
		}
	}
	if action == "history" && r.Method == http.MethodGet {
		history, err := s.Engine.TaskHistoryContext(r.Context(), taskID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		attempts, err := s.Engine.TaskAttemptsContext(r.Context(), taskID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": history, "attempts": attempts})
		return
	}
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if p, ok := auth.PrincipalFromContext(r.Context()); ok && p.Role != "admin" && p.Role != "operator" {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "operator role required"})
		return
	}
	var err error
	switch action {
	case "pause":
		err = s.Engine.Pause(r.Context(), taskID)
	case "resume":
		err = s.Engine.Resume(r.Context(), taskID)
	case "cancel":
		err = s.Engine.Cancel(r.Context(), taskID)
	case "retry":
		err = s.Engine.Retry(r.Context(), taskID)
	default:
		http.NotFound(w, r)
		return
	}
	if err != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	_ = s.Engine.Audit(r.Context(), scheduler.AuditRecord{TenantID: tenantFromRequest(r), ActorID: actorID(r), Action: "task." + action, ResourceType: "task", ResourceID: taskID, RequestID: r.Header.Get("X-Request-ID"), TaskID: taskID})
	writeJSON(w, http.StatusOK, map[string]any{"task_id": taskID, "status": action})
}

func (s *Server) bulkTasks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		TaskIDs []string `json:"task_ids"`
		Action  string   `json:"action"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if len(req.TaskIDs) == 0 || len(req.TaskIDs) > 500 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "task_ids must contain 1-500 items"})
		return
	}
	results := make([]map[string]any, 0, len(req.TaskIDs))
	for _, taskID := range req.TaskIDs {
		var err error
		switch req.Action {
		case "pause":
			err = s.Engine.Pause(r.Context(), taskID)
		case "resume":
			err = s.Engine.Resume(r.Context(), taskID)
		case "cancel":
			err = s.Engine.Cancel(r.Context(), taskID)
		case "retry":
			err = s.Engine.Retry(r.Context(), taskID)
		default:
			err = fmt.Errorf("unsupported action %q", req.Action)
		}
		item := map[string]any{"task_id": taskID, "ok": err == nil}
		if err != nil {
			item["error"] = err.Error()
		}
		results = append(results, item)
	}
	writeJSON(w, http.StatusMultiStatus, map[string]any{"items": results})
}

func (s *Server) taskDetail(w http.ResponseWriter, r *http.Request) {
	taskID := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/v1/tasks/"), "/")
	if strings.Contains(taskID, "/") || taskID == "" {
		http.NotFound(w, r)
		return
	}
	if tenant := tenantFromRequest(r); tenant != "" {
		if owner, ok := s.Engine.TaskTenant(taskID); !ok || owner != tenant {
			http.NotFound(w, r)
			return
		}
	}
	task, ok := s.Engine.SnapshotTask(taskID)
	if !ok {
		http.NotFound(w, r)
		return
	}
	history, err := s.Engine.TaskHistoryContext(r.Context(), taskID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	attempts, err := s.Engine.TaskAttemptsContext(r.Context(), taskID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"task": task, "history": history, "attempts": attempts})
}

func (s *Server) events(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"items": s.Engine.SnapshotEvents()})
}
func (s *Server) overview(w http.ResponseWriter, _ *http.Request) {
	role := "slave"
	if s.Engine.Role() == scheduler.RoleMaster {
		role = "master"
	}
	writeJSON(w, http.StatusOK, map[string]any{"node_id": s.NodeID, "role": role, "queue_depth": s.Engine.ReadyLen(), "workers_online": s.Engine.WorkerCount(), "events": len(s.Engine.SnapshotEvents())})
}
func (s *Server) dashboard(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.Engine.Dashboard())
}
func (s *Server) metricsHandler() http.Handler {
	if s.Metrics == nil {
		return http.NotFoundHandler()
	}
	return s.Metrics.Handler()
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func cors(next http.Handler, allowed map[string]struct{}) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if _, ok := allowed[origin]; ok {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
		}
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Idempotency-Key")
		w.Header().Set("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" {
			id = fmt.Sprintf("req-%d", time.Now().UnixNano())
		}
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r)
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}
func (w *statusWriter) Write(body []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(body)
}
func requestMetrics(next http.Handler, metrics *observability.Metrics) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, span := otel.Tracer("scheduler/http").Start(r.Context(), "HTTP "+r.Method+" "+r.URL.Path)
		defer span.End()
		r = r.WithContext(ctx)
		started := time.Now()
		sw := &statusWriter{ResponseWriter: w}
		next.ServeHTTP(sw, r)
		if sw.status == 0 {
			sw.status = http.StatusOK
		}
		if metrics != nil {
			metrics.HTTPRequests.WithLabelValues(r.Method, r.URL.Path, fmt.Sprint(sw.status)).Inc()
			metrics.HTTPDuration.WithLabelValues(r.Method, r.URL.Path).Observe(time.Since(started).Seconds())
		}
		slog.Info("http_request", "request_id", w.Header().Get("X-Request-ID"), "method", r.Method, "path", r.URL.Path, "status", sw.status, "duration_ms", time.Since(started).Milliseconds())
	})
}

func RunTicker(stop <-chan struct{}, engine *scheduler.Engine) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			engine.Tick(now)
		case <-stop:
			return
		}
	}
}
