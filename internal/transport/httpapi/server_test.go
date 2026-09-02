package httpapi

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/yowaimono/go-grpc-scheduler/internal/auth"
	"github.com/yowaimono/go-grpc-scheduler/internal/observability"
	"github.com/yowaimono/go-grpc-scheduler/internal/scheduler"
)

func TestDiscoveryAndTaskAPI(t *testing.T) {
	engine := scheduler.New("node-1", &scheduler.Coordinator{})
	engine.Elect()
	server := New(engine, "node-1", "master", observability.NewMetrics())
	handler := server.Handler()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/discovery", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("discovery status = %d", rec.Code)
	}
	var discovery map[string]any
	_ = json.NewDecoder(rec.Body).Decode(&discovery)
	if discovery["role"] != "master" {
		t.Fatalf("role = %v", discovery["role"])
	}

	body := bytes.NewBufferString(`{"task_name":"demo.echo","role":"default","delay_ms":0}`)
	req = httptest.NewRequest(http.MethodPost, "/api/v1/tasks", body)
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("submit status = %d, body=%s", rec.Code, rec.Body.String())
	}

	engine.RegisterWorker("w-1", []string{"default"}, 1)
	engine.Tick(time.Now().Add(time.Second))
	req = httptest.NewRequest(http.MethodGet, "/api/v1/tasks", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	raw, _ := io.ReadAll(rec.Body)
	if rec.Code != http.StatusOK || !bytes.Contains(raw, []byte("demo.echo")) {
		t.Fatalf("tasks response = %d %s", rec.Code, raw)
	}
	req = httptest.NewRequest(http.MethodGet, "/api/v1/events", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !bytes.Contains(rec.Body.Bytes(), []byte("task.submitted")) {
		t.Fatalf("events response = %d %s", rec.Code, rec.Body.String())
	}
	req = httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !bytes.Contains(rec.Body.Bytes(), []byte("scheduler_tasks_submitted_total")) {
		t.Fatalf("metrics response = %d", rec.Code)
	}
	req = httptest.NewRequest(http.MethodGet, "/api/v1/observability/dashboard", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !bytes.Contains(rec.Body.Bytes(), []byte("submitted_total")) {
		t.Fatalf("dashboard response=%d %s", rec.Code, rec.Body.String())
	}
	engine.BeginDrain()
	req = httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("ready during drain = %d", rec.Code)
	}
	// operator actions are explicit state transitions
	req = httptest.NewRequest(http.MethodPost, "/api/v1/tasks/task-1/pause", nil)
	// task-1 is not present; ensure the API reports a conflict rather than silently succeeding.
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("pause missing task status = %d", rec.Code)
	}
}

func TestCORSOnlyAllowsConfiguredOriginsAndPayloadLimit(t *testing.T) {
	engine := scheduler.New("node", &scheduler.Coordinator{})
	engine.Elect()
	server := New(engine, "node", "master", observability.NewMetrics())
	server.MaxPayloadBytes = 10
	handler := server.Handler()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set("Origin", "https://evil.example")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("unconfigured origin allowed")
	}
	req = httptest.NewRequest(http.MethodPost, "/api/v1/tasks", bytes.NewBufferString(`{"task_name":"long-payload","role":"default"}`))
	req.ContentLength = 100
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("payload status=%d", rec.Code)
	}
}

func TestRBACAndTenantIsolation(t *testing.T) {
	engine := scheduler.New("node", &scheduler.Coordinator{})
	engine.Elect()
	server := New(engine, "node", "master", observability.NewMetrics())
	server.Auth = auth.New(map[string]auth.Principal{"viewer-a": {Subject: "viewer", Tenant: "tenant-a", Role: "viewer"}, "operator-a": {Subject: "operator", Tenant: "tenant-a", Role: "operator"}, "operator-b": {Subject: "operator", Tenant: "tenant-b", Role: "operator"}})
	handler := server.Handler()
	request := func(method, path, token, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}
	if rec := request(http.MethodPost, "/api/v1/tasks", "viewer-a", `{"task_name":"denied","role":"default"}`); rec.Code != http.StatusForbidden {
		t.Fatalf("viewer write status=%d", rec.Code)
	}
	rec := request(http.MethodPost, "/api/v1/tasks", "operator-a", `{"task_id":"tenant-task","task_name":"allowed","role":"default"}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("operator create=%d %s", rec.Code, rec.Body.String())
	}
	if rec = request(http.MethodGet, "/api/v1/tasks", "operator-b", ""); rec.Code != http.StatusOK || bytes.Contains(rec.Body.Bytes(), []byte("tenant-task")) {
		t.Fatalf("tenant-b list leaked: %d %s", rec.Code, rec.Body.String())
	}
	if rec = request(http.MethodGet, "/api/v1/tasks/tenant-task", "operator-b", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("tenant-b detail status=%d", rec.Code)
	}
	if rec = request(http.MethodGet, "/api/v1/tasks/tenant-task", "viewer-a", ""); rec.Code != http.StatusOK || !bytes.Contains(rec.Body.Bytes(), []byte("allowed")) {
		t.Fatalf("tenant-a detail=%d %s", rec.Code, rec.Body.String())
	}
	if rec = request(http.MethodPost, "/api/v1/tasks/bulk", "operator-a", `{"task_ids":["tenant-task","missing"],"action":"pause"}`); rec.Code != http.StatusMultiStatus || !bytes.Contains(rec.Body.Bytes(), []byte(`"ok":true`)) || !bytes.Contains(rec.Body.Bytes(), []byte(`"ok":false`)) {
		t.Fatalf("bulk=%d %s", rec.Code, rec.Body.String())
	}
}

func TestHTTPErrorBoundaries(t *testing.T) {
	engine := scheduler.New("node", &scheduler.Coordinator{})
	engine.Elect()
	server := New(engine, "node", "master", observability.NewMetrics())
	handler := server.Handler()
	cases := []struct {
		method, path, body string
		want               int
	}{{http.MethodPut, "/api/v1/tasks", "", http.StatusMethodNotAllowed}, {http.MethodPost, "/api/v1/tasks", "{", http.StatusBadRequest}, {http.MethodPost, "/api/v1/tasks/bulk", `{"task_ids":[],"action":"pause"}`, http.StatusBadRequest}, {http.MethodPost, "/api/v1/tasks/bulk", `{"task_ids":["x"],"action":"explode"}`, http.StatusMultiStatus}, {http.MethodGet, "/api/v1/tasks/missing", "", http.StatusNotFound}}
	for _, tc := range cases {
		req := httptest.NewRequest(tc.method, tc.path, bytes.NewBufferString(tc.body))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != tc.want {
			t.Errorf("%s %s status=%d want=%d body=%s", tc.method, tc.path, rec.Code, tc.want, rec.Body.String())
		}
	}
}

func TestRequestIDAndTimeoutBoundaries(t *testing.T) {
	engine := scheduler.New("node", &scheduler.Coordinator{})
	engine.Elect()
	server := New(engine, "node", "master", observability.NewMetrics())
	server.MaxTaskTimeout = time.Second
	handler := server.Handler()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set("X-Request-ID", "trace-123")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Header().Get("X-Request-ID") != "trace-123" {
		t.Fatalf("request id = %q", rec.Header().Get("X-Request-ID"))
	}
	req = httptest.NewRequest(http.MethodPost, "/api/v1/tasks", bytes.NewBufferString(`{"task_name":"slow","role":"default","timeout_ms":2000}`))
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("timeout status=%d", rec.Code)
	}
}
