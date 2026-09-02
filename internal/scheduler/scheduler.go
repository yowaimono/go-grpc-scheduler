package scheduler

import (
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yowaimono/go-grpc-scheduler/internal/observability"
	"github.com/yowaimono/go-grpc-scheduler/internal/timewheel"
)

var ErrNotLeader = errors.New("scheduler is not master")
var ErrNoWorker = errors.New("no worker with required role")

type Role uint8

const (
	RoleSlave Role = iota
	RoleMaster
)

type Task struct {
	ID            string
	Name          string
	Role          string
	Payload       []byte
	Params        map[string]json.RawMessage
	Priority      int
	RunAt         time.Time
	ScheduleType  string
	Interval      time.Duration
	Attempts      int
	LeaseUntil    time.Time
	Version       int64
	OwnerWorkerID string
	Status        string
	MaxAttempts   int
	RetryBackoff  time.Duration
	Timeout       time.Duration
	TenantID      string
	CronExpr      string
	Timezone      string
}

type Worker struct {
	ID       string
	Roles    map[string]struct{}
	Slots    int
	InFlight int
}

type Repository interface {
	UpsertTask(context.Context, Task) error
	LoadPending(context.Context, time.Time) ([]Task, error)
	ClaimTask(context.Context, string, string, int64, time.Duration) (bool, error)
	CompleteTask(context.Context, string, string, bool) error
}

type StateRepository interface {
	UpdateTaskState(context.Context, string, string, string) error
}

type AttemptRepository interface {
	RecordAttempt(context.Context, string, int, string, string, string, []byte, int64) error
}
type AuditRepository interface {
	AppendAudit(context.Context, AuditRecord) error
}
type AuditRecord struct {
	TenantID, ActorID, Action, ResourceType, ResourceID, RequestID, TaskID, WorkerID string
	Epoch                                                                            int64
	Metadata                                                                         map[string]any
}
type AttemptView struct {
	Attempt    int        `json:"attempt"`
	WorkerID   string     `json:"worker_id,omitempty"`
	Status     string     `json:"status"`
	StartedAt  time.Time  `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	Error      string     `json:"error,omitempty"`
	Epoch      int64      `json:"epoch"`
}
type HistoryRepository interface {
	LoadTaskHistory(context.Context, string) ([]Event, error)
	LoadTaskAttempts(context.Context, string) ([]AttemptView, error)
}

type RecurringRepository interface {
	RescheduleTask(context.Context, Task, string) error
}

type Coordinator struct{ leader atomic.Pointer[string] }

func (c *Coordinator) TryAcquire(id string) bool {
	current := c.leader.Load()
	if current != nil {
		return *current == id
	}
	n := id
	return c.leader.CompareAndSwap(nil, &n)
}
func (c *Coordinator) Leader() string {
	if p := c.leader.Load(); p != nil {
		return *p
	}
	return ""
}

type taskHeap []*Task

func (h taskHeap) Len() int { return len(h) }
func (h taskHeap) Less(i, j int) bool {
	if h[i].RunAt.Equal(h[j].RunAt) {
		return h[i].Priority > h[j].Priority
	}
	return h[i].RunAt.Before(h[j].RunAt)
}
func (h taskHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *taskHeap) Push(x any)   { *h = append(*h, x.(*Task)) }
func (h *taskHeap) Pop() any     { old := *h; n := len(old); x := old[n-1]; *h = old[:n-1]; return x }

type Engine struct {
	mu                  sync.Mutex
	id                  string
	role                atomic.Uint32
	epoch               int64
	coord               *Coordinator
	workers             map[string]*Worker
	tasks               map[string]*Task
	ready               taskHeap
	inFlight            map[string]*Task
	wheel               *timewheel.Wheel
	repo                Repository
	metrics             *observability.Metrics
	events              []Event
	draining            atomic.Bool
	submittedTotal      atomic.Int64
	assignedTotal       atomic.Int64
	succeededTotal      atomic.Int64
	failedTotal         atomic.Int64
	requeuedTotal       atomic.Int64
	lagTotalNanos       atomic.Int64
	lagSamples          atomic.Int64
	dashboard           []DashboardSnapshot
	lastDashboardSample time.Time
}

const schedulerTick = 100 * time.Millisecond

func (e *Engine) WithRepository(repo Repository) *Engine             { e.repo = repo; return e }
func (e *Engine) WithMetrics(metrics *observability.Metrics) *Engine { e.metrics = metrics; return e }
func (e *Engine) Audit(ctx context.Context, record AuditRecord) error {
	if repo, ok := e.repo.(AuditRepository); ok {
		record.Epoch = e.epoch
		return repo.AppendAudit(ctx, record)
	}
	return nil
}

func New(id string, coord *Coordinator) *Engine {
	now := time.Now()
	e := &Engine{id: id, coord: coord, workers: map[string]*Worker{}, tasks: map[string]*Task{}, inFlight: map[string]*Task{}, wheel: timewheel.New(schedulerTick, 36000, now)}
	heap.Init(&e.ready)
	return e
}
func (e *Engine) ID() string { return e.id }
func (e *Engine) Elect() Role {
	if e.coord.TryAcquire(e.id) {
		e.role.Store(uint32(RoleMaster))
	} else {
		e.role.Store(uint32(RoleSlave))
	}
	return Role(e.role.Load())
}
func (e *Engine) Role() Role { return Role(e.role.Load()) }
func (e *Engine) SetRole(role Role) {
	e.role.Store(uint32(role))
	if e.metrics != nil {
		if role == RoleMaster {
			e.metrics.LeaderState.Set(1)
		} else {
			e.metrics.LeaderState.Set(0)
		}
	}
}
func (e *Engine) SetEpoch(epoch int64) { e.mu.Lock(); e.epoch = epoch; e.mu.Unlock() }
func (e *Engine) BeginDrain()          { e.draining.Store(true) }
func (e *Engine) IsDraining() bool     { return e.draining.Load() }

func (e *Engine) RegisterWorker(id string, roles []string, slots int) {
	if slots < 1 {
		slots = 1
	}
	roleSet := make(map[string]struct{}, len(roles))
	for _, role := range roles {
		roleSet[role] = struct{}{}
	}
	e.mu.Lock()
	e.workers[id] = &Worker{ID: id, Roles: roleSet, Slots: slots}
	if e.metrics != nil {
		e.metrics.WorkersOnline.Set(float64(len(e.workers)))
	}
	e.mu.Unlock()
}

func (e *Engine) Submit(ctx context.Context, task Task) error {
	if e.draining.Load() {
		return errors.New("scheduler is draining")
	}
	if e.Role() != RoleMaster {
		return ErrNotLeader
	}
	if task.ID == "" {
		return errors.New("task id is required")
	}
	if task.RunAt.IsZero() {
		task.RunAt = time.Now()
	}
	if task.Status == "" {
		task.Status = "PENDING"
	}
	if task.TenantID == "" {
		task.TenantID = "local"
	}
	if task.MaxAttempts < 1 {
		task.MaxAttempts = 3
	}
	if task.RetryBackoff <= 0 {
		task.RetryBackoff = time.Second
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, exists := e.tasks[task.ID]; exists {
		return fmt.Errorf("task %q already exists", task.ID)
	}
	if e.repo != nil {
		if err := e.repo.UpsertTask(ctx, task); err != nil {
			return err
		}
	}
	task.Version = 1
	e.tasks[task.ID] = &task
	e.recordEventLocked(Event{Type: "task.submitted", TaskID: task.ID, Role: task.Role})
	e.submittedTotal.Add(1)
	if e.metrics != nil {
		e.metrics.TasksSubmitted.Inc()
	}
	if !task.RunAt.After(time.Now()) {
		heap.Push(&e.ready, &task)
	} else {
		e.wheel.Add(&timewheel.Entry{Key: task.ID, Due: task.RunAt, Version: task.Version})
	}
	return nil
}

func (e *Engine) advanceLocked(now time.Time) {
	for _, entry := range e.wheel.Advance(now) {
		if task := e.tasks[entry.Key]; task != nil && task.Version == entry.Version && task.LeaseUntil.IsZero() && (task.Status == "PENDING" || task.Status == "RETRY_WAIT") {
			heap.Push(&e.ready, task)
		}
	}
}

func supports(w *Worker, role string) bool { _, ok := w.Roles[role]; return ok }

func (e *Engine) Assign(workerID string, now time.Time) (*Task, error) {
	return e.AssignContext(context.Background(), workerID, now)
}

func (e *Engine) AssignContext(ctx context.Context, workerID string, now time.Time) (*Task, error) {
	if e.draining.Load() {
		return nil, errors.New("scheduler is draining")
	}
	if e.Role() != RoleMaster {
		return nil, ErrNotLeader
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.advanceLocked(now)
	w, ok := e.workers[workerID]
	if !ok || w.InFlight >= w.Slots {
		return nil, ErrNoWorker
	}
	var skipped taskHeap
	for e.ready.Len() > 0 {
		t := heap.Pop(&e.ready).(*Task)
		if supports(w, t.Role) && (t.Status == "PENDING" || t.Status == "RETRY_WAIT") && (t.LeaseUntil.IsZero() || !now.Before(t.LeaseUntil)) {
			if e.repo != nil {
				claimed, err := e.repo.ClaimTask(ctx, t.ID, workerID, e.epoch, 30*time.Second)
				if err != nil || !claimed {
					skipped = append(skipped, t)
					continue
				}
			}
			t.Attempts++
			t.Status = "RUNNING"
			t.LeaseUntil = now.Add(30 * time.Second)
			t.OwnerWorkerID = workerID
			e.recordEventLocked(Event{Type: "task.assigned", TaskID: t.ID, WorkerID: workerID, Role: t.Role})
			e.assignedTotal.Add(1)
			lag := now.Sub(t.RunAt)
			if lag > 0 {
				e.lagTotalNanos.Add(lag.Nanoseconds())
				e.lagSamples.Add(1)
			}
			if e.metrics != nil {
				e.metrics.TasksAssigned.Inc()
				e.metrics.ScheduleLag.Observe(time.Since(t.RunAt).Seconds())
			}
			e.inFlight[t.ID] = t
			w.InFlight++
			for _, s := range skipped {
				heap.Push(&e.ready, s)
			}
			return t, nil
		}
		skipped = append(skipped, t)
	}
	for _, s := range skipped {
		heap.Push(&e.ready, s)
	}
	return nil, ErrNoWorker
}

func (e *Engine) Complete(workerID, taskID string, success bool) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	t, ok := e.inFlight[taskID]
	if !ok {
		return fmt.Errorf("task %q is not leased", taskID)
	}
	if t.OwnerWorkerID != workerID {
		return fmt.Errorf("task %q is leased by worker %q", taskID, t.OwnerWorkerID)
	}
	delete(e.inFlight, taskID)
	if w := e.workers[workerID]; w != nil && w.InFlight > 0 {
		w.InFlight--
	}
	t.OwnerWorkerID = ""
	if success {
		if repo, ok := e.repo.(AttemptRepository); ok {
			_ = repo.RecordAttempt(context.Background(), taskID, t.Attempts, workerID, "SUCCEEDED", "", nil, e.epoch)
		}
		if e.metrics != nil {
			e.metrics.TasksSucceeded.Inc()
		}
		e.recordEventLocked(Event{Type: "task.succeeded", TaskID: taskID, WorkerID: workerID, Role: t.Role})
		e.succeededTotal.Add(1)
		if (t.ScheduleType == "INTERVAL" && t.Interval > 0) || t.ScheduleType == "CRON" {
			t.Status = "PENDING"
			t.LeaseUntil = time.Time{}
			t.OwnerWorkerID = ""
			if t.ScheduleType == "INTERVAL" {
				t.RunAt = t.RunAt.Add(t.Interval)
			} else if next, err := NextCron(t.CronExpr, t.Timezone, t.RunAt); err == nil {
				t.RunAt = next
			} else {
				t.Status = "FAILED"
			}
			t.Version++
			e.tasks[taskID] = t
			if repo, ok := e.repo.(RecurringRepository); ok {
				_ = repo.RescheduleTask(context.Background(), *t, workerID)
			}
			e.wheel.Add(&timewheel.Entry{Key: taskID, Due: t.RunAt, Version: t.Version})
			return nil
		}
		if e.repo != nil {
			_ = e.repo.CompleteTask(context.Background(), taskID, workerID, true)
		}
		delete(e.tasks, taskID)
		return nil
	}
	if t.Attempts >= t.MaxAttempts {
		t.Status = "FAILED"
		if repo, ok := e.repo.(StateRepository); ok {
			_ = repo.UpdateTaskState(context.Background(), taskID, "FAILED", "max attempts reached")
		}
		e.tasks[taskID] = t
		if repo, ok := e.repo.(AttemptRepository); ok {
			_ = repo.RecordAttempt(context.Background(), taskID, t.Attempts, workerID, "FAILED", "max attempts reached", nil, e.epoch)
		}
		return nil
	}
	t.Status = "RETRY_WAIT"
	e.failedTotal.Add(1)
	e.requeuedTotal.Add(1)
	if repo, ok := e.repo.(AttemptRepository); ok {
		_ = repo.RecordAttempt(context.Background(), taskID, t.Attempts, workerID, "FAILED", "task returned failure", nil, e.epoch)
	}
	t.LeaseUntil = time.Time{}
	t.Version++
	if e.metrics != nil {
		e.metrics.TasksFailed.Inc()
		e.metrics.TasksRequeued.Inc()
	}
	e.recordEventLocked(Event{Type: "task.failed", TaskID: taskID, WorkerID: workerID, Role: t.Role})
	if stateRepo, ok := e.repo.(StateRepository); ok {
		_ = stateRepo.UpdateTaskState(context.Background(), taskID, "RETRY_WAIT", "task returned failure")
	} else if e.repo != nil {
		_ = e.repo.CompleteTask(context.Background(), taskID, workerID, false)
	}
	e.tasks[taskID] = t
	e.wheel.Add(&timewheel.Entry{Key: taskID, Due: time.Now().Add(t.RetryBackoff), Version: t.Version})
	return nil
}

func (e *Engine) RequeueExpired(now time.Time) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	n := 0
	for id, t := range e.inFlight {
		if now.Before(t.LeaseUntil) {
			continue
		}
		delete(e.inFlight, id)
		if w := e.workers[t.OwnerWorkerID]; w != nil && w.InFlight > 0 {
			w.InFlight--
		}
		t.LeaseUntil = time.Time{}
		t.OwnerWorkerID = ""
		t.Status = "RETRY_WAIT"
		e.requeuedTotal.Add(1)
		t.Version++
		if e.metrics != nil {
			e.metrics.TasksRequeued.Inc()
		}
		e.recordEventLocked(Event{Type: "task.lease_expired", TaskID: id, WorkerID: t.OwnerWorkerID, Role: t.Role})
		e.wheel.Add(&timewheel.Entry{Key: id, Due: now, Version: t.Version})
		n++
	}
	return n
}

func (e *Engine) ReadyLen() int { e.mu.Lock(); defer e.mu.Unlock(); return e.ready.Len() }
func (e *Engine) Tick(now time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.Role() == RoleMaster {
		e.advanceLocked(now)
	}
	if e.metrics != nil {
		e.metrics.QueueDepth.Set(float64(e.ready.Len()))
		e.metrics.WorkersOnline.Set(float64(len(e.workers)))
	}
	e.sampleDashboardLocked(now)
}
func (e *Engine) WorkerCount() int { e.mu.Lock(); defer e.mu.Unlock(); return len(e.workers) }

func (e *Engine) Sync(ctx context.Context, since time.Time) error {
	if e.repo == nil {
		return nil
	}
	tasks, err := e.repo.LoadPending(ctx, since)
	if err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, t := range tasks {
		if t.Status == "" {
			t.Status = "PENDING"
		}
		if existing := e.tasks[t.ID]; existing != nil && existing.Version >= t.Version {
			continue
		}
		e.tasks[t.ID] = &t
		if !t.RunAt.After(time.Now()) {
			heap.Push(&e.ready, &t)
		} else {
			e.wheel.Add(&timewheel.Entry{Key: t.ID, Due: t.RunAt, Version: t.Version})
		}
	}
	return nil
}

type WorkerView struct {
	WorkerID string   `json:"worker_id"`
	Roles    []string `json:"roles"`
	Slots    int      `json:"slots"`
	InFlight int      `json:"in_flight"`
}

type DashboardSnapshot struct {
	At                   time.Time `json:"at"`
	QueueDepth           int       `json:"queue_depth"`
	Running              int       `json:"running"`
	Pending              int       `json:"pending"`
	RetryWait            int       `json:"retry_wait"`
	Paused               int       `json:"paused"`
	Failed               int       `json:"failed"`
	WorkersOnline        int       `json:"workers_online"`
	WorkerSlots          int       `json:"worker_slots"`
	WorkerInFlight       int       `json:"worker_in_flight"`
	SubmittedTotal       int64     `json:"submitted_total"`
	AssignedTotal        int64     `json:"assigned_total"`
	SucceededTotal       int64     `json:"succeeded_total"`
	FailedTotal          int64     `json:"failed_total"`
	RequeuedTotal        int64     `json:"requeued_total"`
	AverageScheduleLagMS float64   `json:"average_schedule_lag_ms"`
}
type DashboardReport struct {
	Current DashboardSnapshot   `json:"current"`
	History []DashboardSnapshot `json:"history"`
}

func (e *Engine) snapshotDashboardLocked(now time.Time) DashboardSnapshot {
	s := DashboardSnapshot{At: now, QueueDepth: e.ready.Len(), WorkersOnline: len(e.workers), SubmittedTotal: e.submittedTotal.Load(), AssignedTotal: e.assignedTotal.Load(), SucceededTotal: e.succeededTotal.Load(), FailedTotal: e.failedTotal.Load(), RequeuedTotal: e.requeuedTotal.Load()}
	for _, task := range e.tasks {
		switch task.Status {
		case "RUNNING":
			s.Running++
		case "PENDING":
			s.Pending++
		case "RETRY_WAIT":
			s.RetryWait++
		case "PAUSED":
			s.Paused++
		case "FAILED":
			s.Failed++
		}
	}
	for _, worker := range e.workers {
		s.WorkerSlots += worker.Slots
		s.WorkerInFlight += worker.InFlight
	}
	if samples := e.lagSamples.Load(); samples > 0 {
		s.AverageScheduleLagMS = float64(e.lagTotalNanos.Load()) / float64(samples) / float64(time.Millisecond)
	}
	return s
}
func (e *Engine) sampleDashboardLocked(now time.Time) {
	if !e.lastDashboardSample.IsZero() && now.Sub(e.lastDashboardSample) < time.Second {
		return
	}
	e.lastDashboardSample = now
	e.dashboard = append(e.dashboard, e.snapshotDashboardLocked(now))
	if len(e.dashboard) > 900 {
		e.dashboard = e.dashboard[len(e.dashboard)-900:]
	}
}
func (e *Engine) Dashboard() DashboardReport {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := time.Now()
	current := e.snapshotDashboardLocked(now)
	history := make([]DashboardSnapshot, len(e.dashboard))
	copy(history, e.dashboard)
	return DashboardReport{Current: current, History: history}
}

type TaskView struct {
	TaskID       string    `json:"task_id"`
	TaskName     string    `json:"task_name"`
	RequiredRole string    `json:"required_role"`
	Status       string    `json:"status"`
	NextRunAt    time.Time `json:"next_run_at"`
	Priority     int       `json:"priority"`
	TenantID     string    `json:"tenant_id"`
	ScheduleType string    `json:"schedule_type,omitempty"`
	IntervalMS   int64     `json:"interval_ms,omitempty"`
	CronExpr     string    `json:"cron_expr,omitempty"`
	Timezone     string    `json:"timezone,omitempty"`
	Attempts     int       `json:"attempts"`
	MaxAttempts  int       `json:"max_attempts"`
	TimeoutMS    int64     `json:"timeout_ms,omitempty"`
}

func (e *Engine) SnapshotWorkers() []WorkerView {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]WorkerView, 0, len(e.workers))
	for _, w := range e.workers {
		roles := make([]string, 0, len(w.Roles))
		for role := range w.Roles {
			roles = append(roles, role)
		}
		out = append(out, WorkerView{WorkerID: w.ID, Roles: roles, Slots: w.Slots, InFlight: w.InFlight})
	}
	return out
}

func (e *Engine) SnapshotTasks() []TaskView {
	return e.snapshotTasks("")
}

func (e *Engine) SnapshotTasksForTenant(tenant string) []TaskView { return e.snapshotTasks(tenant) }
func (e *Engine) snapshotTasks(tenant string) []TaskView {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]TaskView, 0, len(e.tasks))
	for _, t := range e.tasks {
		if tenant != "" && t.TenantID != tenant {
			continue
		}
		status := t.Status
		if status == "" {
			status = "PENDING"
		}
		out = append(out, TaskView{TaskID: t.ID, TaskName: t.Name, RequiredRole: t.Role, Status: status, NextRunAt: t.RunAt, Priority: t.Priority, TenantID: t.TenantID, ScheduleType: t.ScheduleType, IntervalMS: t.Interval.Milliseconds(), CronExpr: t.CronExpr, Timezone: t.Timezone, Attempts: t.Attempts, MaxAttempts: t.MaxAttempts, TimeoutMS: t.Timeout.Milliseconds()})
	}
	return out
}

func (e *Engine) TaskTenant(taskID string) (string, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	t, ok := e.tasks[taskID]
	if !ok {
		return "", false
	}
	return t.TenantID, true
}

func (e *Engine) SnapshotTask(taskID string) (TaskView, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	t, ok := e.tasks[taskID]
	if !ok {
		return TaskView{}, false
	}
	status := t.Status
	if status == "" {
		status = "PENDING"
	}
	return TaskView{TaskID: t.ID, TaskName: t.Name, RequiredRole: t.Role, Status: status, NextRunAt: t.RunAt, Priority: t.Priority, TenantID: t.TenantID, ScheduleType: t.ScheduleType, IntervalMS: t.Interval.Milliseconds(), CronExpr: t.CronExpr, Timezone: t.Timezone, Attempts: t.Attempts, MaxAttempts: t.MaxAttempts, TimeoutMS: t.Timeout.Milliseconds()}, true
}

func (e *Engine) transition(ctx context.Context, taskID, target, reason string) error {
	if e.Role() != RoleMaster {
		return ErrNotLeader
	}
	e.mu.Lock()
	t, ok := e.tasks[taskID]
	if !ok {
		e.mu.Unlock()
		return fmt.Errorf("task %q not found", taskID)
	}
	valid := false
	switch target {
	case "PAUSED":
		valid = t.Status == "PENDING" || t.Status == "RETRY_WAIT"
	case "PENDING":
		valid = t.Status == "PAUSED" || t.Status == "FAILED"
	case "CANCELED":
		valid = t.Status != "SUCCEEDED" && t.Status != "CANCELED"
	}
	if !valid {
		e.mu.Unlock()
		return fmt.Errorf("invalid transition %s -> %s", t.Status, target)
	}
	if target == "CANCELED" && t.OwnerWorkerID != "" {
		delete(e.inFlight, taskID)
		if worker := e.workers[t.OwnerWorkerID]; worker != nil && worker.InFlight > 0 {
			worker.InFlight--
		}
	}
	t.Status = target
	t.Version++
	t.LeaseUntil = time.Time{}
	t.OwnerWorkerID = ""
	e.tasks[taskID] = t
	e.recordEventLocked(Event{Type: "task." + strings.ToLower(target), TaskID: taskID, Role: t.Role})
	if target == "PENDING" {
		e.wheel.Add(&timewheel.Entry{Key: taskID, Due: time.Now(), Version: t.Version})
	}
	repo := e.repo
	e.mu.Unlock()
	if stateRepo, ok := repo.(StateRepository); ok {
		return stateRepo.UpdateTaskState(ctx, taskID, target, reason)
	}
	return nil
}

func (e *Engine) Pause(ctx context.Context, taskID string) error {
	return e.transition(ctx, taskID, "PAUSED", "paused by operator")
}
func (e *Engine) Resume(ctx context.Context, taskID string) error {
	return e.transition(ctx, taskID, "PENDING", "resumed by operator")
}
func (e *Engine) Cancel(ctx context.Context, taskID string) error {
	return e.transition(ctx, taskID, "CANCELED", "canceled by operator")
}
func (e *Engine) Retry(ctx context.Context, taskID string) error {
	return e.transition(ctx, taskID, "PENDING", "manual retry")
}
func (e *Engine) TaskHistory(taskID string) []Event {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]Event, 0)
	for _, event := range e.events {
		if event.TaskID == taskID {
			out = append(out, event)
		}
	}
	return out
}

func (e *Engine) TaskHistoryContext(ctx context.Context, taskID string) ([]Event, error) {
	if repo, ok := e.repo.(HistoryRepository); ok {
		return repo.LoadTaskHistory(ctx, taskID)
	}
	return e.TaskHistory(taskID), nil
}
func (e *Engine) TaskAttemptsContext(ctx context.Context, taskID string) ([]AttemptView, error) {
	if repo, ok := e.repo.(HistoryRepository); ok {
		return repo.LoadTaskAttempts(ctx, taskID)
	}
	return nil, nil
}

type Event struct {
	At       time.Time `json:"at"`
	Type     string    `json:"type"`
	TaskID   string    `json:"task_id,omitempty"`
	WorkerID string    `json:"worker_id,omitempty"`
	Role     string    `json:"role,omitempty"`
	Epoch    int64     `json:"epoch"`
}

func (e *Engine) recordEventLocked(event Event) {
	event.At = time.Now()
	event.Epoch = e.epoch
	e.events = append(e.events, event)
	if len(e.events) > 200 {
		e.events = e.events[len(e.events)-200:]
	}
}
func (e *Engine) SnapshotEvents() []Event {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]Event, len(e.events))
	copy(out, e.events)
	return out
}
