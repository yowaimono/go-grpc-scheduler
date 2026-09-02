package scheduler

import (
	"context"
	"fmt"
	"testing"
	"time"
)

type fakeRepo struct {
	tasks     []Task
	claims    int
	completed int
	states    []string
}

func (f *fakeRepo) UpsertTask(context.Context, Task) error                 { return nil }
func (f *fakeRepo) LoadPending(context.Context, time.Time) ([]Task, error) { return f.tasks, nil }
func (f *fakeRepo) ClaimTask(context.Context, string, string, int64, time.Duration) (bool, error) {
	f.claims++
	return true, nil
}
func (f *fakeRepo) CompleteTask(context.Context, string, string, bool) error {
	f.completed++
	return nil
}
func (f *fakeRepo) UpdateTaskState(_ context.Context, _ string, status, _ string) error {
	f.states = append(f.states, status)
	return nil
}

func TestMasterSlaveElectionAndLease(t *testing.T) {
	coord := &Coordinator{}
	master := New("master-a", coord)
	slave := New("slave-b", coord)
	if got := master.Elect(); got != RoleMaster {
		t.Fatalf("master role = %v", got)
	}
	if got := slave.Elect(); got != RoleSlave {
		t.Fatalf("slave role = %v", got)
	}
	master.RegisterWorker("worker-1", []string{"gpu"}, 1)
	if err := slave.Submit(context.Background(), Task{ID: "rejected"}); err != ErrNotLeader {
		t.Fatalf("slave submit error = %v", err)
	}
	if err := master.Submit(context.Background(), Task{ID: "t-1", Role: "gpu", Priority: 10}); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	task, err := master.Assign("worker-1", now)
	if err != nil || task.ID != "t-1" {
		t.Fatalf("assign = %#v, %v", task, err)
	}
	if got := master.RequeueExpired(now.Add(31 * time.Second)); got != 1 {
		t.Fatalf("requeued = %d", got)
	}
}

func TestRolePoolOnlyAssignsSupportedRole(t *testing.T) {
	engine := New("master", &Coordinator{})
	engine.Elect()
	engine.RegisterWorker("image-1", []string{"image"}, 1)
	engine.RegisterWorker("video-1", []string{"video"}, 1)
	if err := engine.Submit(context.Background(), Task{ID: "video-task", Role: "video", RunAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Assign("image-1", time.Now()); err == nil {
		t.Fatal("image worker accepted video task")
	}
	assigned, err := engine.Assign("video-1", time.Now())
	if err != nil || assigned.ID != "video-task" {
		t.Fatalf("assignment = %#v, %v", assigned, err)
	}
}

func TestWorkerCannotCompleteAnotherWorkersLease(t *testing.T) {
	engine := New("master", &Coordinator{})
	engine.Elect()
	engine.RegisterWorker("worker-a", []string{"default"}, 1)
	engine.RegisterWorker("worker-b", []string{"default"}, 1)
	if err := engine.Submit(context.Background(), Task{ID: "owned", Role: "default", RunAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	assigned, err := engine.Assign("worker-a", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Complete("worker-b", assigned.ID, true); err == nil {
		t.Fatal("wrong worker completed lease")
	}
	if _, err := engine.Assign("worker-b", time.Now()); err == nil {
		t.Fatal("wrong worker received task while owner active")
	}
	if err := engine.Complete("worker-a", assigned.ID, true); err != nil {
		t.Fatal(err)
	}
}

func TestRolePoolDistributesAcrossWorkers(t *testing.T) {
	engine := New("master", &Coordinator{})
	engine.Elect()
	engine.RegisterWorker("worker-a", []string{"default"}, 2)
	engine.RegisterWorker("worker-b", []string{"default"}, 2)
	for i := 0; i < 4; i++ {
		if err := engine.Submit(context.Background(), Task{ID: fmt.Sprintf("balanced-%d", i), Role: "default", RunAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	assignedA, err := engine.Assign("worker-a", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	assignedB, err := engine.Assign("worker-b", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if assignedA.ID == assignedB.ID {
		t.Fatal("same task assigned twice")
	}
	if err := engine.Complete("worker-a", assignedA.ID, true); err != nil {
		t.Fatal(err)
	}
	if err := engine.Complete("worker-b", assignedB.ID, true); err != nil {
		t.Fatal(err)
	}
	workers := engine.SnapshotWorkers()
	if len(workers) != 2 {
		t.Fatalf("workers=%d", len(workers))
	}
	if workers[0].InFlight != 0 || workers[1].InFlight != 0 {
		t.Fatalf("inflight not released: %#v", workers)
	}
}

func TestIntervalTaskIsScheduledAgain(t *testing.T) {
	engine := New("master", &Coordinator{})
	engine.Elect()
	engine.RegisterWorker("worker", []string{"default"}, 1)
	first := time.Now()
	if err := engine.Submit(context.Background(), Task{ID: "periodic", Name: "demo", Role: "default", RunAt: first, ScheduleType: "INTERVAL", Interval: time.Second}); err != nil {
		t.Fatal(err)
	}
	assigned, err := engine.Assign("worker", first)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Complete("worker", assigned.ID, true); err != nil {
		t.Fatal(err)
	}
	if engine.ReadyLen() != 0 {
		t.Fatal("periodic task should wait for next interval")
	}
	engine.Tick(first.Add(2 * time.Second))
	if engine.ReadyLen() != 1 {
		t.Fatalf("ready after interval = %d", engine.ReadyLen())
	}
}

func TestTaskStateTransitions(t *testing.T) {
	engine := New("master", &Coordinator{})
	engine.Elect()
	if err := engine.Submit(context.Background(), Task{ID: "state-task", Role: "default", RunAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := engine.Pause(context.Background(), "state-task"); err != nil {
		t.Fatal(err)
	}
	if err := engine.Resume(context.Background(), "state-task"); err != nil {
		t.Fatal(err)
	}
	if err := engine.Cancel(context.Background(), "state-task"); err != nil {
		t.Fatal(err)
	}
	if err := engine.Resume(context.Background(), "state-task"); err == nil {
		t.Fatal("canceled task resumed")
	}
	history := engine.TaskHistory("state-task")
	if len(history) < 3 {
		t.Fatalf("history events = %d", len(history))
	}
}

func TestTaskRetriesUntilMaxAttempts(t *testing.T) {
	engine := New("master", &Coordinator{})
	engine.Elect()
	engine.RegisterWorker("worker", []string{"default"}, 1)
	if err := engine.Submit(context.Background(), Task{ID: "retry-task", Role: "default", RunAt: time.Now(), MaxAttempts: 2, RetryBackoff: time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	first, err := engine.Assign("worker", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Complete("worker", first.ID, false); err != nil {
		t.Fatal(err)
	}
	engine.Tick(time.Now().Add(time.Second))
	second, err := engine.Assign("worker", time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Complete("worker", second.ID, false); err != nil {
		t.Fatal(err)
	}
	if got := engine.SnapshotTasks()[0].Status; got != "FAILED" {
		t.Fatalf("status = %s", got)
	}
}

func TestFailedTaskPersistsRetryWaitInsteadOfTerminalFailed(t *testing.T) {
	repo := &fakeRepo{}
	engine := New("master", &Coordinator{}).WithRepository(repo)
	engine.Elect()
	engine.RegisterWorker("worker", []string{"default"}, 1)
	if err := engine.Submit(context.Background(), Task{ID: "persist-retry", Role: "default", RunAt: time.Now(), MaxAttempts: 3, RetryBackoff: time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	assigned, err := engine.Assign("worker", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Complete("worker", assigned.ID, false); err != nil {
		t.Fatal(err)
	}
	if len(repo.states) != 1 || repo.states[0] != "RETRY_WAIT" {
		t.Fatalf("persisted states = %#v", repo.states)
	}
	if repo.completed != 0 {
		t.Fatalf("terminal completion called %d times", repo.completed)
	}
}

func TestBeginDrainStopsNewWork(t *testing.T) {
	engine := New("master", &Coordinator{})
	engine.Elect()
	engine.BeginDrain()
	if !engine.IsDraining() {
		t.Fatal("engine did not enter drain")
	}
	if err := engine.Submit(context.Background(), Task{ID: "blocked", Role: "default"}); err == nil {
		t.Fatal("submit accepted during drain")
	}
	if _, err := engine.Assign("missing", time.Now()); err == nil {
		t.Fatal("assign accepted during drain")
	}
}

func TestSyncRestoresPendingTasksFromRepository(t *testing.T) {
	now := time.Now()
	repo := &fakeRepo{tasks: []Task{{ID: "restore", Role: "default", RunAt: now}}}
	engine := New("master", &Coordinator{}).WithRepository(repo)
	engine.Elect()
	if err := engine.Sync(context.Background(), now.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	if engine.ReadyLen() != 1 {
		t.Fatalf("ready after sync = %d", engine.ReadyLen())
	}
	engine.RegisterWorker("worker", []string{"default"}, 1)
	assigned, err := engine.Assign("worker", now)
	if err != nil || assigned.ID != "restore" {
		t.Fatalf("assign after sync = %#v, %v", assigned, err)
	}
	if repo.claims != 1 {
		t.Fatalf("claims = %d", repo.claims)
	}
}

func TestIntervalOneSecondCadence(t *testing.T) {
	base := time.Now().Truncate(time.Second)
	engine := New("master", &Coordinator{})
	engine.Elect()
	engine.RegisterWorker("worker", []string{"default"}, 1)
	if err := engine.Submit(context.Background(), Task{ID: "one-second", Name: "tick", Role: "default", RunAt: base, ScheduleType: "INTERVAL", Interval: time.Second}); err != nil {
		t.Fatal(err)
	}
	observed := make([]time.Time, 0, 8)
	for i := 0; i < 8; i++ {
		now := base.Add(time.Duration(i) * time.Second)
		engine.Tick(now)
		assigned, err := engine.Assign("worker", now)
		if err != nil {
			t.Fatalf("run %d assign: %v", i, err)
		}
		observed = append(observed, now)
		if err := engine.Complete("worker", assigned.ID, true); err != nil {
			t.Fatal(err)
		}
	}
	for i := 1; i < len(observed); i++ {
		if got := observed[i].Sub(observed[i-1]); got != time.Second {
			t.Fatalf("interval %d = %v", i, got)
		}
	}
}

func TestDashboardTracksCountersAndStatusDistribution(t *testing.T) {
	base := time.Now()
	engine := New("master", &Coordinator{})
	engine.Elect()
	engine.RegisterWorker("worker", []string{"default"}, 2)
	if err := engine.Submit(context.Background(), Task{ID: "dash-1", Role: "default", RunAt: base}); err != nil {
		t.Fatal(err)
	}
	if err := engine.Submit(context.Background(), Task{ID: "dash-2", Role: "default", RunAt: base, ScheduleType: "INTERVAL", Interval: time.Second}); err != nil {
		t.Fatal(err)
	}
	assigned, err := engine.Assign("worker", base)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Complete("worker", assigned.ID, true); err != nil {
		t.Fatal(err)
	}
	engine.Tick(base.Add(time.Second))
	report := engine.Dashboard()
	if report.Current.SubmittedTotal != 2 {
		t.Fatalf("submitted=%d", report.Current.SubmittedTotal)
	}
	if report.Current.AssignedTotal != 1 {
		t.Fatalf("assigned=%d", report.Current.AssignedTotal)
	}
	if report.Current.SucceededTotal != 1 {
		t.Fatalf("succeeded=%d", report.Current.SucceededTotal)
	}
	if report.Current.WorkersOnline != 1 || report.Current.WorkerSlots != 2 {
		t.Fatalf("workers=%d slots=%d", report.Current.WorkersOnline, report.Current.WorkerSlots)
	}
	if len(report.History) == 0 {
		t.Fatal("dashboard history empty after tick")
	}
}
