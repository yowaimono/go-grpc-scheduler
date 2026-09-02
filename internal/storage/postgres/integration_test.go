package postgres

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/yowaimono/go-grpc-scheduler/internal/scheduler"
)

func TestPostgresLeaderFailoverAndTaskClaim(t *testing.T) {
	dsn := os.Getenv("SCHEDULER_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set SCHEDULER_TEST_DATABASE_URL to run PostgreSQL integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	a, err := Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if err := a.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Pool.Exec(ctx, "TRUNCATE scheduler_leader, scheduler_tasks, scheduler_workers"); err != nil {
		t.Fatal(err)
	}
	b, err := Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	first, epoch1, err := a.TryAcquireLeader(ctx, "integration", "node-a", 5*time.Second)
	if err != nil || !first || epoch1 != 1 {
		t.Fatalf("first election = %v, epoch=%d, err=%v", first, epoch1, err)
	}
	second, _, err := b.TryAcquireLeader(ctx, "integration", "node-b", 5*time.Second)
	if err != nil || second {
		t.Fatalf("split brain election = %v, err=%v", second, err)
	}
	if ok, err := a.HeartbeatLeader(ctx, "integration", "node-a", epoch1, 5*time.Second); err != nil || !ok {
		t.Fatalf("heartbeat = %v, %v", ok, err)
	}
	if _, err := a.Pool.Exec(ctx, "UPDATE scheduler_leader SET lease_until=NOW()-INTERVAL '1 second' WHERE cluster_id='integration'"); err != nil {
		t.Fatal(err)
	}
	taken, epoch2, err := b.TryAcquireLeader(ctx, "integration", "node-b", 5*time.Second)
	if err != nil || !taken || epoch2 != 2 {
		t.Fatalf("takeover = %v, epoch=%d, err=%v", taken, epoch2, err)
	}
	if ok, err := a.HeartbeatLeader(ctx, "integration", "node-a", epoch1, 5*time.Second); err != nil || ok {
		t.Fatalf("old leader heartbeat = %v, %v", ok, err)
	}
	params := map[string]json.RawMessage{"message": json.RawMessage(`"hello"`)}
	if err := b.UpsertTask(ctx, scheduler.Task{ID: "integration-task", Name: "demo", Role: "default", Params: params, RunAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if ok, err := b.ClaimTask(ctx, "integration-task", "worker-a", epoch2, 30*time.Second); err != nil || !ok {
		t.Fatalf("claim = %v, %v", ok, err)
	}
	if ok, err := b.ClaimTask(ctx, "integration-task", "worker-b", epoch2, 30*time.Second); err != nil || ok {
		t.Fatalf("double claim = %v, %v", ok, err)
	}
	if err := b.CompleteTask(ctx, "integration-task", "worker-a", true); err != nil {
		t.Fatal(err)
	}
	if err := b.UpdateTaskState(ctx, "integration-task", "CANCELED", "integration audit"); err == nil {
		t.Fatal("expected terminal transition to fail")
	}
	if _, err := b.Pool.Exec(ctx, `INSERT INTO scheduler_task_history(task_id,from_status,to_status,reason) VALUES('integration-task','SUCCEEDED','CANCELED','audit check')`); err != nil {
		t.Fatal(err)
	}
	history, err := b.LoadTaskHistory(ctx, "integration-task")
	if err != nil || len(history) == 0 {
		t.Fatalf("history = %v, %v", history, err)
	}
	if err := b.AppendAudit(ctx, scheduler.AuditRecord{TenantID: "tenant-a", ActorID: "operator", Action: "task.inspect", ResourceType: "task", ResourceID: "integration-task", TaskID: "integration-task", Epoch: epoch2, Metadata: map[string]any{"source": "integration"}}); err != nil {
		t.Fatal(err)
	}
	var auditCount int
	if err := b.Pool.QueryRow(ctx, `SELECT COUNT(*) FROM scheduler_audit_log WHERE tenant_id='tenant-a' AND task_id='integration-task'`).Scan(&auditCount); err != nil {
		t.Fatal(err)
	}
	if auditCount != 1 {
		t.Fatalf("audit rows=%d", auditCount)
	}
	if err := b.RecordAttempt(ctx, "integration-task", 1, "worker-a", "SUCCEEDED", "", []byte("ok"), epoch2); err != nil {
		t.Fatal(err)
	}
	attempts, err := b.LoadTaskAttempts(ctx, "integration-task")
	if err != nil || len(attempts) != 1 || attempts[0].Status != "SUCCEEDED" {
		t.Fatalf("attempts=%#v err=%v", attempts, err)
	}
}
