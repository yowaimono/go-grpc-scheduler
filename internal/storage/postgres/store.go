package postgres

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yowaimono/go-grpc-scheduler/internal/scheduler"
)

//go:embed schema.sql
var schemaFS embed.FS

type TaskRecord struct {
	ID            string
	TenantID      string
	Name          string
	Role          string
	Params        []byte
	Payload       []byte
	NextRunAt     time.Time
	ScheduleType  string
	IntervalMS    int64
	CronExpr      string
	Timezone      string
	Priority      int
	Status        string
	OwnerWorkerID *string
	LeaseUntil    *time.Time
	Attempt       int
	Epoch         int64
	Version       int64
}

type Store struct{ Pool *pgxpool.Pool }

func Open(ctx context.Context, dsn string) (*Store, error) {
	if dsn == "" {
		return nil, errors.New("postgres dsn is required")
	}
	p, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err = p.Ping(ctx); err != nil {
		p.Close()
		return nil, err
	}
	return &Store{Pool: p}, nil
}

func (s *Store) Close() {
	if s != nil && s.Pool != nil {
		s.Pool.Close()
	}
}
func (s *Store) Migrate(ctx context.Context) error {
	b, err := schemaFS.ReadFile("schema.sql")
	if err != nil {
		return err
	}
	_, err = s.Pool.Exec(ctx, string(b))
	return err
}

func (s *Store) TryAcquireLeader(ctx context.Context, clusterID, nodeID string, lease time.Duration) (bool, int64, error) {
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, 0, err
	}
	defer tx.Rollback(ctx)
	var leader string
	var epoch int64
	var until time.Time
	err = tx.QueryRow(ctx, `SELECT leader_id, epoch, lease_until FROM scheduler_leader WHERE cluster_id=$1 FOR UPDATE`, clusterID).Scan(&leader, &epoch, &until)
	if errors.Is(err, pgx.ErrNoRows) {
		epoch = 1
		_, err = tx.Exec(ctx, `INSERT INTO scheduler_leader(cluster_id,leader_id,epoch,heartbeat_at,lease_until) VALUES($1,$2,$3,NOW(),NOW()+$4::interval)`, clusterID, nodeID, epoch, lease.String())
		if err != nil {
			return false, 0, err
		}
		if err = tx.Commit(ctx); err != nil {
			return false, 0, err
		}
		return true, epoch, nil
	}
	if err != nil {
		return false, 0, err
	}
	if leader != nodeID && time.Now().Before(until) {
		return false, epoch, tx.Commit(ctx)
	}
	if leader != nodeID {
		epoch++
	}
	_, err = tx.Exec(ctx, `UPDATE scheduler_leader SET leader_id=$2, epoch=$3, heartbeat_at=NOW(), lease_until=NOW()+$4::interval, updated_at=NOW() WHERE cluster_id=$1`, clusterID, nodeID, epoch, lease.String())
	if err != nil {
		return false, 0, err
	}
	err = tx.Commit(ctx)
	return true, epoch, err
}

func (s *Store) HeartbeatLeader(ctx context.Context, clusterID, nodeID string, epoch int64, lease time.Duration) (bool, error) {
	r, err := s.Pool.Exec(ctx, `UPDATE scheduler_leader SET heartbeat_at=NOW(), lease_until=NOW()+$4::interval, updated_at=NOW() WHERE cluster_id=$1 AND leader_id=$2 AND epoch=$3`, clusterID, nodeID, epoch, lease.String())
	return r.RowsAffected() == 1, err
}

func (s *Store) UpsertTask(ctx context.Context, t scheduler.Task) error {
	params, err := json.Marshal(t.Params)
	if err != nil {
		return err
	}
	_, err = s.Pool.Exec(ctx, `INSERT INTO scheduler_tasks(task_id,tenant_id,task_name,required_role,params,payload,schedule_type,interval_ms,cron_expr,timezone,next_run_at,priority,status,version) VALUES($1,$2,$3,$4,$5::jsonb,$6,$7,$8,$9,$10,$11,$12,'PENDING',$13)`, t.ID, t.TenantID, t.Name, t.Role, params, t.Payload, t.ScheduleType, t.Interval.Milliseconds(), t.CronExpr, t.Timezone, t.RunAt, t.Priority, max64(t.Version, 1))
	return err
}

func (s *Store) LoadPending(ctx context.Context, since time.Time) ([]scheduler.Task, error) {
	rows, err := s.Pool.Query(ctx, `SELECT task_id,tenant_id,task_name,required_role,params,payload,schedule_type,interval_ms,cron_expr,timezone,next_run_at,priority,status,owner_worker_id,lease_until,attempt,epoch,version FROM scheduler_tasks WHERE (status IN ('PENDING','RETRY_WAIT') OR (status='RUNNING' AND lease_until < NOW())) AND updated_at >= $1 ORDER BY next_run_at, priority DESC`, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []scheduler.Task
	for rows.Next() {
		var t TaskRecord
		if err := rows.Scan(&t.ID, &t.TenantID, &t.Name, &t.Role, &t.Params, &t.Payload, &t.ScheduleType, &t.IntervalMS, &t.CronExpr, &t.Timezone, &t.NextRunAt, &t.Priority, &t.Status, &t.OwnerWorkerID, &t.LeaseUntil, &t.Attempt, &t.Epoch, &t.Version); err != nil {
			return nil, err
		}
		var params map[string]json.RawMessage
		if err := json.Unmarshal(t.Params, &params); err != nil {
			return nil, err
		}
		out = append(out, scheduler.Task{ID: t.ID, Name: t.Name, Role: t.Role, Params: params, Payload: t.Payload, RunAt: t.NextRunAt, Priority: t.Priority, Attempts: t.Attempt, Version: t.Version, ScheduleType: t.ScheduleType, Interval: time.Duration(t.IntervalMS) * time.Millisecond, TenantID: t.TenantID, CronExpr: t.CronExpr, Timezone: t.Timezone})
	}
	return out, rows.Err()
}

func (s *Store) ClaimTask(ctx context.Context, taskID, workerID string, epoch int64, lease time.Duration) (bool, error) {
	r, err := s.Pool.Exec(ctx, `UPDATE scheduler_tasks SET status='RUNNING', owner_worker_id=$2, lease_until=NOW()+$4::interval, attempt=attempt+1, epoch=$3, version=version+1, updated_at=NOW() WHERE task_id=$1 AND status IN ('PENDING','RETRY_WAIT')`, taskID, workerID, epoch, lease.String())
	return r.RowsAffected() == 1, err
}

func (s *Store) CompleteTask(ctx context.Context, taskID, workerID string, success bool) error {
	status := "FAILED"
	if success {
		status = "SUCCEEDED"
	}
	_, err := s.Pool.Exec(ctx, `UPDATE scheduler_tasks SET status=$1, owner_worker_id=NULL, lease_until=NULL, updated_at=NOW() WHERE task_id=$2 AND owner_worker_id=$3`, status, taskID, workerID)
	return err
}

func (s *Store) RescheduleTask(ctx context.Context, t scheduler.Task, workerID string) error {
	_, err := s.Pool.Exec(ctx, `UPDATE scheduler_tasks SET status='PENDING', next_run_at=$1, owner_worker_id=NULL, lease_until=NULL, attempt=0, version=$2, updated_at=NOW() WHERE task_id=$3 AND owner_worker_id=$4`, t.RunAt, t.Version, t.ID, workerID)
	return err
}

func (s *Store) UpdateTaskState(ctx context.Context, taskID, status, reason string) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var from string
	if err = tx.QueryRow(ctx, `SELECT status FROM scheduler_tasks WHERE task_id=$1 FOR UPDATE`, taskID).Scan(&from); err != nil {
		return err
	}
	if !validStateTransition(from, status) {
		return errors.New("invalid task state transition: " + from + " -> " + status)
	}
	if _, err = tx.Exec(ctx, `UPDATE scheduler_tasks SET status=$1, owner_worker_id=NULL, lease_until=NULL, version=version+1, updated_at=NOW() WHERE task_id=$2`, status, taskID); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO scheduler_task_history(task_id,from_status,to_status,reason) VALUES($1,$2,$3,$4)`, taskID, from, status, reason); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func validStateTransition(from, to string) bool {
	switch from {
	case "PENDING", "RETRY_WAIT":
		return to == "PAUSED" || to == "CANCELED" || to == "RUNNING" || to == "RETRY_WAIT" || to == "FAILED"
	case "RUNNING":
		return to == "SUCCEEDED" || to == "FAILED" || to == "RETRY_WAIT" || to == "CANCELED" || to == "PENDING"
	case "PAUSED":
		return to == "PENDING" || to == "CANCELED"
	case "FAILED":
		return to == "PENDING" || to == "CANCELED"
	default:
		return false
	}
}

func (s *Store) RecordAttempt(ctx context.Context, taskID string, attempt int, workerID, status, taskErr string, output []byte, epoch int64) error {
	_, err := s.Pool.Exec(ctx, `INSERT INTO scheduler_task_attempts(task_id,attempt,worker_id,status,finished_at,error,output,epoch) VALUES($1,$2,$3,$4,NOW(),$5,$6,$7)`, taskID, attempt, workerID, status, taskErr, output, epoch)
	return err
}

func (s *Store) AppendAudit(ctx context.Context, record scheduler.AuditRecord) error {
	metadata, err := json.Marshal(record.Metadata)
	if err != nil {
		return err
	}
	_, err = s.Pool.Exec(ctx, `INSERT INTO scheduler_audit_log(tenant_id,actor_id,action,resource_type,resource_id,request_id,task_id,worker_id,epoch,metadata) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb)`, record.TenantID, record.ActorID, record.Action, record.ResourceType, record.ResourceID, record.RequestID, record.TaskID, record.WorkerID, record.Epoch, metadata)
	return err
}

func (s *Store) LoadTaskAttempts(ctx context.Context, taskID string) ([]scheduler.AttemptView, error) {
	rows, err := s.Pool.Query(ctx, `SELECT attempt,worker_id,status,started_at,finished_at,error,epoch FROM scheduler_task_attempts WHERE task_id=$1 ORDER BY attempt DESC`, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []scheduler.AttemptView
	for rows.Next() {
		var item scheduler.AttemptView
		if err := rows.Scan(&item.Attempt, &item.WorkerID, &item.Status, &item.StartedAt, &item.FinishedAt, &item.Error, &item.Epoch); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *Store) LoadTaskHistory(ctx context.Context, taskID string) ([]scheduler.Event, error) {
	rows, err := s.Pool.Query(ctx, `SELECT created_at,to_status,task_id FROM scheduler_task_history WHERE task_id=$1 ORDER BY created_at DESC`, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []scheduler.Event
	for rows.Next() {
		var item scheduler.Event
		if err := rows.Scan(&item.At, &item.Type, &item.TaskID); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func max64(v, fallback int64) int64 {
	if v == 0 {
		return fallback
	}
	return v
}
