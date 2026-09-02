CREATE TABLE IF NOT EXISTS scheduler_leader (
    cluster_id TEXT PRIMARY KEY,
    leader_id TEXT NOT NULL,
    epoch BIGINT NOT NULL DEFAULT 0,
    heartbeat_at TIMESTAMPTZ NOT NULL,
    lease_until TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS scheduler_tasks (
    task_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL DEFAULT 'local',
    task_name TEXT NOT NULL,
    required_role TEXT NOT NULL,
    params JSONB NOT NULL DEFAULT '{}'::jsonb,
    payload BYTEA,
    schedule_type TEXT NOT NULL DEFAULT 'ONCE',
    interval_ms BIGINT NOT NULL DEFAULT 0,
    cron_expr TEXT NOT NULL DEFAULT '',
    timezone TEXT NOT NULL DEFAULT 'UTC',
    next_run_at TIMESTAMPTZ NOT NULL,
    priority INT NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'PENDING',
    owner_worker_id TEXT,
    lease_until TIMESTAMPTZ,
    attempt INT NOT NULL DEFAULT 0,
    epoch BIGINT NOT NULL DEFAULT 0,
    version BIGINT NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_scheduler_tasks_due ON scheduler_tasks(status, next_run_at, priority DESC);
ALTER TABLE scheduler_tasks ADD COLUMN IF NOT EXISTS tenant_id TEXT NOT NULL DEFAULT 'local';
ALTER TABLE scheduler_tasks ADD COLUMN IF NOT EXISTS cron_expr TEXT NOT NULL DEFAULT '';
ALTER TABLE scheduler_tasks ADD COLUMN IF NOT EXISTS timezone TEXT NOT NULL DEFAULT 'UTC';

CREATE TABLE IF NOT EXISTS scheduler_task_history (
    id BIGSERIAL PRIMARY KEY,
    task_id TEXT NOT NULL,
    from_status TEXT,
    to_status TEXT NOT NULL,
    reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_scheduler_task_history_task ON scheduler_task_history(task_id, created_at DESC);

CREATE TABLE IF NOT EXISTS scheduler_task_attempts (
    id BIGSERIAL PRIMARY KEY,
    task_id TEXT NOT NULL,
    attempt INT NOT NULL,
    worker_id TEXT,
    status TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    error TEXT,
    output BYTEA,
    epoch BIGINT NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_scheduler_task_attempts_task ON scheduler_task_attempts(task_id, attempt DESC);

CREATE TABLE IF NOT EXISTS scheduler_audit_log (
    id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL DEFAULT 'local',
    actor_id TEXT NOT NULL,
    action TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    request_id TEXT,
    task_id TEXT,
    worker_id TEXT,
    epoch BIGINT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_scheduler_audit_tenant_time ON scheduler_audit_log(tenant_id, created_at DESC);

CREATE TABLE IF NOT EXISTS scheduler_workers (
    worker_id TEXT PRIMARY KEY,
    roles JSONB NOT NULL DEFAULT '[]'::jsonb,
    slots INT NOT NULL DEFAULT 1,
    in_flight INT NOT NULL DEFAULT 0,
    last_heartbeat TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
