package scheduler

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"
)

type benchmarkRepo struct{ tasks []Task }

func (r *benchmarkRepo) UpsertTask(context.Context, Task) error                 { return nil }
func (r *benchmarkRepo) LoadPending(context.Context, time.Time) ([]Task, error) { return r.tasks, nil }
func (r *benchmarkRepo) ClaimTask(context.Context, string, string, int64, time.Duration) (bool, error) {
	return true, nil
}
func (r *benchmarkRepo) CompleteTask(context.Context, string, string, bool) error { return nil }

func BenchmarkSnapshotSync(b *testing.B) {
	count := 10000
	if raw := os.Getenv("TASK_COUNT"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			count = parsed
		}
	}
	repo := &benchmarkRepo{}
	for i := 0; i < count; i++ {
		repo.tasks = append(repo.tasks, Task{ID: fmt.Sprintf("task-%d", i), Role: "default", RunAt: time.Now()})
	}
	engine := New("master", &Coordinator{}).WithRepository(repo)
	engine.Elect()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = engine.Sync(context.Background(), time.Now().Add(-time.Minute))
	}
}
