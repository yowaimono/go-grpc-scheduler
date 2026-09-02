package scheduler

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"
)

func BenchmarkSubmitTenThousandTasks(b *testing.B) {
	count := 10000
	if raw := os.Getenv("TASK_COUNT"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			count = parsed
		}
	}
	for n := 0; n < b.N; n++ {
		engine := New("master", &Coordinator{})
		engine.Elect()
		for i := 0; i < count; i++ {
			_ = engine.Submit(context.Background(), Task{ID: fmt.Sprintf("task-%d", i), Role: "default", RunAt: time.Now()})
		}
	}
}

func BenchmarkConcurrentAssign(b *testing.B) {
	engine := New("master", &Coordinator{})
	engine.Elect()
	for i := 0; i < 10000; i++ {
		engine.Submit(context.Background(), Task{ID: fmt.Sprintf("task-%d", i), Role: "default", RunAt: time.Now()})
	}
	for i := 0; i < 32; i++ {
		engine.RegisterWorker(fmt.Sprintf("worker-%d", i), []string{"default"}, 32)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		worker := fmt.Sprintf("worker-%d", time.Now().UnixNano()%32)
		for pb.Next() {
			_, _ = engine.Assign(worker, time.Now())
		}
	})
}
