package main

import (
	"context"
	"log"
	"os"

	tasksdk "github.com/yowaimono/task-sdk"
	"github.com/yowaimono/task-sdk/task"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	workerID := getenv("DEMO_WORKER_ID", "demo-worker-01")
	endpoint := getenv("DEMO_GRPC_ENDPOINT", "localhost:19090")
	handler := task.Func(func(ctx task.Context) (*task.Result, error) {
		value, ok := ctx.Param("message")
		if !ok {
			value = "hello"
		}
		return &task.Result{Data: []byte(toString(value))}, nil
	})
	worker := tasksdk.Worker().
		Config(&tasksdk.Config{WorkerID: workerID, Roles: []string{"default"}, Slots: 4, SchedulerInstances: []string{endpoint}}).
		Register(tasksdk.DefineTask("demo.echo", "default", handler)).
		SetServerPort(getenv("DEMO_HEALTH_PORT", "18081"))
	if err := worker.Start(ctx); err != nil {
		log.Fatal(err)
	}
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
func toString(value any) string {
	if text, ok := value.(string); ok {
		return text
	}
	return "hello"
}
