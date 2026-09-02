package grpcserver

import (
	"context"
	"net"
	"testing"
	"time"

	schedulerv1 "github.com/yowaimono/go-grpc-scheduler/api/gen"
	"github.com/yowaimono/go-grpc-scheduler/internal/auth"
	"github.com/yowaimono/go-grpc-scheduler/internal/scheduler"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

func TestWorkerStreamReceivesRoleAssignment(t *testing.T) {
	engine := scheduler.New("master", &scheduler.Coordinator{})
	engine.Elect()
	engine.RegisterWorker("worker-a", []string{"image"}, 1)
	if err := engine.Submit(context.Background(), scheduler.Task{ID: "t-1", Name: "image.resize", Role: "image", RunAt: time.Now()}); err != nil {
		t.Fatal(err)
	}

	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	schedulerv1.RegisterSchedulerServer(server, &Server{Engine: engine})
	go server.Serve(listener)
	defer server.Stop()

	conn, err := grpc.DialContext(context.Background(), "bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithInsecure())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := schedulerv1.NewSchedulerClient(conn)
	if _, err := client.RegisterWorker(context.Background(), &schedulerv1.RegisterWorkerRequest{WorkerId: "worker-a", Roles: []string{"image"}, Slots: 1}); err != nil {
		t.Fatal(err)
	}
	stream, err := client.Work(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&schedulerv1.WorkerMessage{Kind: "hello", WorkerId: "worker-a"}); err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&schedulerv1.WorkerMessage{Kind: "pull", WorkerId: "worker-a", Pull: &schedulerv1.PullRequest{MaxTasks: 1}}); err != nil {
		t.Fatal(err)
	}
	msg, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if msg.Assignment == nil || msg.Assignment.TaskId != "t-1" {
		t.Fatalf("assignment = %#v", msg.Assignment)
	}
}

func TestWorkerRegistrationRequiresToken(t *testing.T) {
	engine := scheduler.New("master", &scheduler.Coordinator{})
	engine.Elect()
	srv := &Server{Engine: engine, WorkerAuth: auth.New(map[string]auth.Principal{"secret": {Subject: "w", Tenant: "t", Role: "worker"}})}
	if _, err := srv.RegisterWorker(context.Background(), &schedulerv1.RegisterWorkerRequest{WorkerId: "w"}); err == nil {
		t.Fatal("registration accepted without token")
	}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer secret"))
	if _, err := srv.RegisterWorker(ctx, &schedulerv1.RegisterWorkerRequest{WorkerId: "w"}); err != nil {
		t.Fatal(err)
	}
}

func TestSubmitTaskCronValidationAndGeneratedID(t *testing.T) {
	engine := scheduler.New("master", &scheduler.Coordinator{})
	engine.Elect()
	srv := &Server{Engine: engine}
	if _, err := srv.SubmitTask(context.Background(), &schedulerv1.SubmitTaskRequest{TaskName: "cron", Role: "default", ScheduleType: "CRON", CronExpr: "invalid", Timezone: "UTC"}); err == nil {
		t.Fatal("invalid cron accepted")
	}
	resp, err := srv.SubmitTask(context.Background(), &schedulerv1.SubmitTaskRequest{TaskName: "once", Role: "default"})
	if err != nil || resp.TaskId == "" {
		t.Fatalf("generated id = %#v, %v", resp, err)
	}
}
