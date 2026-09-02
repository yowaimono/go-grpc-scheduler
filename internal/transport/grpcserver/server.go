package grpcserver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	schedulerv1 "github.com/yowaimono/go-grpc-scheduler/api/gen"
	"github.com/yowaimono/go-grpc-scheduler/internal/auth"
	"github.com/yowaimono/go-grpc-scheduler/internal/scheduler"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type Server struct {
	schedulerv1.UnimplementedSchedulerServer
	Engine     *scheduler.Engine
	WorkerAuth *auth.Authenticator
}

func (s *Server) RegisterWorker(ctx context.Context, req *schedulerv1.RegisterWorkerRequest) (*schedulerv1.RegisterWorkerResponse, error) {
	if err := s.authorizeWorker(ctx); err != nil {
		return nil, err
	}
	if req.WorkerId == "" {
		return nil, fmt.Errorf("worker_id is required")
	}
	roles := append([]string(nil), req.Roles...)
	s.Engine.RegisterWorker(req.WorkerId, roles, int(req.Slots))
	return &schedulerv1.RegisterWorkerResponse{SchedulerId: s.Engine.ID()}, nil
}

func (s *Server) authorizeWorker(ctx context.Context) error {
	if s.WorkerAuth == nil || !s.WorkerAuth.Require {
		return nil
	}
	md, _ := metadata.FromIncomingContext(ctx)
	values := md.Get("authorization")
	token := ""
	if len(values) > 0 {
		token = values[0]
	}
	if _, err := s.WorkerAuth.Authenticate(strings.TrimSpace(strings.TrimPrefix(token, "Bearer "))); err != nil {
		return status.Error(codes.Unauthenticated, "worker token invalid")
	}
	return nil
}

func (s *Server) SubmitTask(ctx context.Context, req *schedulerv1.SubmitTaskRequest) (*schedulerv1.SubmitTaskResponse, error) {
	id := req.TaskId
	if id == "" {
		id = fmt.Sprintf("task-%d", time.Now().UnixNano())
	}
	runAt := time.Now()
	if req.RunAtUnixMs > 0 {
		runAt = time.UnixMilli(req.RunAtUnixMs)
	}
	if req.DelayMs > 0 {
		runAt = time.Now().Add(time.Duration(req.DelayMs) * time.Millisecond)
	}
	if req.ScheduleType == "CRON" {
		next, err := scheduler.NextCron(req.CronExpr, req.Timezone, time.Now())
		if err != nil {
			return nil, err
		}
		runAt = next
	}
	params := make(map[string]json.RawMessage, len(req.Params))
	for key, value := range req.Params {
		params[key] = append(json.RawMessage(nil), value...)
	}
	if err := s.Engine.Submit(ctx, scheduler.Task{ID: id, Name: req.TaskName, Role: req.Role, Params: params, Payload: req.Payload, Priority: int(req.Priority), RunAt: runAt, ScheduleType: req.ScheduleType, Interval: time.Duration(req.IntervalMs) * time.Millisecond, Timeout: time.Duration(req.TimeoutMs) * time.Millisecond, TenantID: req.TenantId, CronExpr: req.CronExpr, Timezone: req.Timezone}); err != nil {
		return nil, err
	}
	return &schedulerv1.SubmitTaskResponse{TaskId: id}, nil
}

func (s *Server) Work(stream schedulerv1.Scheduler_WorkServer) error {
	if err := s.authorizeWorker(stream.Context()); err != nil {
		return err
	}
	workerID := ""
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if msg.WorkerId != "" {
			workerID = msg.WorkerId
		}
		switch msg.Kind {
		case "hello":
			continue
		case "pull":
			if workerID == "" || msg.Pull == nil {
				continue
			}
			max := int(msg.Pull.MaxTasks)
			if max < 1 {
				max = 1
			}
			for i := 0; i < max; i++ {
				t, assignErr := s.Engine.AssignContext(stream.Context(), workerID, time.Now())
				if assignErr != nil {
					break
				}
				assignment := &schedulerv1.TaskAssignment{TaskId: t.ID, TaskName: t.Name, Role: t.Role, Params: make(map[string][]byte, len(t.Params)), Attempt: int32(t.Attempts), TimeoutMs: t.Timeout.Milliseconds()}
				for key, value := range t.Params {
					assignment.Params[key] = append([]byte(nil), value...)
				}
				if sendErr := stream.Send(&schedulerv1.SchedulerMessage{Kind: "assignment", Assignment: assignment}); sendErr != nil {
					return sendErr
				}
			}
		case "result":
			if msg.Result != nil {
				_ = s.Engine.Complete(workerID, msg.Result.TaskId, msg.Result.Success)
			}
		case "heartbeat":
		}
	}
}
