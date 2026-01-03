package worker

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/hyp3rd/ewrap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"

	workerpb "github.com/hyp3rd/go-worker/pkg/worker/v1"
)

// HandlerSpec describes a single handler for a gRPC method.
type HandlerSpec struct {
	// Make returns a zero value of the payload message to unmarshal into.
	Make func() protoreflect.ProtoMessage
	// Fn does the work. Your Task.Execute will call this.
	Fn func(ctx context.Context, payload protoreflect.ProtoMessage) (any, error)
}

// GRPCServer implements the generated WorkerServiceServer interface.
type GRPCServer struct {
	svc      Service
	handlers map[string]HandlerSpec
}

// NewGRPCServer creates a new gRPC server backed by the provided Service.
func NewGRPCServer(svc Service, handlers map[string]HandlerSpec) *GRPCServer {
	return &GRPCServer{svc: svc, handlers: handlers}
}

// RegisterTasks registers one or more tasks with the underlying service.
func (s *GRPCServer) RegisterTasks(ctx context.Context, req *workerpb.RegisterTasksRequest) (*workerpb.RegisterTasksResponse, error) {
	tasks := make([]*Task, 0, len(req.GetTasks()))

	for _, taskReq := range req.GetTasks() {
		spec, ok := s.handlers[taskReq.GetName()]
		if !ok {
			return nil, status.Errorf(codes.NotFound, "no handler registered for %q", taskReq.GetName())
		}

		// materialize the payload message and unmarshal the Any into it
		var payload protoreflect.ProtoMessage

		if taskReq.GetPayload() != nil {
			payload = spec.Make()

			err := anypb.UnmarshalTo(taskReq.GetPayload(), payload, proto.UnmarshalOptions{})
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "bad payload for %q: %v", taskReq.GetName(), err)
			}
		}

		task, err := NewTask(ctx, func(ctx context.Context, _ ...any) (any, error) {
			return spec.Fn(ctx, payload) // pass the typed payload to the handler
		})
		if err != nil {
			return nil, err
		}

		task.Name = taskReq.GetName()
		task.Description = taskReq.GetDescription()
		task.Priority = int(taskReq.GetPriority())
		task.Retries = int(taskReq.GetRetries())

		if d := taskReq.GetRetryDelay(); d != nil {
			task.RetryDelay = d.AsDuration()
		}

		tasks = append(tasks, task)
	}

	s.svc.RegisterTasks(ctx, tasks...)

	ids := make([]string, len(tasks))

	for i, task := range tasks {
		ids[i] = task.ID.String()
	}

	return &workerpb.RegisterTasksResponse{Ids: ids}, nil
}

// StreamResults streams task results back to the client.
//
//nolint:cyclop,revive
func (s *GRPCServer) StreamResults(req *workerpb.StreamResultsRequest, stream workerpb.WorkerService_StreamResultsServer) error {
	ctx := stream.Context()

	target := map[string]struct{}{}
	for _, id := range req.GetIds() {
		target[id] = struct{}{}
	}

	remaining := len(target)
	isTarget := func(id string) bool {
		if len(target) == 0 {
			return true
		} // firehose mode

		_, ok := target[id]

		return ok
	}

	for res := range s.svc.StreamResults() {
		select {
		case <-ctx.Done():
			// client went away; stop streaming (tasks continue)
			return ewrap.Wrap(status.FromContextError(ctx.Err()).Err(), "client went away; stop streaming")
		default:
		}

		if res.Task == nil {
			continue
		}

		id := res.Task.ID.String()

		if !isTarget(id) {
			continue
		}

		out := &workerpb.StreamResultsResponse{
			Id: id,
		}

		if res.Result != nil {
			out.Output = fmt.Sprint(res.Result)
		}

		if res.Error != nil {
			out.Error = res.Error.Error()
		}

		err := stream.Send(out)
		if err != nil {
			return ewrap.Wrap(err, "failed to send stream response")
		}

		// If we’re tracking a specific set, check for terminal state.
		if len(target) > 0 {
			//nolint:exhaustive
			switch res.Task.Status {
			case Completed, Cancelled, Failed:
				if _, ok := target[id]; ok {
					delete(target, id)

					remaining--
				}

				if req.GetCloseOnCompletion() && remaining == 0 {
					return nil // clean EOF on client
				}
			default:
				// not done yet
			}
		}
	}

	return nil
}

// CancelTask cancels an active task by its ID.
func (s *GRPCServer) CancelTask(_ context.Context, req *workerpb.CancelTaskRequest) (*workerpb.CancelTaskResponse, error) {
	id, err := uuid.Parse(req.GetId())
	if err != nil {
		return nil, fmt.Errorf("parse id: %w", err)
	}

	s.svc.CancelTask(id)

	return &workerpb.CancelTaskResponse{}, nil
}

// GetTask returns information about a task by its ID.
func (s *GRPCServer) GetTask(_ context.Context, req *workerpb.GetTaskRequest) (*workerpb.GetTaskResponse, error) {
	id, err := uuid.Parse(req.GetId())
	if err != nil {
		return nil, fmt.Errorf("parse id: %w", err)
	}

	task, err := s.svc.GetTask(id)
	if err != nil {
		return nil, fmt.Errorf("get task: %w", err)
	}

	resp := &workerpb.GetTaskResponse{
		Id:     task.ID.String(),
		Name:   task.Name,
		Status: task.Status.String(),
	}

	if v := task.Result.Load(); v != nil {
		resp.Output = fmt.Sprint(v)
	}

	if v := task.Error.Load(); v != nil {
		resp.Error = fmt.Sprint(v)
	}

	return resp, nil
}

var _ workerpb.WorkerServiceServer = (*GRPCServer)(nil)
