package worker

import (
	"context"
	"errors"
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

	err := s.svc.RegisterTasks(ctx, tasks...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "register tasks: %v", err)
	}

	ids := make([]string, len(tasks))
	for i, task := range tasks {
		ids[i] = task.ID.String()
	}

	return &workerpb.RegisterTasksResponse{Ids: ids}, nil
}

func streamContextErr(ctx context.Context) error {
	select {
	case <-ctx.Done():
		// client went away; stop streaming (tasks continue)
		return ewrap.Wrap(status.FromContextError(ctx.Err()).Err(), "client went away; stop streaming")
	default:
		return nil
	}
}

func buildTargetSet(ids []string) (map[string]struct{}, int) {
	target := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		target[id] = struct{}{}
	}

	return target, len(target)
}

func shouldStreamResult(target map[string]struct{}, id string) bool {
	if len(target) == 0 {
		return true
	}

	_, ok := target[id]

	return ok
}

func makeStreamResponse(id string, res Result) *workerpb.StreamResultsResponse {
	out := &workerpb.StreamResultsResponse{Id: id}

	if res.Result != nil {
		out.Output = fmt.Sprint(res.Result)
	}

	if res.Error != nil {
		out.Error = res.Error.Error()
	}

	return out
}

func shouldCloseStream(
	req *workerpb.StreamResultsRequest,
	target map[string]struct{},
	remaining *int,
	id string,
	taskStatus TaskStatus,
) bool {
	if len(target) == 0 {
		return false
	}

	if !isTerminalStatus(taskStatus) {
		return false
	}

	if _, ok := target[id]; ok {
		delete(target, id)

		*remaining--
	}

	return req.GetCloseOnCompletion() && *remaining == 0
}

// StreamResults streams task results back to the client.
//
//nolint:revive
func (s *GRPCServer) StreamResults(req *workerpb.StreamResultsRequest, stream workerpb.WorkerService_StreamResultsServer) error {
	ctx := stream.Context()

	ch, unsubscribe := s.svc.SubscribeResults(DefaultMaxTasks)
	defer unsubscribe()

	target, remaining := buildTargetSet(req.GetIds())

	for res := range ch {
		err := streamContextErr(ctx)
		if err != nil {
			return err
		}

		if res.Task == nil {
			continue
		}

		id := res.Task.ID.String()
		if !shouldStreamResult(target, id) {
			continue
		}

		err = stream.Send(makeStreamResponse(id, res))
		if err != nil {
			return ewrap.Wrap(err, "failed to send stream response")
		}

		status := res.Task.Status()
		if shouldCloseStream(req, target, &remaining, id, status) {
			return nil
		}
	}

	return nil
}

// CancelTask cancels an active task by its ID.
func (s *GRPCServer) CancelTask(_ context.Context, req *workerpb.CancelTaskRequest) (*workerpb.CancelTaskResponse, error) {
	id, err := uuid.Parse(req.GetId())
	if err != nil {
		return nil, ewrap.Wrap(err, "parse id")
	}

	err = s.svc.CancelTask(id)
	if err != nil {
		if errors.Is(err, ErrTaskNotFound) {
			return nil, status.Errorf(codes.NotFound, "task %s not found", id)
		}

		return nil, status.Errorf(codes.Internal, "cancel task: %v", err)
	}

	return &workerpb.CancelTaskResponse{}, nil
}

// GetTask returns information about a task by its ID.
func (s *GRPCServer) GetTask(_ context.Context, req *workerpb.GetTaskRequest) (*workerpb.GetTaskResponse, error) {
	id, err := uuid.Parse(req.GetId())
	if err != nil {
		return nil, ewrap.Wrap(err, "parse id")
	}

	task, err := s.svc.GetTask(id)
	if err != nil {
		if errors.Is(err, ErrTaskNotFound) {
			return nil, status.Errorf(codes.NotFound, "task %s not found", id)
		}

		return nil, status.Errorf(codes.Internal, "get task: %v", err)
	}

	resp := &workerpb.GetTaskResponse{
		Id:     task.ID.String(),
		Name:   task.Name,
		Status: task.Status().String(),
	}

	if v := task.Result(); v != nil {
		resp.Output = fmt.Sprint(v)
	}

	v := task.Error()
	if v != nil {
		resp.Error = fmt.Sprint(v)
	}

	return resp, nil
}

var _ workerpb.WorkerServiceServer = (*GRPCServer)(nil)
