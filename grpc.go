package worker

import (
	"context"
	"fmt"

	workerpb "github.com/hyp3rd/go-worker/pkg/worker/v1"
)

// GRPCServer implements the generated WorkerServiceServer interface.
type GRPCServer struct {
	svc Service
}

// NewGRPCServer creates a new gRPC server backed by the provided Service.
func NewGRPCServer(svc Service) *GRPCServer {
	return &GRPCServer{svc: svc}
}

// RegisterTasks registers one or more tasks with the underlying service.
func (s *GRPCServer) RegisterTasks(ctx context.Context, req *workerpb.RegisterTasksRequest) (*workerpb.RegisterTasksResponse, error) {
	tasks := make([]*Task, 0, len(req.GetTasks()))

	for _, taskReq := range req.GetTasks() {
		payload := taskReq.GetPayload()

		task, err := NewTask(ctx, func(ctx context.Context, args ...any) (any, error) {
			return payload, nil
		})
		if err != nil {
			return nil, err
		}

		task.Name = taskReq.GetName()
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
func (s *GRPCServer) StreamResults(req *workerpb.StreamResultsRequest, stream workerpb.WorkerService_StreamResultsServer) error {
	for res := range s.svc.StreamResults() {
		out := &workerpb.StreamResultsResponse{
			Id: res.Task.ID.String(),
		}

		if res.Result != nil {
			out.Output = fmt.Sprint(res.Result)
		}

		if res.Error != nil {
			out.Error = res.Error.Error()
		}

		err := stream.Send(out)
		if err != nil {
			return fmt.Errorf("stream send: %w", err)
		}
	}

	return nil
}

var _ workerpb.WorkerServiceServer = (*GRPCServer)(nil)
