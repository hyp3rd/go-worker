package worker

import (
        "context"
        "fmt"

        "github.com/google/uuid"

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
                task, err := NewTask(ctx, func(ctx context.Context, args ...any) (any, error) {
                        return fmt.Sprintf("executed %s", taskReq.GetName()), nil
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
func (s *GRPCServer) StreamResults(req *workerpb.StreamResultsRequest, stream workerpb.WorkerService_StreamResultsServer) error {
        results := s.svc.StreamResults()

        for {
                select {
                case <-stream.Context().Done():
                        return fmt.Errorf("stream context: %w", stream.Context().Err())
                case res, ok := <-results:
                        if !ok {
                                return nil
                        }

                        out := &workerpb.StreamResultsResponse{Id: res.Task.ID.String()}

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
        }
}

// CancelTask cancels an active task by its ID.
func (s *GRPCServer) CancelTask(ctx context.Context, req *workerpb.CancelTaskRequest) (*workerpb.CancelTaskResponse, error) {
        id, err := uuid.Parse(req.GetId())
        if err != nil {
                return nil, fmt.Errorf("parse id: %w", err)
        }

        s.svc.CancelTask(id)

        return &workerpb.CancelTaskResponse{}, nil
}

// GetTask returns information about a task by its ID.
func (s *GRPCServer) GetTask(ctx context.Context, req *workerpb.GetTaskRequest) (*workerpb.GetTaskResponse, error) {
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
