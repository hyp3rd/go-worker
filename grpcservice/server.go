package grpcservice

import (
	"context"
	"fmt"

	worker "github.com/hyp3rd/go-worker"
	workerv1 "github.com/hyp3rd/go-worker/gen/worker/v1"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	workerv1.UnimplementedWorkerServiceServer
	svc worker.Service
}

func New(svc worker.Service) *Server {
	return &Server{svc: svc}
}

func (s *Server) RegisterTasks(ctx context.Context, req *workerv1.RegisterTasksRequest) (*workerv1.RegisterTasksResponse, error) {
	for _, t := range req.GetTasks() {
		task := &worker.Task{
			ID:          uuid.New(),
			Name:        t.GetName(),
			Description: t.GetDescription(),
			Priority:    int(t.GetPriority()),
			Retries:     int(t.GetRetries()),
			RetryDelay:  t.GetRetryDelay().AsDuration(),
			Execute: func(name string) worker.TaskFunc {
				return func() (any, error) {
					return fmt.Sprintf("task %s executed", name), nil
				}
			}(t.GetName()),
		}
		if err := s.svc.RegisterTask(ctx, task); err != nil {
			return nil, status.Errorf(codes.Internal, "register task: %v", err)
		}
	}
	return &workerv1.RegisterTasksResponse{Ok: true}, nil
}

func (s *Server) StreamResults(req *workerv1.StreamResultsRequest, stream workerv1.WorkerService_StreamResultsServer) error {
	for res := range s.svc.StreamResults() {
		r := &workerv1.TaskResult{
			Name:        res.Task.Name,
			Description: res.Task.Description,
			Priority:    int32(res.Task.Priority),
		}
		if res.Result != nil {
			r.Result = fmt.Sprint(res.Result)
		}
		if res.Error != nil {
			r.Error = res.Error.Error()
		}
		if err := stream.Send(r); err != nil {
			return err
		}
	}
	return nil
}
