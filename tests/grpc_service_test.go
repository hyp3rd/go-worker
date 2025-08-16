package tests

import (
	"context"
	"testing"
	"time"

	worker "github.com/hyp3rd/go-worker"
	workerv1 "github.com/hyp3rd/go-worker/gen/worker/v1"
	"github.com/hyp3rd/go-worker/grpcservice"

	"google.golang.org/grpc/metadata"
)

type resultStream struct {
	ctx     context.Context
	results []*workerv1.TaskResult
}

func (s *resultStream) SetHeader(metadata.MD) error  { return nil }
func (s *resultStream) SendHeader(metadata.MD) error { return nil }
func (s *resultStream) SetTrailer(metadata.MD)       {}
func (s *resultStream) Context() context.Context     { return s.ctx }
func (s *resultStream) SendMsg(m interface{}) error  { return nil }
func (s *resultStream) RecvMsg(m interface{}) error  { return nil }
func (s *resultStream) Send(r *workerv1.TaskResult) error {
	s.results = append(s.results, r)
	return nil
}

func TestGRPCServer_RegisterAndStream(t *testing.T) {
	tm := worker.NewTaskManager(context.TODO(), 2, 10, 5, time.Second, time.Second, 3)
	srv := grpcservice.New(tm)

	req := &workerv1.RegisterTasksRequest{Tasks: []*workerv1.TaskInput{{Name: "test", Priority: 1}}}
	if _, err := srv.RegisterTasks(context.Background(), req); err != nil {
		t.Fatalf("RegisterTasks failed: %v", err)
	}

	stream := &resultStream{ctx: context.Background()}
	go func() {
		_ = srv.StreamResults(&workerv1.StreamResultsRequest{}, stream)
	}()

	time.Sleep(100 * time.Millisecond)
	if len(stream.results) == 0 {
		t.Fatalf("expected results, got none")
	}
}
