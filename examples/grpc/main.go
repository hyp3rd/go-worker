package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"

	worker "github.com/hyp3rd/go-worker"
	workerpb "github.com/hyp3rd/go-worker/pkg/worker/v1"
)

const (
	timeout = 5 * time.Second
	addr    = "127.0.0.1:50051"
)

func handlers() map[string]worker.HandlerSpec {
	handlers := map[string]worker.HandlerSpec{
		"create_user": {
			Make: func() protoreflect.ProtoMessage { return &workerpb.CreateUserPayload{} },
			Fn: func(ctx context.Context, p protoreflect.ProtoMessage) (any, error) {
				in, ok := p.(*workerpb.CreateUserPayload)
				if !ok {
					return nil, status.Errorf(codes.InvalidArgument, "invalid payload type")
				}
				// do work...
				return fmt.Sprintf("created %s with email %s", in.GetName(), in.GetEmail()), nil
			},
		},
		// Other Tasks:
		// "send_email": {
		// 	Make: func() protoreflect.ProtoMessage { return &workerpb.SendEmailPayload{} },
		// 	Fn: func(ctx context.Context, p protoreflect.ProtoMessage) (any, error) {
		// 		in := p.(*workerpb.SendEmailPayload)
		// 		// do work...
		// 		return "ok", nil
		// 	},
		// },
	}

	return handlers
}

func initComponents() (*worker.TaskManager, *grpc.Server, workerpb.WorkerServiceClient) {
	tm := worker.NewTaskManagerWithDefaults(context.Background())

	server := grpc.NewServer()
	workerpb.RegisterWorkerServiceServer(server, worker.NewGRPCServer(tm, handlers()))

	ctx := context.Background()

	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	go func() {
		err = server.Serve(lis)
		if err != nil {
			log.Fatalf("serve: %v", err)
		}
	}()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial: %v", err)
	}

	// defer func() {
	// 	closeErr := conn.Close()
	// 	if closeErr != nil {
	// 		log.Printf("close: %v", closeErr)
	// 	}
	// }()

	client := workerpb.NewWorkerServiceClient(conn)

	return tm, server, client
}

func main() {
	ctx := context.Background()

	tm, server, client := initComponents()

	defer server.GracefulStop()
	defer tm.Stop()

	// client := workerpb.NewWorkerServiceClient(conn)
	// 1) Build the typed payload
	// singlePayload := &workerpb.CreateUserPayload{
	// 	Name:  "Ada Lovelace",
	// 	Email: "ada@example.com",
	// }

	// 2) Wrap it into Any (sets type_url automatically)
	// anyPayload, err := anypb.New(singlePayload)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// 1) Build the tasks
	tasks := []*workerpb.Task{}
	for i := range 3 {
		p := &workerpb.CreateUserPayload{
			Name:  fmt.Sprintf("user-%d", i),
			Email: fmt.Sprintf("user-%d@example.com", i),
		}

		anyP, err := anypb.New(p)
		if err != nil {
			log.Println(err)

			return
		}

		tasks = append(tasks, &workerpb.Task{
			Name:       "create_user",
			Priority:   1,
			Retries:    1,
			RetryDelay: durationpb.New(time.Second),
			Payload:    anyP,
		})
	}

	ctxWithCancel, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// 4) Send it (repeated lets you send one or many)
	resp, err := client.RegisterTasks(ctx, &workerpb.RegisterTasksRequest{
		Tasks: tasks,
	})
	if err != nil {
		log.Println(err)

		return
	}

	ids := resp.GetIds()

	log.Printf("registered task IDs: %v", ids)

	stream, err := client.StreamResults(ctxWithCancel, &workerpb.StreamResultsRequest{
		Ids:               ids,
		CloseOnCompletion: true,
	})
	if err != nil {
		log.Println(err)

		return
	}

	for {
		msg, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			st, _ := status.FromError(err)
			if st != nil && st.Code() == codes.DeadlineExceeded {
				// you timed out waiting; tasks may still be running
				break
			}

			log.Println(err)

			return
		}

		if msg.GetError() != "" {
			log.Printf("task %s failed: %s", msg.GetId(), msg.GetError())

			continue
		}

		log.Printf("task %s output: %s", msg.GetId(), msg.GetOutput())
	}
}
