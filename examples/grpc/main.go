package main

import (
        "context"
        "log"
        "net"
        "time"

        "google.golang.org/grpc"
        "google.golang.org/grpc/credentials/insecure"
        "google.golang.org/protobuf/types/known/durationpb"

        worker "github.com/hyp3rd/go-worker"
        workerpb "github.com/hyp3rd/go-worker/pkg/worker/v1"
)

func main() {
	tm := worker.NewTaskManagerWithDefaults(context.Background())

	server := grpc.NewServer()
	workerpb.RegisterWorkerServiceServer(server, worker.NewGRPCServer(tm))

	ctx := context.Background()

	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:50051")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	go func() {
		err = server.Serve(lis)
		if err != nil {
			log.Fatalf("serve: %v", err)
		}
	}()

	conn, err := grpc.NewClient("127.0.0.1:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial: %v", err)
	}

	defer func() {
		cerr := conn.Close()
		if cerr != nil {
			log.Printf("close: %v", cerr)
		}
	}()

	defer server.GracefulStop()
	defer tm.Stop()

	client := workerpb.NewWorkerServiceClient(conn)

	const timeout = 5 * time.Second

	cctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	_, err = client.RegisterTasks(cctx, &workerpb.RegisterTasksRequest{
                Tasks: []*workerpb.Task{
                        {
                                Name:        "demo",
                                Description: "demo task",
                                Priority:    1,
                                Retries:     0,
                                RetryDelay:  durationpb.New(time.Second),
                        },
                },
        })
	if err != nil {
		log.Printf("register: %v", err)

		return
	}

	stream, err := client.StreamResults(cctx, &workerpb.StreamResultsRequest{})
	if err != nil {
		log.Printf("stream: %v", err)

		return
	}

	res, err := stream.Recv()
	if err != nil {
		log.Printf("recv: %v", err)

		return
	}

	log.Printf("task %s result=%s error=%s", res.GetId(), res.GetOutput(), res.GetError())
}
