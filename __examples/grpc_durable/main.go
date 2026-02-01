package main

import (
	"context"
	"flag"
	"log"
	"net"
	"strings"
	"time"

	"github.com/redis/rueidis"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"

	worker "github.com/hyp3rd/go-worker"
	workerpb "github.com/hyp3rd/go-worker/pkg/worker/v1"
)

const (
	grpcAddr         = "127.0.0.1:50052"
	defaultScanCount = 100
)

func main() {
	ctx := context.Background()

	const (
		defaultRedisAddr     = "localhost:6380"
		defaultRedisPassword = "supersecret"
		defaultRedisPrefix   = "go-worker"
		dialTimeout          = 5 * time.Second
		streamTimeout        = 10 * time.Second
	)

	redisAddr := flag.String("redis-addr", defaultRedisAddr, "redis host:port")
	redisPassword := flag.String("redis-password", defaultRedisPassword, "redis password (empty for no auth)")
	redisPrefix := flag.String("redis-prefix", defaultRedisPrefix, "redis key prefix")
	cleanup := flag.Bool("cleanup", true, "delete durable Redis keys for the example prefix")
	debug := flag.Bool("debug", false, "enable verbose durable backend logging")
	flag.Parse()

	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{*redisAddr},
		Password:    *redisPassword,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	backend, err := worker.NewRedisDurableBackend(client, worker.WithRedisDurablePrefix(*redisPrefix))
	if err != nil {
		log.Fatal(err)
	}

	var durableBackend worker.DurableBackend = backend
	if *debug {
		durableBackend = &loggingBackend{inner: durableBackend}
	}

	durableHandlers := map[string]worker.DurableHandlerSpec{
		"send_email": {
			Make: func() proto.Message { return &workerpb.SendEmailPayload{} },
			Fn: func(ctx context.Context, payload proto.Message) (any, error) {
				_ = payload.(*workerpb.SendEmailPayload)
				log.Printf("send_email handler invoked")
				return "ok", nil
			},
		},
	}

	handlers := map[string]worker.HandlerSpec{
		"send_email": {
			Make: func() protoreflect.ProtoMessage { return &workerpb.SendEmailPayload{} },
			Fn: func(ctx context.Context, payload protoreflect.ProtoMessage) (any, error) {
				_ = payload.(*workerpb.SendEmailPayload)
				log.Printf("grpc handler send_email invoked")
				return "ok", nil
			},
		},
	}

	tm := worker.NewTaskManagerWithOptions(
		ctx,
		worker.WithDurableBackend(durableBackend),
		worker.WithDurableHandlers(durableHandlers),
	)

	if *cleanup {
		err := cleanupPrefix(ctx, client, *redisPrefix, defaultScanCount)
		if err != nil {
			log.Fatalf("cleanup failed: %v", err)
		}
	}

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatal(err)
	}

	grpcServer := grpc.NewServer()
	workerpb.RegisterWorkerServiceServer(grpcServer, worker.NewGRPCServer(tm, handlers))
	go func() {
		log.Printf("gRPC server listening on %s", grpcAddr)
		if serveErr := grpcServer.Serve(lis); serveErr != nil {
			log.Fatalf("grpc serve: %v", serveErr)
		}
	}()
	defer grpcServer.GracefulStop()

	dialCtx, dialCancel := context.WithTimeout(ctx, dialTimeout)
	defer dialCancel()

	// NOTE: insecure transport is for local demos only; use TLS credentials in production.
	conn, err := grpc.DialContext(dialCtx, grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	api := workerpb.NewWorkerServiceClient(conn)

	streamCtx, cancel := context.WithTimeout(ctx, streamTimeout)
	defer cancel()

	stream, err := api.StreamResults(streamCtx, &workerpb.StreamResultsRequest{
		CloseOnCompletion: false,
	})
	if err != nil {
		log.Fatal(err)
	}

	payload, err := anypb.New(&workerpb.SendEmailPayload{
		To:      "ops@example.com",
		Subject: "Hello durable gRPC",
		Body:    "This task is persisted via Redis.",
	})
	if err != nil {
		log.Fatal(err)
	}

	registerResp, err := api.RegisterDurableTasks(ctx, &workerpb.RegisterDurableTasksRequest{
		Tasks: []*workerpb.DurableTask{
			{
				Name:           "send_email",
				Payload:        payload,
				IdempotencyKey: "durable:send_email:ops@example.com",
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	if len(registerResp.Ids) == 0 {
		log.Fatal("no task ids returned")
	}

	targetID := registerResp.Ids[0]
	log.Printf("registered durable task id=%s", targetID)
	log.Printf("note: durable execution is at-least-once; handlers should be idempotent")

	for {
		msg, err := stream.Recv()
		if err != nil {
			log.Printf("stream recv error: %v", err)
			break
		}
		if msg.Id == targetID {
			log.Printf("result id=%s error=%s", msg.Id, msg.Error)
			break
		}
	}
}

func keyPrefix(prefix string) string {
	if prefix == "" {
		return "go-worker"
	}

	if strings.Contains(prefix, "{") {
		return prefix
	}

	return "{" + prefix + "}"
}

func cleanupPrefix(ctx context.Context, client rueidis.Client, prefix string, count int64) error {
	if count <= 0 {
		count = defaultScanCount
	}

	cursor := uint64(0)
	pattern := keyPrefix(prefix) + ":*"

	for {
		resp := client.Do(ctx, client.B().Scan().Cursor(cursor).Match(pattern).Count(count).Build())
		entry, err := resp.AsScanEntry()
		if err != nil {
			return err
		}

		if len(entry.Elements) > 0 {
			del := client.B().Del().Key(entry.Elements...).Build()
			err := client.Do(ctx, del).Error()
			if err != nil {
				return err
			}
		}

		if entry.Cursor == 0 {
			break
		}

		cursor = entry.Cursor
	}

	return nil
}

type loggingBackend struct {
	inner worker.DurableBackend
}

func (b *loggingBackend) Enqueue(ctx context.Context, task worker.DurableTask) error {
	err := b.inner.Enqueue(ctx, task)
	if err != nil {
		log.Printf("enqueue error: %v", err)
	}
	return err
}

func (b *loggingBackend) Dequeue(ctx context.Context, limit int, lease time.Duration) ([]worker.DurableTaskLease, error) {
	leases, err := b.inner.Dequeue(ctx, limit, lease)
	if err != nil {
		log.Printf("dequeue error: %v", err)
		return leases, err
	}
	if len(leases) > 0 {
		log.Printf("dequeued %d lease(s)", len(leases))
	}
	return leases, nil
}

func (b *loggingBackend) Ack(ctx context.Context, lease worker.DurableTaskLease) error {
	err := b.inner.Ack(ctx, lease)
	if err != nil {
		log.Printf("ack error: %v", err)
	}
	return err
}

func (b *loggingBackend) Nack(ctx context.Context, lease worker.DurableTaskLease, delay time.Duration) error {
	err := b.inner.Nack(ctx, lease, delay)
	if err != nil {
		log.Printf("nack error: %v", err)
	}
	return err
}

func (b *loggingBackend) Fail(ctx context.Context, lease worker.DurableTaskLease, err error) error {
	failErr := b.inner.Fail(ctx, lease, err)
	if failErr != nil {
		log.Printf("fail error: %v", failErr)
	}
	return failErr
}

func (b *loggingBackend) Extend(ctx context.Context, lease worker.DurableTaskLease, duration time.Duration) error {
	extendErr := b.inner.Extend(ctx, lease, duration)
	if extendErr != nil {
		log.Printf("extend error: %v", extendErr)
	}
	return extendErr
}
