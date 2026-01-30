package worker

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/hyp3rd/ewrap"
	"google.golang.org/protobuf/proto"
)

// DurableTask represents a task that can be persisted and rehydrated.
type DurableTask struct {
	ID         uuid.UUID
	Handler    string
	Message    proto.Message
	Payload    []byte
	Priority   int
	Retries    int
	RetryDelay time.Duration
	Metadata   map[string]string
}

// DurableTaskLease represents a leased durable task.
type DurableTaskLease struct {
	Task       DurableTask
	LeaseID    string
	Attempts   int
	MaxRetries int
}

// DurableHandlerSpec describes a durable task handler.
type DurableHandlerSpec struct {
	Make func() proto.Message
	Fn   func(ctx context.Context, payload proto.Message) (any, error)
}

// DurableCodec marshals and unmarshals durable task payloads.
type DurableCodec interface {
	Marshal(msg proto.Message) ([]byte, error)
	Unmarshal(data []byte, msg proto.Message) error
}

// ProtoDurableCodec uses protobuf for serialization.
type ProtoDurableCodec struct{}

// Marshal marshals a protobuf message to bytes.
func (ProtoDurableCodec) Marshal(msg proto.Message) ([]byte, error) {
	data, err := proto.Marshal(msg)
	if err != nil {
		return nil, ewrap.Wrap(err, "marshal protobuf message")
	}

	return data, nil
}

// Unmarshal unmarshals bytes into a protobuf message.
func (ProtoDurableCodec) Unmarshal(data []byte, msg proto.Message) error {
	err := ewrap.Wrap(proto.Unmarshal(data, msg), "unmarshal protobuf message")
	if err != nil {
		return ewrap.Wrap(err, "unmarshal protobuf message")
	}

	return nil
}

// DurableBackend provides persistence and leasing for durable tasks.
type DurableBackend interface {
	Enqueue(ctx context.Context, task DurableTask) error
	Dequeue(ctx context.Context, limit int, lease time.Duration) ([]DurableTaskLease, error)
	Ack(ctx context.Context, lease DurableTaskLease) error
	Nack(ctx context.Context, lease DurableTaskLease, delay time.Duration) error
	Fail(ctx context.Context, lease DurableTaskLease, err error) error
	Extend(ctx context.Context, lease DurableTaskLease, leaseDuration time.Duration) error
}
