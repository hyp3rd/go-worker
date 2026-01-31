package tests

import (
	"context"
	"errors"
	"testing"

	"github.com/hyp3rd/ewrap"
	"google.golang.org/protobuf/reflect/protoreflect"

	worker "github.com/hyp3rd/go-worker"
	workerpb "github.com/hyp3rd/go-worker/pkg/worker/v1"
)

func assertTypedHandlerInvocation(
	t *testing.T,
	specFn func(context.Context, any) (any, error),
) {
	t.Helper()

	result, err := specFn(context.Background(), &workerpb.SendEmailPayload{})
	if err != nil {
		t.Fatalf("handler returned error: %v", err)
	}

	if result != "ok" {
		t.Fatalf("expected result ok, got %v", result)
	}

	_, err = specFn(context.Background(), &workerpb.Task{})
	if !errors.Is(err, worker.ErrHandlerPayloadTypeMismatch) {
		t.Fatalf("expected ErrHandlerPayloadTypeMismatch, got %v", err)
	}
}

func TestTypedHandlerConversions(t *testing.T) {
	t.Parallel()

	makeSpec := func() worker.TypedHandlerSpec[*workerpb.SendEmailPayload] {
		return worker.TypedHandlerSpec[*workerpb.SendEmailPayload]{
			Make: func() *workerpb.SendEmailPayload { return &workerpb.SendEmailPayload{} },
			Fn: func(_ context.Context, payload *workerpb.SendEmailPayload) (any, error) {
				if payload == nil {
					return nil, ewrap.New("missing payload")
				}

				return "ok", nil
			},
		}
	}

	cases := []struct {
		name   string
		specFn func(context.Context, any) (any, error)
	}{
		{
			name: "grpc",
			specFn: func(ctx context.Context, payload any) (any, error) {
				spec := worker.TypedHandler(makeSpec())

				p, ok := payload.(protoreflect.ProtoMessage)
				if !ok {
					return nil, worker.ErrHandlerPayloadTypeMismatch
				}

				return spec.Fn(ctx, p)
			},
		},
		{
			name: "durable",
			specFn: func(ctx context.Context, payload any) (any, error) {
				spec := worker.TypedDurableHandler(worker.TypedDurableHandlerSpec[*workerpb.SendEmailPayload]{
					Make: func() *workerpb.SendEmailPayload { return &workerpb.SendEmailPayload{} },
					Fn: func(_ context.Context, payload *workerpb.SendEmailPayload) (any, error) {
						if payload == nil {
							return nil, ewrap.New("missing payload")
						}

						return "ok", nil
					},
				})

				p, ok := payload.(protoreflect.ProtoMessage)
				if !ok {
					return nil, worker.ErrHandlerPayloadTypeMismatch
				}

				return spec.Fn(ctx, p)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assertTypedHandlerInvocation(t, tc.specFn)
		})
	}
}

func TestTypedRegistry_DuplicateHandler(t *testing.T) {
	t.Parallel()

	registry := worker.NewTypedHandlerRegistry()

	err := worker.AddTypedHandler(registry, "send_email", worker.TypedHandlerSpec[*workerpb.SendEmailPayload]{
		Make: func() *workerpb.SendEmailPayload { return &workerpb.SendEmailPayload{} },
		Fn: func(_ context.Context, _ *workerpb.SendEmailPayload) (any, error) {
			return "ok", nil
		},
	})
	if err != nil {
		t.Fatalf("Add returned error: %v", err)
	}

	err = worker.AddTypedHandler(registry, "send_email", worker.TypedHandlerSpec[*workerpb.SendEmailPayload]{
		Make: func() *workerpb.SendEmailPayload { return &workerpb.SendEmailPayload{} },
		Fn: func(_ context.Context, _ *workerpb.SendEmailPayload) (any, error) {
			return "ok", nil
		},
	})
	if !errors.Is(err, worker.ErrHandlerAlreadyRegistered) {
		t.Fatalf("expected ErrHandlerAlreadyRegistered, got %v", err)
	}

	durableRegistry := worker.NewTypedDurableRegistry()

	err = worker.AddTypedDurableHandler(durableRegistry, "send_email", worker.TypedDurableHandlerSpec[*workerpb.SendEmailPayload]{
		Make: func() *workerpb.SendEmailPayload { return &workerpb.SendEmailPayload{} },
		Fn: func(_ context.Context, _ *workerpb.SendEmailPayload) (any, error) {
			return "ok", nil
		},
	})
	if err != nil {
		t.Fatalf("Add durable handler returned error: %v", err)
	}
}
