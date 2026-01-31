package worker

import (
	"context"
	"maps"

	"github.com/hyp3rd/ewrap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var (
	// ErrHandlerNameRequired indicates a missing handler name during registration.
	ErrHandlerNameRequired = ewrap.New("handler name is required")
	// ErrHandlerSpecInvalid indicates an invalid handler spec during registration.
	ErrHandlerSpecInvalid = ewrap.New("handler spec is invalid")
	// ErrHandlerAlreadyRegistered indicates a duplicate handler name during registration.
	ErrHandlerAlreadyRegistered = ewrap.New("handler already registered")
	// ErrHandlerPayloadTypeMismatch indicates a payload type mismatch during handler invocation.
	ErrHandlerPayloadTypeMismatch = ewrap.New("handler payload type mismatch")
	// ErrHandlerRegistryNil indicates a nil handler registry during registration.
	ErrHandlerRegistryNil = ewrap.New("handler registry is nil")
)

// TypedHandlerSpec provides a compile-time checked handler signature for gRPC tasks.
type TypedHandlerSpec[T protoreflect.ProtoMessage] struct {
	Make func() T
	Fn   func(ctx context.Context, payload T) (any, error)
}

// TypedHandlerRegistry collects typed gRPC handlers and exposes the untyped map.
type TypedHandlerRegistry struct {
	handlers map[string]HandlerSpec
}

// NewTypedHandlerRegistry constructs a registry for typed gRPC handlers.
func NewTypedHandlerRegistry() *TypedHandlerRegistry {
	return &TypedHandlerRegistry{
		handlers: make(map[string]HandlerSpec),
	}
}

// Add registers an untyped handler spec into the registry.
func (r *TypedHandlerRegistry) Add(name string, spec HandlerSpec) error {
	if r == nil {
		return ErrHandlerRegistryNil
	}

	return r.register(name, spec)
}

// Handlers returns a defensive copy of the registered handler map.
func (r *TypedHandlerRegistry) Handlers() map[string]HandlerSpec {
	if len(r.handlers) == 0 {
		return map[string]HandlerSpec{}
	}

	out := make(map[string]HandlerSpec, len(r.handlers))
	maps.Copy(out, r.handlers)

	return out
}

func (r *TypedHandlerRegistry) register(name string, spec HandlerSpec) error {
	if name == "" {
		return ErrHandlerNameRequired
	}

	if spec.Make == nil || spec.Fn == nil {
		return ErrHandlerSpecInvalid
	}

	if r.handlers == nil {
		r.handlers = make(map[string]HandlerSpec)
	}

	if _, exists := r.handlers[name]; exists {
		return ewrap.Wrapf(ErrHandlerAlreadyRegistered, "handler %q", name)
	}

	r.handlers[name] = spec

	return nil
}

// AddTypedHandler registers a typed handler spec into the registry.
func AddTypedHandler[T protoreflect.ProtoMessage](r *TypedHandlerRegistry, name string, spec TypedHandlerSpec[T]) error {
	if r == nil {
		return ErrHandlerRegistryNil
	}

	return r.register(name, TypedHandler(spec))
}

// TypedHandler converts a typed spec into the untyped HandlerSpec.
func TypedHandler[T protoreflect.ProtoMessage](spec TypedHandlerSpec[T]) HandlerSpec {
	return HandlerSpec{
		Make: func() protoreflect.ProtoMessage {
			return spec.Make()
		},
		Fn: func(ctx context.Context, payload protoreflect.ProtoMessage) (any, error) {
			typed, ok := payload.(T)
			if !ok {
				return nil, ErrHandlerPayloadTypeMismatch
			}

			return spec.Fn(ctx, typed)
		},
	}
}

// TypedDurableHandlerSpec provides a compile-time checked handler signature for durable tasks.
type TypedDurableHandlerSpec[T proto.Message] struct {
	Make func() T
	Fn   func(ctx context.Context, payload T) (any, error)
}

// TypedDurableRegistry collects typed durable handlers and exposes the untyped map.
type TypedDurableRegistry struct {
	handlers map[string]DurableHandlerSpec
}

// NewTypedDurableRegistry constructs a registry for typed durable handlers.
func NewTypedDurableRegistry() *TypedDurableRegistry {
	return &TypedDurableRegistry{
		handlers: make(map[string]DurableHandlerSpec),
	}
}

// Add registers an untyped durable handler spec into the registry.
func (r *TypedDurableRegistry) Add(name string, spec DurableHandlerSpec) error {
	if r == nil {
		return ErrHandlerRegistryNil
	}

	return r.register(name, spec)
}

// Handlers returns a defensive copy of the registered durable handler map.
func (r *TypedDurableRegistry) Handlers() map[string]DurableHandlerSpec {
	if len(r.handlers) == 0 {
		return map[string]DurableHandlerSpec{}
	}

	out := make(map[string]DurableHandlerSpec, len(r.handlers))
	maps.Copy(out, r.handlers)

	return out
}

func (r *TypedDurableRegistry) register(name string, spec DurableHandlerSpec) error {
	if name == "" {
		return ErrHandlerNameRequired
	}

	if spec.Make == nil || spec.Fn == nil {
		return ErrHandlerSpecInvalid
	}

	if r.handlers == nil {
		r.handlers = make(map[string]DurableHandlerSpec)
	}

	if _, exists := r.handlers[name]; exists {
		return ewrap.Wrapf(ErrHandlerAlreadyRegistered, "durable handler %q", name)
	}

	r.handlers[name] = spec

	return nil
}

// AddTypedDurableHandler registers a typed durable handler spec into the registry.
func AddTypedDurableHandler[T proto.Message](r *TypedDurableRegistry, name string, spec TypedDurableHandlerSpec[T]) error {
	if r == nil {
		return ErrHandlerRegistryNil
	}

	return r.register(name, TypedDurableHandler(spec))
}

// TypedDurableHandler converts a typed spec into the untyped DurableHandlerSpec.
func TypedDurableHandler[T proto.Message](spec TypedDurableHandlerSpec[T]) DurableHandlerSpec {
	return DurableHandlerSpec{
		Make: func() proto.Message {
			return spec.Make()
		},
		Fn: func(ctx context.Context, payload proto.Message) (any, error) {
			typed, ok := payload.(T)
			if !ok {
				return nil, ErrHandlerPayloadTypeMismatch
			}

			return spec.Fn(ctx, typed)
		},
	}
}
