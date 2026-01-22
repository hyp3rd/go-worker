package worker

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/hyp3rd/ewrap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"

	workerpb "github.com/hyp3rd/go-worker/pkg/worker/v1"
)

const idempotencySignatureSize = sha256.Size

// HandlerSpec describes a single handler for a gRPC method.
type HandlerSpec struct {
	// Make returns a zero value of the payload message to unmarshal into.
	Make func() protoreflect.ProtoMessage
	// Fn does the work. Your Task.Execute will call this.
	Fn func(ctx context.Context, payload protoreflect.ProtoMessage) (any, error)
}

// GRPCServer implements the generated WorkerServiceServer interface.
type GRPCServer struct {
	svc      Service
	handlers map[string]HandlerSpec

	idempotencyMu sync.Mutex
	idempotency   map[string]*idempotencyRecord
}

// NewGRPCServer creates a new gRPC server backed by the provided Service.
func NewGRPCServer(svc Service, handlers map[string]HandlerSpec) *GRPCServer {
	return &GRPCServer{
		svc:         svc,
		handlers:    handlers,
		idempotency: make(map[string]*idempotencyRecord),
	}
}

type idempotencyRecord struct {
	sig   [idempotencySignatureSize]byte
	id    string
	err   error
	ready chan struct{}
}

type payloadResult struct {
	payload protoreflect.ProtoMessage
}

// RegisterTasks registers one or more tasks with the underlying service.
func (s *GRPCServer) RegisterTasks(ctx context.Context, req *workerpb.RegisterTasksRequest) (*workerpb.RegisterTasksResponse, error) {
	ids := make([]string, len(req.GetTasks()))

	for i, taskReq := range req.GetTasks() {
		id, err := s.registerTaskFromRequest(ctx, taskReq)
		if err != nil {
			return nil, err
		}

		ids[i] = id
	}

	return &workerpb.RegisterTasksResponse{Ids: ids}, nil
}

// StreamResults streams task results back to the client.
func (s *GRPCServer) StreamResults(req *workerpb.StreamResultsRequest, stream workerpb.WorkerService_StreamResultsServer) error {
	ctx := stream.Context()

	ch, unsubscribe := s.svc.SubscribeResults(DefaultMaxTasks)
	defer unsubscribe()

	target, remaining := buildTargetSet(req.GetIds())

	for res := range ch {
		err := streamContextErr(ctx)
		if err != nil {
			return err
		}

		if res.Task == nil {
			continue
		}

		id := res.Task.ID.String()
		if !shouldStreamResult(target, id) {
			continue
		}

		err = stream.Send(makeStreamResponse(id, res))
		if err != nil {
			return ewrap.Wrap(err, "failed to send stream response")
		}

		taskStatus := res.Task.Status()
		if shouldCloseStream(req, target, &remaining, id, taskStatus) {
			return nil
		}
	}

	return nil
}

// CancelTask cancels an active task by its ID.
func (s *GRPCServer) CancelTask(_ context.Context, req *workerpb.CancelTaskRequest) (*workerpb.CancelTaskResponse, error) {
	id, err := uuid.Parse(req.GetId())
	if err != nil {
		return nil, ewrap.Wrap(err, "parse id")
	}

	err = s.svc.CancelTask(id)
	if err != nil {
		if errors.Is(err, ErrTaskNotFound) {
			return nil, status.Errorf(codes.NotFound, "task %s not found", id)
		}

		return nil, status.Errorf(codes.Internal, "cancel task: %v", err)
	}

	return &workerpb.CancelTaskResponse{}, nil
}

// GetTask returns information about a task by its ID.
func (s *GRPCServer) GetTask(_ context.Context, req *workerpb.GetTaskRequest) (*workerpb.GetTaskResponse, error) {
	id, err := uuid.Parse(req.GetId())
	if err != nil {
		return nil, ewrap.Wrap(err, "parse id")
	}

	task, err := s.svc.GetTask(id)
	if err != nil {
		if errors.Is(err, ErrTaskNotFound) {
			return nil, status.Errorf(codes.NotFound, "task %s not found", id)
		}

		return nil, status.Errorf(codes.Internal, "get task: %v", err)
	}

	resp := &workerpb.GetTaskResponse{
		Id:     task.ID.String(),
		Name:   task.Name,
		Status: task.Status().String(),
	}

	if v := task.Result(); v != nil {
		resp.Output = fmt.Sprint(v)
	}

	v := task.Error()
	if v != nil {
		resp.Error = fmt.Sprint(v)
	}

	return resp, nil
}

func (s *GRPCServer) registerTaskFromRequest(ctx context.Context, taskReq *workerpb.Task) (string, error) {
	idempotencyKey := strings.TrimSpace(taskReq.GetIdempotencyKey())

	record, existingID, err := s.beginIdempotency(ctx, idempotencyKey, taskReq)
	if err != nil {
		return "", err
	}

	if existingID != "" {
		return existingID, nil
	}

	spec, err := s.lookupHandler(taskReq.GetName(), idempotencyKey, record)
	if err != nil {
		return "", err
	}

	payload, err := s.decodePayload(taskReq, spec, idempotencyKey, record)
	if err != nil {
		return "", err
	}

	task, err := s.newTaskFromRequest(ctx, taskReq, spec, payload.payload, idempotencyKey, record)
	if err != nil {
		return "", err
	}

	return s.registerTaskWithIdempotency(ctx, task, idempotencyKey, record)
}

func (s *GRPCServer) beginIdempotency(ctx context.Context, key string, taskReq *workerpb.Task) (*idempotencyRecord, string, error) {
	if key == "" {
		return nil, "", nil
	}

	sig, err := idempotencySignature(taskReq)
	if err != nil {
		return nil, "", status.Errorf(codes.InvalidArgument, "idempotency signature for %q: %v", taskReq.GetName(), err)
	}

	existingID, record, err := s.resolveIdempotency(ctx, key, sig)
	if err != nil {
		return nil, "", err
	}

	if existingID != "" {
		return nil, existingID, nil
	}

	return record, "", nil
}

func (s *GRPCServer) lookupHandler(name, key string, record *idempotencyRecord) (HandlerSpec, error) {
	spec, ok := s.handlers[name]
	if ok {
		return spec, nil
	}

	return HandlerSpec{}, s.completeIdempotencyError(
		key,
		record,
		status.Errorf(codes.NotFound, "no handler registered for %q", name),
	)
}

func (s *GRPCServer) decodePayload(
	taskReq *workerpb.Task,
	spec HandlerSpec,
	key string,
	record *idempotencyRecord,
) (payloadResult, error) {
	if taskReq.GetPayload() == nil {
		return payloadResult{}, nil
	}

	payload := spec.Make()

	err := anypb.UnmarshalTo(taskReq.GetPayload(), payload, proto.UnmarshalOptions{})
	if err != nil {
		return payloadResult{}, s.completeIdempotencyError(
			key,
			record,
			status.Errorf(codes.InvalidArgument, "bad payload for %q: %v", taskReq.GetName(), err),
		)
	}

	return payloadResult{payload: payload}, nil
}

func (s *GRPCServer) newTaskFromRequest(
	ctx context.Context,
	taskReq *workerpb.Task,
	spec HandlerSpec,
	payload protoreflect.ProtoMessage,
	key string,
	record *idempotencyRecord,
) (*Task, error) {
	task, err := NewTask(ctx, func(ctx context.Context, _ ...any) (any, error) {
		return spec.Fn(ctx, payload) // pass the typed payload to the handler
	})
	if err != nil {
		return nil, s.completeIdempotencyError(key, record, err)
	}

	task.Name = taskReq.GetName()
	task.Description = taskReq.GetDescription()
	task.Priority = int(taskReq.GetPriority())
	task.Retries = int(taskReq.GetRetries())

	if d := taskReq.GetRetryDelay(); d != nil {
		task.RetryDelay = d.AsDuration()
	}

	return task, nil
}

func (s *GRPCServer) registerTaskWithIdempotency(
	ctx context.Context,
	task *Task,
	key string,
	record *idempotencyRecord,
) (string, error) {
	err := s.svc.RegisterTask(ctx, task)
	if err != nil {
		return "", s.completeIdempotencyError(key, record, status.Errorf(codes.Internal, "register task: %v", err))
	}

	s.completeIdempotencySuccess(key, record, task.ID.String())

	return task.ID.String(), nil
}

func (s *GRPCServer) completeIdempotencyError(key string, record *idempotencyRecord, err error) error {
	if record != nil {
		s.completeIdempotency(key, record, "", err)
	}

	return err
}

func (s *GRPCServer) completeIdempotencySuccess(key string, record *idempotencyRecord, id string) {
	if record != nil {
		s.completeIdempotency(key, record, id, nil)
	}
}

func idempotencySignature(taskReq *workerpb.Task) ([idempotencySignatureSize]byte, error) {
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(taskReq)
	if err != nil {
		return [idempotencySignatureSize]byte{}, ewrap.Wrapf(
			err,
			"marshal task request for idempotency signature calculation. Task name: %q",
			taskReq.GetName())
	}

	return sha256.Sum256(payload), nil
}

func (s *GRPCServer) resolveIdempotency(
	ctx context.Context,
	key string,
	sig [idempotencySignatureSize]byte,
) (string, *idempotencyRecord, error) {
	for {
		s.idempotencyMu.Lock()

		record, ok := s.idempotency[key]
		if !ok {
			record = &idempotencyRecord{sig: sig, ready: make(chan struct{})}
			s.idempotency[key] = record
			s.idempotencyMu.Unlock()

			return "", record, nil
		}

		if record.sig != sig {
			s.idempotencyMu.Unlock()

			return "", nil, status.Errorf(codes.AlreadyExists, "idempotency key %q already used for different task", key)
		}

		ready := record.ready
		id := record.id
		err := record.err

		s.idempotencyMu.Unlock()

		if ready != nil {
			waitErr := waitForIdempotency(ctx, ready)
			if waitErr != nil {
				return "", nil, waitErr
			}

			continue
		}

		if err != nil {
			return "", nil, err
		}

		if id == "" {
			return "", nil, status.Errorf(codes.Internal, "idempotency key %q completed without result", key)
		}

		if !s.idempotencyTaskExists(id) {
			s.forgetIdempotency(key)

			continue
		}

		return id, nil, nil
	}
}

func (s *GRPCServer) completeIdempotency(key string, record *idempotencyRecord, id string, err error) {
	s.idempotencyMu.Lock()
	defer s.idempotencyMu.Unlock()

	current, ok := s.idempotency[key]
	if !ok || current != record {
		return
	}

	record.id = id
	record.err = err

	if record.ready != nil {
		close(record.ready)
		record.ready = nil
	}
}

func waitForIdempotency(ctx context.Context, ready <-chan struct{}) error {
	select {
	case <-ready:
		return nil
	case <-ctx.Done():
		return ewrap.Wrap(status.FromContextError(ctx.Err()).Err(), "client went away; stop streaming")
	}
}

func (s *GRPCServer) idempotencyTaskExists(id string) bool {
	parsed, err := uuid.Parse(id)
	if err != nil {
		return false
	}

	_, err = s.svc.GetTask(parsed)

	return err == nil
}

func (s *GRPCServer) forgetIdempotency(key string) {
	s.idempotencyMu.Lock()
	delete(s.idempotency, key)
	s.idempotencyMu.Unlock()
}

func streamContextErr(ctx context.Context) error {
	select {
	case <-ctx.Done():
		// client went away; stop streaming (tasks continue)
		return ewrap.Wrap(status.FromContextError(ctx.Err()).Err(), "client went away; stop streaming")
	default:
		return nil
	}
}

func buildTargetSet(ids []string) (map[string]struct{}, int) {
	target := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		target[id] = struct{}{}
	}

	return target, len(target)
}

func shouldStreamResult(target map[string]struct{}, id string) bool {
	if len(target) == 0 {
		return true
	}

	_, ok := target[id]

	return ok
}

func makeStreamResponse(id string, res Result) *workerpb.StreamResultsResponse {
	out := &workerpb.StreamResultsResponse{Id: id}

	if res.Result != nil {
		out.Output = fmt.Sprint(res.Result)
	}

	if res.Error != nil {
		out.Error = res.Error.Error()
	}

	return out
}

func shouldCloseStream(
	req *workerpb.StreamResultsRequest,
	target map[string]struct{},
	remaining *int,
	id string,
	taskStatus TaskStatus,
) bool {
	if len(target) == 0 {
		return false
	}

	if !isTerminalStatus(taskStatus) {
		return false
	}

	if _, ok := target[id]; ok {
		delete(target, id)

		*remaining--
	}

	return req.GetCloseOnCompletion() && *remaining == 0
}

var _ workerpb.WorkerServiceServer = (*GRPCServer)(nil)
