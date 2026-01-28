package worker

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/hyp3rd/ewrap"
	"github.com/redis/rueidis"
)

const (
	redisDefaultPrefix     = "go-worker"
	redisTaskKeyPrefix     = "task:"
	redisReadyKey          = "ready"
	redisProcessingKey     = "processing"
	redisDeadKey           = "dead"
	redisFieldHandler      = "handler"
	redisFieldPayload      = "payload"
	redisFieldPriority     = "priority"
	redisFieldRetries      = "retries"
	redisFieldRetryDelayMs = "retry_delay_ms"
	redisFieldAttempts     = "attempts"
	redisFieldReadyAtMs    = "ready_at_ms"
	redisFieldMetadata     = "metadata"
	redisFieldLastError    = "last_error"
	redisFieldFailedAtMs   = "failed_at_ms"
	redisFieldLeaseAtMs    = "lease_at_ms"
	redisFieldUpdatedAtMs  = "updated_at_ms"
	redisKVSeparator       = ":"
	redisDefaultBatchSize  = 50
	redisLeaseDefault      = 30 * time.Second
	redisLeasePayloadSize  = 8
)

// RedisDurableBackend implements a durable task backend using Redis.
type RedisDurableBackend struct {
	client    rueidis.Client
	prefix    string
	batchSize int
	enqueue   *rueidis.Lua
	dequeue   *rueidis.Lua
	ack       *rueidis.Lua
	nack      *rueidis.Lua
	fail      *rueidis.Lua
}

// RedisDurableOption defines an option for configuring RedisDurableBackend.
type RedisDurableOption func(*RedisDurableBackend)

// WithRedisDurablePrefix sets the key prefix for Redis durable backend.
func WithRedisDurablePrefix(prefix string) RedisDurableOption {
	return func(b *RedisDurableBackend) {
		if prefix != "" {
			b.prefix = prefix
		}
	}
}

// WithRedisDurableBatchSize sets the batch size for dequeuing tasks.
func WithRedisDurableBatchSize(size int) RedisDurableOption {
	return func(b *RedisDurableBackend) {
		if size > 0 {
			b.batchSize = size
		}
	}
}

// NewRedisDurableBackend creates a new RedisDurableBackend with the given Redis client and options.
func NewRedisDurableBackend(client rueidis.Client, opts ...RedisDurableOption) (*RedisDurableBackend, error) {
	if client == nil {
		return nil, ewrap.New("redis client is nil")
	}

	backend := &RedisDurableBackend{
		client:    client,
		prefix:    redisDefaultPrefix,
		batchSize: redisDefaultBatchSize,
		enqueue:   rueidis.NewLuaScript(redisEnqueueScript),
		dequeue:   rueidis.NewLuaScript(redisDequeueScript),
		ack:       rueidis.NewLuaScript(redisAckScript),
		nack:      rueidis.NewLuaScript(redisNackScript),
		fail:      rueidis.NewLuaScript(redisFailScript),
	}

	for _, opt := range opts {
		if opt != nil {
			opt(backend)
		}
	}

	return backend, nil
}

// Enqueue adds a new durable task to the Redis backend.
func (b *RedisDurableBackend) Enqueue(ctx context.Context, task DurableTask) error {
	if ctx == nil {
		return ErrInvalidTaskContext
	}

	if task.ID == uuid.Nil {
		return ewrap.New("durable task id is required")
	}

	if task.Handler == "" {
		return ewrap.New("durable task handler is required")
	}

	readyAt := time.Now().UnixMilli()

	meta, err := encodeMetadata(task.Metadata)
	if err != nil {
		return err
	}

	taskKey := b.taskKey(task.ID)

	resp := b.enqueue.Exec(
		ctx,
		b.client,
		[]string{b.readyKey(), taskKey},
		[]string{
			strconv.FormatInt(readyAt, 10),
			task.Handler,
			rueidis.BinaryString(task.Payload),
			strconv.Itoa(task.Priority),
			strconv.Itoa(task.Retries),
			strconv.FormatInt(task.RetryDelay.Milliseconds(), 10),
			meta,
			task.ID.String(),
		},
	)

	err = resp.Error()
	if err != nil {
		return ewrap.Wrap(err, "enqueue durable task")
	}

	result, err := resp.AsInt64()
	if err != nil {
		return ewrap.Wrap(err, "parse enqueue result")
	}

	if result != 1 {
		return ewrap.New("durable task already exists")
	}

	return nil
}

// Dequeue retrieves a batch of durable tasks from the Redis backend with a lease.
func (b *RedisDurableBackend) Dequeue(ctx context.Context, limit int, lease time.Duration) ([]DurableTaskLease, error) {
	if ctx == nil {
		return nil, ErrInvalidTaskContext
	}

	if limit <= 0 {
		limit = 1
	}

	if lease <= 0 {
		lease = redisLeaseDefault
	}

	results := make([]DurableTaskLease, 0, limit)
	for range limit {
		leaseItem, ok, err := b.dequeueOne(ctx, lease)
		if err != nil {
			return nil, err
		}

		if !ok {
			break
		}

		results = append(results, leaseItem)
	}

	return results, nil
}

// Ack acknowledges the successful processing of a durable task.
func (b *RedisDurableBackend) Ack(ctx context.Context, lease DurableTaskLease) error {
	if ctx == nil {
		return ErrInvalidTaskContext
	}

	taskID := lease.Task.ID.String()
	taskKey := b.taskKey(lease.Task.ID)

	resp := b.ack.Exec(ctx, b.client, []string{b.processingKey(), taskKey}, []string{taskID})

	return ewrap.Wrap(resp.Error(), "ack durable task")
}

// Nack negatively acknowledges a durable task, making it available for reprocessing after a delay.
func (b *RedisDurableBackend) Nack(ctx context.Context, lease DurableTaskLease, delay time.Duration) error {
	if ctx == nil {
		return ErrInvalidTaskContext
	}

	taskID := lease.Task.ID.String()
	taskKey := b.taskKey(lease.Task.ID)
	readyAt := time.Now().Add(delay).UnixMilli()

	resp := b.nack.Exec(
		ctx,
		b.client,
		[]string{b.readyKey(), b.processingKey(), taskKey},
		[]string{taskID, strconv.FormatInt(readyAt, 10)},
	)

	return ewrap.Wrap(resp.Error(), "nack durable task")
}

// Fail marks a durable task as failed and moves it to the dead letter queue.
func (b *RedisDurableBackend) Fail(ctx context.Context, lease DurableTaskLease, err error) error {
	if ctx == nil {
		return ErrInvalidTaskContext
	}

	taskID := lease.Task.ID.String()
	taskKey := b.taskKey(lease.Task.ID)

	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	}

	resp := b.fail.Exec(
		ctx,
		b.client,
		[]string{b.readyKey(), b.processingKey(), b.deadKey(), taskKey},
		[]string{taskID, strconv.FormatInt(time.Now().UnixMilli(), 10), errMsg},
	)

	return ewrap.Wrap(resp.Error(), "fail durable task")
}

// dequeueOne retrieves a single durable task lease from Redis.
func (b *RedisDurableBackend) dequeueOne(ctx context.Context, lease time.Duration) (DurableTaskLease, bool, error) {
	now := time.Now().UnixMilli()

	leaseMs := lease.Milliseconds()
	if leaseMs <= 0 {
		leaseMs = int64(30 * time.Second / time.Millisecond)
	}

	resp := b.dequeue.Exec(
		ctx,
		b.client,
		[]string{b.readyKey(), b.processingKey(), b.taskPrefixKey()},
		[]string{
			strconv.FormatInt(now, 10),
			strconv.FormatInt(leaseMs, 10),
			strconv.Itoa(b.batchSize),
		},
	)

	err := resp.Error()
	if err != nil {
		return DurableTaskLease{}, false, ewrap.Wrap(err, "dequeue durable task")
	}

	values, err := resp.AsStrSlice()
	if err != nil {
		return DurableTaskLease{}, false, ewrap.Wrap(err, "dequeue durable task")
	}

	if len(values) == 0 {
		return DurableTaskLease{}, false, nil
	}

	leaseItem, err := parseLease(values)
	if err != nil {
		return DurableTaskLease{}, false, ewrap.Wrap(err, "dequeue durable task")
	}

	return leaseItem, true, nil
}

// readyKey returns the Redis key for the ready sorted set.
func (b *RedisDurableBackend) readyKey() string {
	return b.prefix + redisKVSeparator + redisReadyKey
}

// processingKey returns the Redis key for the processing sorted set.
func (b *RedisDurableBackend) processingKey() string {
	return b.prefix + redisKVSeparator + redisProcessingKey
}

// deadKey returns the Redis key for the dead letter list.
func (b *RedisDurableBackend) deadKey() string {
	return b.prefix + redisKVSeparator + redisDeadKey
}

// taskPrefixKey returns the prefix for durable task keys.
func (b *RedisDurableBackend) taskPrefixKey() string {
	return b.prefix + redisKVSeparator + redisTaskKeyPrefix
}

// taskKey returns the Redis key for a specific durable task.
func (b *RedisDurableBackend) taskKey(id uuid.UUID) string {
	return b.taskPrefixKey() + id.String()
}

// parseLease parses a durable task lease from Redis command results.
func parseLease(values []string) (DurableTaskLease, error) {
	if len(values) < redisLeasePayloadSize {
		return DurableTaskLease{}, ewrap.New("invalid lease payload")
	}

	id, err := uuid.Parse(values[0])
	if err != nil {
		return DurableTaskLease{}, ewrap.Wrap(err, "parse durable task id")
	}

	priority, err := strconv.Atoi(values[3])
	if err != nil {
		return DurableTaskLease{}, ewrap.Wrap(err, "parse durable priority")
	}

	retries, err := strconv.Atoi(values[4])
	if err != nil {
		return DurableTaskLease{}, ewrap.Wrap(err, "parse durable retries")
	}
	//nolint:revive
	retryDelayMs, err := strconv.ParseInt(values[5], 10, 64)
	if err != nil {
		return DurableTaskLease{}, ewrap.Wrap(err, "parse durable retry delay")
	}

	attempts, err := strconv.Atoi(values[6])
	if err != nil {
		return DurableTaskLease{}, ewrap.Wrap(err, "parse durable attempts")
	}

	meta, err := decodeMetadata(values[7])
	if err != nil {
		return DurableTaskLease{}, err
	}

	return DurableTaskLease{
		Task: DurableTask{
			ID:         id,
			Handler:    values[1],
			Payload:    []byte(values[2]),
			Priority:   priority,
			Retries:    retries,
			RetryDelay: time.Duration(retryDelayMs) * time.Millisecond,
			Metadata:   meta,
		},
		LeaseID:    values[0],
		Attempts:   attempts,
		MaxRetries: retries,
	}, nil
}

// encodeMetadata encodes task metadata into a JSON string.
func encodeMetadata(meta map[string]string) (string, error) {
	if len(meta) == 0 {
		return "", nil
	}

	data, err := json.Marshal(meta)
	if err != nil {
		return "", ewrap.Wrap(err, "encode metadata")
	}

	return string(data), nil
}

// decodeMetadata decodes task metadata from a JSON string.
func decodeMetadata(raw string) (map[string]string, error) {
	if raw == "" {
		return map[string]string{}, nil
	}

	out := map[string]string{}

	err := json.Unmarshal([]byte(raw), &out)
	if err != nil {
		return nil, ewrap.Wrap(err, "decode metadata")
	}

	return out, nil
}

const redisEnqueueScript = `
local ready = KEYS[1]
local taskKey = KEYS[2]
local now = tonumber(ARGV[1])
local handler = ARGV[2]
local payload = ARGV[3]
local priority = ARGV[4]
local retries = ARGV[5]
local retryDelay = ARGV[6]
local metadata = ARGV[7]
local id = ARGV[8]

if redis.call("EXISTS", taskKey) == 1 then
  return 0
end

redis.call("HSET", taskKey,
  "` + redisFieldHandler + `", handler,
  "` + redisFieldPayload + `", payload,
  "` + redisFieldPriority + `", priority,
  "` + redisFieldRetries + `", retries,
  "` + redisFieldRetryDelayMs + `", retryDelay,
  "` + redisFieldAttempts + `", 0,
  "` + redisFieldReadyAtMs + `", now,
  "` + redisFieldUpdatedAtMs + `", now,
  "` + redisFieldMetadata + `", metadata
)

redis.call("ZADD", ready, now, id)
return 1
`

//nolint:revive
const redisDequeueScript = `
local ready = KEYS[1]
local processing = KEYS[2]
local taskPrefix = KEYS[3]
local now = tonumber(ARGV[1])
local leaseMs = tonumber(ARGV[2])
local batch = tonumber(ARGV[3])

local expired = redis.call("ZRANGEBYSCORE", processing, "-inf", now, "LIMIT", 0, batch)
for _, id in ipairs(expired) do
  redis.call("ZREM", processing, id)
  local taskKey = taskPrefix .. id
  local readyAt = redis.call("HGET", taskKey, "` + redisFieldReadyAtMs + `")
  if readyAt == false then
    readyAt = now
  end
  redis.call("ZADD", ready, readyAt, id)
end

local due = redis.call("ZRANGEBYSCORE", ready, "-inf", now, "LIMIT", 0, batch)
if #due == 0 then
  return {}
end

local bestId = nil
local bestPriority = nil
for _, id in ipairs(due) do
  local taskKey = taskPrefix .. id
  local prio = redis.call("HGET", taskKey, "` + redisFieldPriority + "\")\n  if prio ~= false then\n    prio = tonumber(prio)\n    if bestPriority == nil or prio < bestPriority then\n      bestPriority = prio\n      bestId = id\n    end\n  if bestId == nil then\n  return {}\nend\n\nredis.call(\"ZREM\", ready, bestId)\nredis.call(\"ZADD\", processing, now + leaseMs, bestId)\n\nlocal taskKey = taskPrefix .. bestId\nlocal attempts = redis.call(\"HINCRBY\", taskKey," + redisFieldAttempts + `", 1)
redis.call("HSET", taskKey,
  "` + redisFieldLeaseAtMs + `", now,
  "` + redisFieldUpdatedAtMs + `", now
)

local handler = redis.call("HGET", taskKey, "` + redisFieldHandler + `")
local payload = redis.call("HGET", taskKey, "` + redisFieldPayload + `")
local retries = redis.call("HGET", taskKey, "` + redisFieldRetries + `")
local retryDelay = redis.call("HGET", taskKey, "` + redisFieldRetryDelayMs + `")
local metadata = redis.call("HGET", taskKey, "` + redisFieldMetadata + `")

return {bestId, handler, payload, tostring(bestPriority), retries, retryDelay, tostring(attempts), metadata}
`

const redisAckScript = `
local processing = KEYS[1]
local taskKey = KEYS[2]
local id = ARGV[1]

redis.call("ZREM", processing, id)
redis.call("DEL", taskKey)
return 1
`

const redisNackScript = `
local ready = KEYS[1]
local processing = KEYS[2]
local taskKey = KEYS[3]
local id = ARGV[1]
local readyAt = tonumber(ARGV[2])

redis.call("ZREM", processing, id)
redis.call("HSET", taskKey, "` + redisFieldReadyAtMs + `", readyAt, "` + redisFieldUpdatedAtMs + `", readyAt)
redis.call("ZADD", ready, readyAt, id)
return 1
`

const redisFailScript = `
local ready = KEYS[1]
local processing = KEYS[2]
local dead = KEYS[3]
local taskKey = KEYS[4]
local id = ARGV[1]
local now = ARGV[2]
local err = ARGV[3]

redis.call("ZREM", processing, id)
redis.call("ZREM", ready, id)
redis.call("HSET", taskKey,
  "` + redisFieldLastError + `", err,
  "` + redisFieldFailedAtMs + `", now,
  "` + redisFieldUpdatedAtMs + `", now
)
redis.call("LPUSH", dead, id)
return 1
`
