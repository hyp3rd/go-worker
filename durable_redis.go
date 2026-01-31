package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	redisQueuesKey         = "queues"
	redisFieldHandler      = "handler"
	redisFieldPayload      = "payload"
	redisFieldPriority     = "priority"
	redisFieldQueue        = "queue"
	redisFieldWeight       = "weight"
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
	redisLeasePayloadSize  = 10
)

// RedisDurableBackend implements a durable task backend using Redis.
type RedisDurableBackend struct {
	client       rueidis.Client
	prefix       string
	batchSize    int
	queueMu      sync.RWMutex
	defaultQueue string
	queueWeights map[string]int
	queueCursor  atomic.Uint32
	enqueue      *rueidis.Lua
	dequeue      *rueidis.Lua
	ack          *rueidis.Lua
	nack         *rueidis.Lua
	fail         *rueidis.Lua
	extend       *rueidis.Lua
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

// WithRedisDurableDefaultQueue sets the default queue name for durable tasks.
func WithRedisDurableDefaultQueue(name string) RedisDurableOption {
	return func(b *RedisDurableBackend) {
		b.defaultQueue = normalizeQueueName(name)
	}
}

// WithRedisDurableQueueWeights sets queue weights for durable task scheduling.
func WithRedisDurableQueueWeights(weights map[string]int) RedisDurableOption {
	return func(b *RedisDurableBackend) {
		b.queueWeights = copyQueueWeights(weights)
	}
}

// NewRedisDurableBackend creates a new RedisDurableBackend with the given Redis client and options.
func NewRedisDurableBackend(client rueidis.Client, opts ...RedisDurableOption) (*RedisDurableBackend, error) {
	if client == nil {
		return nil, ewrap.New("redis client is nil")
	}

	backend := &RedisDurableBackend{
		client:       client,
		prefix:       redisDefaultPrefix,
		batchSize:    redisDefaultBatchSize,
		defaultQueue: DefaultQueueName,
		queueWeights: map[string]int{},
		enqueue:      rueidis.NewLuaScript(redisEnqueueScript),
		dequeue:      rueidis.NewLuaScript(redisDequeueScript()),
		ack:          rueidis.NewLuaScript(redisAckScript),
		nack:         rueidis.NewLuaScript(redisNackScript),
		fail:         rueidis.NewLuaScript(redisFailScript),
		extend:       rueidis.NewLuaScript(redisExtendScript),
	}

	for _, opt := range opts {
		if opt != nil {
			opt(backend)
		}
	}

	return backend, nil
}

// ConfigureQueues applies default queue and weights for durable scheduling.
func (b *RedisDurableBackend) ConfigureQueues(defaultQueue string, weights map[string]int) {
	b.queueMu.Lock()
	defer b.queueMu.Unlock()

	b.defaultQueue = normalizeQueueName(defaultQueue)
	b.queueWeights = copyQueueWeights(weights)
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

	if task.Queue == "" {
		task.Queue = b.defaultQueueName()
	}

	if task.Weight <= 0 {
		task.Weight = DefaultTaskWeight
	}

	now := time.Now()

	readyAt := now
	if !task.RunAt.IsZero() && task.RunAt.After(now) {
		readyAt = task.RunAt
	}

	meta, err := encodeMetadata(task.Metadata)
	if err != nil {
		return err
	}

	taskKey := b.taskKey(task.ID)

	resp := b.enqueue.Exec(
		ctx,
		b.client,
		[]string{b.readyKey(task.Queue), taskKey, b.queuesKey()},
		[]string{
			strconv.FormatInt(now.UnixMilli(), 10),
			strconv.FormatInt(readyAt.UnixMilli(), 10),
			task.Handler,
			rueidis.BinaryString(task.Payload),
			strconv.Itoa(task.Priority),
			strconv.Itoa(task.Retries),
			strconv.FormatInt(task.RetryDelay.Milliseconds(), 10),
			meta,
			task.ID.String(),
			task.Queue,
			strconv.Itoa(task.Weight),
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

	limit = normalizeDequeueLimit(limit)
	lease = normalizeDequeueLease(lease)

	queues, weights, err := b.dequeueQueues(ctx)
	if err != nil {
		return nil, err
	}

	return b.dequeueFromQueues(ctx, queues, weights, limit, lease)
}

func normalizeDequeueLimit(limit int) int {
	if limit <= 0 {
		return 1
	}

	return limit
}

func normalizeDequeueLease(lease time.Duration) time.Duration {
	if lease <= 0 {
		return redisLeaseDefault
	}

	return lease
}

// Ack acknowledges the successful processing of a durable task.
func (b *RedisDurableBackend) Ack(ctx context.Context, lease DurableTaskLease) error {
	if ctx == nil {
		return ErrInvalidTaskContext
	}

	taskID := lease.Task.ID.String()
	taskKey := b.taskKey(lease.Task.ID)

	queue := lease.Task.Queue
	if queue == "" {
		queue = b.defaultQueueName()
	}

	resp := b.ack.Exec(ctx, b.client, []string{b.processingKey(queue), taskKey}, []string{taskID})

	err := resp.Error()
	if err != nil {
		return ewrap.Wrap(err, "ack durable task")
	}

	return nil
}

// Nack negatively acknowledges a durable task, making it available for reprocessing after a delay.
func (b *RedisDurableBackend) Nack(ctx context.Context, lease DurableTaskLease, delay time.Duration) error {
	if ctx == nil {
		return ErrInvalidTaskContext
	}

	taskID := lease.Task.ID.String()
	taskKey := b.taskKey(lease.Task.ID)

	queue := lease.Task.Queue
	if queue == "" {
		queue = b.defaultQueueName()
	}

	readyAt := time.Now().Add(delay).UnixMilli()

	resp := b.nack.Exec(
		ctx,
		b.client,
		[]string{b.readyKey(queue), b.processingKey(queue), taskKey},
		[]string{taskID, strconv.FormatInt(readyAt, 10)},
	)

	err := resp.Error()
	if err != nil {
		return ewrap.Wrap(err, "nack durable task")
	}

	return nil
}

// Fail marks a durable task as failed and moves it to the dead letter queue.
func (b *RedisDurableBackend) Fail(ctx context.Context, lease DurableTaskLease, err error) error {
	if ctx == nil {
		return ErrInvalidTaskContext
	}

	taskID := lease.Task.ID.String()
	taskKey := b.taskKey(lease.Task.ID)

	queue := lease.Task.Queue
	if queue == "" {
		queue = b.defaultQueueName()
	}

	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	}

	resp := b.fail.Exec(
		ctx,
		b.client,
		[]string{b.readyKey(queue), b.processingKey(queue), b.deadKey(), taskKey},
		[]string{taskID, strconv.FormatInt(time.Now().UnixMilli(), 10), errMsg},
	)

	err = resp.Error()
	if err != nil {
		return ewrap.Wrap(err, "fail durable task")
	}

	return nil
}

// Extend renews the processing lease for a durable task.
func (b *RedisDurableBackend) Extend(ctx context.Context, lease DurableTaskLease, leaseDuration time.Duration) error {
	if ctx == nil {
		return ErrInvalidTaskContext
	}

	if lease.Task.ID == uuid.Nil {
		return ewrap.New("durable task id is required")
	}

	if leaseDuration <= 0 {
		leaseDuration = redisLeaseDefault
	}

	taskID := lease.Task.ID.String()
	taskKey := b.taskKey(lease.Task.ID)

	queue := lease.Task.Queue
	if queue == "" {
		queue = b.defaultQueueName()
	}

	now := time.Now().UnixMilli()
	leaseAt := now + leaseDuration.Milliseconds()

	resp := b.extend.Exec(
		ctx,
		b.client,
		[]string{b.processingKey(queue), taskKey},
		[]string{
			taskID,
			strconv.FormatInt(leaseAt, 10),
			strconv.FormatInt(now, 10),
		},
	)

	err := resp.Error()
	if err != nil {
		return ewrap.Wrap(err, "extend durable task")
	}

	updated, err := resp.AsInt64()
	if err != nil {
		return ewrap.Wrap(err, "parse extend result")
	}

	if updated == 0 {
		return ErrDurableLeaseNotFound
	}

	return nil
}

func (b *RedisDurableBackend) dequeueQueues(ctx context.Context) ([]string, map[string]int, error) {
	defaultQueue, weights := b.queueWeightsSnapshot()

	queues, err := b.queueList(ctx)
	if err != nil {
		return nil, nil, err
	}

	queues = ensureQueueList(queues, defaultQueue)
	sort.Strings(queues)

	return queues, weights, nil
}

func (b *RedisDurableBackend) dequeueFromQueues(
	ctx context.Context,
	queues []string,
	weights map[string]int,
	limit int,
	lease time.Duration,
) ([]DurableTaskLease, error) {
	if len(queues) == 0 {
		return nil, nil
	}

	start := b.queueStartIndex(len(queues))

	results := make([]DurableTaskLease, 0, limit)
	for i := 0; i < len(queues) && len(results) < limit; i++ {
		queue := queues[(start+i)%len(queues)]
		weight := b.queueWeight(weights, queue)

		for j := 0; j < weight && len(results) < limit; j++ {
			leaseItem, ok, err := b.dequeueOne(ctx, queue, lease)
			if err != nil {
				return nil, err
			}

			if !ok {
				break
			}

			applyLeaseDefaults(&leaseItem, queue)
			results = append(results, leaseItem)
		}
	}

	return results, nil
}

func (b *RedisDurableBackend) queueStartIndex(queueCount int) int {
	if queueCount == 0 {
		return 0
	}

	return int(b.queueCursor.Add(1)) % queueCount
}

func (*RedisDurableBackend) queueWeight(weights map[string]int, queue string) int {
	weight := normalizeQueueWeight(weights[queue])
	if weight <= 0 {
		return DefaultQueueWeight
	}

	return weight
}

func ensureQueueList(queues []string, defaultQueue string) []string {
	if len(queues) == 0 {
		return []string{defaultQueue}
	}

	if !containsQueue(queues, defaultQueue) {
		queues = append(queues, defaultQueue)
	}

	return queues
}

func applyLeaseDefaults(lease *DurableTaskLease, queue string) {
	if lease.Task.Queue == "" {
		lease.Task.Queue = queue
	}

	if lease.Task.Weight <= 0 {
		lease.Task.Weight = DefaultTaskWeight
	}
}

// dequeueOne retrieves a single durable task lease from Redis.
func (b *RedisDurableBackend) dequeueOne(ctx context.Context, queue string, lease time.Duration) (DurableTaskLease, bool, error) {
	now := time.Now().UnixMilli()

	leaseMs := lease.Milliseconds()
	if leaseMs <= 0 {
		leaseMs = int64(30 * time.Second / time.Millisecond)
	}

	resp := b.dequeue.Exec(
		ctx,
		b.client,
		[]string{b.readyKey(queue), b.processingKey(queue), b.taskPrefixKey()},
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
func (b *RedisDurableBackend) readyKey(queue string) string {
	queue = normalizeQueueName(queue)

	return b.keyPrefix() + redisKVSeparator + redisReadyKey + redisKVSeparator + queue
}

// processingKey returns the Redis key for the processing sorted set.
func (b *RedisDurableBackend) processingKey(queue string) string {
	queue = normalizeQueueName(queue)

	return b.keyPrefix() + redisKVSeparator + redisProcessingKey + redisKVSeparator + queue
}

// deadKey returns the Redis key for the dead letter list.
func (b *RedisDurableBackend) deadKey() string {
	return b.keyPrefix() + redisKVSeparator + redisDeadKey
}

// queuesKey returns the Redis key for known queues.
func (b *RedisDurableBackend) queuesKey() string {
	return b.keyPrefix() + redisKVSeparator + redisQueuesKey
}

// taskPrefixKey returns the prefix for durable task keys.
func (b *RedisDurableBackend) taskPrefixKey() string {
	return b.keyPrefix() + redisKVSeparator + redisTaskKeyPrefix
}

// taskKey returns the Redis key for a specific durable task.
func (b *RedisDurableBackend) taskKey(id uuid.UUID) string {
	return b.taskPrefixKey() + id.String()
}

func (b *RedisDurableBackend) keyPrefix() string {
	if b.prefix == "" {
		return redisDefaultPrefix
	}

	if strings.Contains(b.prefix, "{") {
		return b.prefix
	}

	return "{" + b.prefix + "}"
}

func (b *RedisDurableBackend) defaultQueueName() string {
	b.queueMu.RLock()
	defer b.queueMu.RUnlock()

	return normalizeQueueName(b.defaultQueue)
}

func (b *RedisDurableBackend) queueWeightsSnapshot() (string, map[string]int) {
	b.queueMu.RLock()
	defaultQueue := normalizeQueueName(b.defaultQueue)
	weights := copyQueueWeights(b.queueWeights)
	b.queueMu.RUnlock()

	if len(weights) == 0 {
		weights = map[string]int{defaultQueue: DefaultQueueWeight}
	}

	if _, ok := weights[defaultQueue]; !ok {
		weights[defaultQueue] = DefaultQueueWeight
	}

	return defaultQueue, weights
}

func (b *RedisDurableBackend) queueList(ctx context.Context) ([]string, error) {
	if ctx == nil {
		return nil, ErrInvalidTaskContext
	}

	resp := b.client.Do(ctx, b.client.B().Smembers().Key(b.queuesKey()).Build())

	values, err := resp.AsStrSlice()
	if err != nil {
		return nil, ewrap.Wrap(err, "read durable queue list")
	}

	return values, nil
}

func containsQueue(queues []string, name string) bool {
	return slices.Contains(queues, name)
}

const (
	leaseIdxID = iota
	leaseIdxHandler
	leaseIdxPayload
	leaseIdxPriority
	leaseIdxRetries
	leaseIdxRetryDelayMs
	leaseIdxAttempts
	leaseIdxMetadata
	leaseIdxQueue
	leaseIdxWeight
)

// parseLease parses a durable task lease from Redis command results.
func parseLease(values []string) (DurableTaskLease, error) {
	if len(values) < redisLeasePayloadSize {
		return DurableTaskLease{}, ewrap.New("invalid lease payload")
	}

	id, err := uuid.Parse(values[leaseIdxID])
	if err != nil {
		return DurableTaskLease{}, ewrap.Wrap(err, "parse durable task id")
	}

	priority, err := strconv.Atoi(values[leaseIdxPriority])
	if err != nil {
		return DurableTaskLease{}, ewrap.Wrap(err, "parse durable priority")
	}

	retries, err := strconv.Atoi(values[leaseIdxRetries])
	if err != nil {
		return DurableTaskLease{}, ewrap.Wrap(err, "parse durable retries")
	}
	//nolint:revive
	retryDelayMs, err := strconv.ParseInt(values[leaseIdxRetryDelayMs], 10, 64)
	if err != nil {
		return DurableTaskLease{}, ewrap.Wrap(err, "parse durable retry delay")
	}

	attempts, err := strconv.Atoi(values[leaseIdxAttempts])
	if err != nil {
		return DurableTaskLease{}, ewrap.Wrap(err, "parse durable attempts")
	}

	meta, err := decodeMetadata(values[leaseIdxMetadata])
	if err != nil {
		return DurableTaskLease{}, err
	}

	queue := values[leaseIdxQueue]

	weight := 0
	if values[leaseIdxWeight] != "" {
		weight, err = strconv.Atoi(values[leaseIdxWeight])
		if err != nil {
			return DurableTaskLease{}, ewrap.Wrap(err, "parse durable weight")
		}
	}

	return DurableTaskLease{
		Task: DurableTask{
			ID:         id,
			Handler:    values[leaseIdxHandler],
			Payload:    []byte(values[leaseIdxPayload]),
			Priority:   priority,
			Queue:      queue,
			Weight:     weight,
			Retries:    retries,
			RetryDelay: time.Duration(retryDelayMs) * time.Millisecond,
			Metadata:   meta,
		},
		LeaseID:    values[leaseIdxID],
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
local queues = KEYS[3]
local now = tonumber(ARGV[1])
local readyAt = tonumber(ARGV[2])
local handler = ARGV[3]
local payload = ARGV[4]
local priority = ARGV[5]
local retries = ARGV[6]
local retryDelay = ARGV[7]
local metadata = ARGV[8]
local id = ARGV[9]
local queue = ARGV[10]
local weight = ARGV[11]

if redis.call("EXISTS", taskKey) == 1 then
  return 0
end

redis.call("HSET", taskKey,
  "` + redisFieldHandler + `", handler,
  "` + redisFieldPayload + `", payload,
  "` + redisFieldPriority + `", priority,
  "` + redisFieldQueue + `", queue,
  "` + redisFieldWeight + `", weight,
  "` + redisFieldRetries + `", retries,
  "` + redisFieldRetryDelayMs + `", retryDelay,
  "` + redisFieldAttempts + `", 0,
  "` + redisFieldReadyAtMs + `", readyAt,
  "` + redisFieldUpdatedAtMs + `", now,
  "` + redisFieldMetadata + `", metadata
)

redis.call("SADD", queues, queue)
redis.call("ZADD", ready, readyAt, id)
return 1
`

//nolint:dupword
const redisDequeueScriptTemplate = `
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
  local readyAt = redis.call("HGET", taskKey, "%s")
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
local bestWeight = nil
for _, id in ipairs(due) do
  local taskKey = taskPrefix .. id
  local prio = redis.call("HGET", taskKey, "%s")
  if prio ~= false then
    prio = tonumber(prio)
    local weight = redis.call("HGET", taskKey, "%s")
    if weight == false then
      weight = "1"
    end
    weight = tonumber(weight)
    if bestPriority == nil or prio < bestPriority or (prio == bestPriority and weight > bestWeight) then
      bestPriority = prio
      bestWeight = weight
      bestId = id
    end
  end
end

if bestId == nil then
  return {}
end

redis.call("ZREM", ready, bestId)
redis.call("ZADD", processing, now + leaseMs, bestId)

local taskKey = taskPrefix .. bestId
local attempts = redis.call("HINCRBY", taskKey, "%s", 1)
redis.call("HSET", taskKey,
  "%s", now + leaseMs,
  "%s", now
)

local handler = redis.call("HGET", taskKey, "%s")
local payload = redis.call("HGET", taskKey, "%s")
local retries = redis.call("HGET", taskKey, "%s")
local retryDelay = redis.call("HGET", taskKey, "%s")
local metadata = redis.call("HGET", taskKey, "%s")
local queue = redis.call("HGET", taskKey, "%s")
if queue == false then
  queue = ""
end

return {bestId, handler, payload, tostring(bestPriority), retries, retryDelay, tostring(attempts), metadata, queue, tostring(bestWeight)}
`

func redisDequeueScript() string {
	return fmt.Sprintf(
		redisDequeueScriptTemplate,
		redisFieldReadyAtMs,
		redisFieldPriority,
		redisFieldWeight,
		redisFieldAttempts,
		redisFieldLeaseAtMs,
		redisFieldUpdatedAtMs,
		redisFieldHandler,
		redisFieldPayload,
		redisFieldRetries,
		redisFieldRetryDelayMs,
		redisFieldMetadata,
		redisFieldQueue,
	)
}

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

const redisExtendScript = `
local processing = KEYS[1]
local taskKey = KEYS[2]
local id = ARGV[1]
local leaseAt = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

if redis.call("ZSCORE", processing, id) == false then
  return 0
end

redis.call("ZADD", processing, leaseAt, id)
redis.call("HSET", taskKey,
  "` + redisFieldLeaseAtMs + `", leaseAt,
  "` + redisFieldUpdatedAtMs + `", now
)
return 1
`
