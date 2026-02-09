package worker

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/hyp3rd/ewrap"
	sectconv "github.com/hyp3rd/sectools/pkg/converters"
	"github.com/redis/rueidis"
)

const (
	adminDLQScanLimit            = 2000
	adminDLQDefaultSize          = 100
	adminDLQQueueIndex           = 0
	adminDLQHandlerIndex         = 1
	adminDLQAttemptsIndex        = 2
	adminDLQFailedAtIndex        = 3
	adminDLQUpdatedAtIndex       = 4
	adminDLQLastErrorIndex       = 5
	adminDLQMetadataIndex        = 6
	adminDLQReplayMaxIDs         = 1000
	adminSecondsPerMinute        = 60
	adminMinutesPerHour          = 60
	adminHoursPerDay             = 24
	adminDLQReplayByIDArgsPrefix = 3
	parseIntBitSize              = 64
	adminJobsKeyName             = "admin:jobs"
	adminJobDefaultDockerfile    = "Dockerfile"
)

//nolint:revive,dupword
const adminDLQReplayScript = "\nlocal dead = KEYS[1]\nlocal taskPrefix = KEYS[2]\nlocal queuesKey = KEYS[3]\nlocal now = tonumber(ARGV[1])\nlocal limit = tonumber(ARGV[2])\nlocal prefix = ARGV[3]\nlocal defaultQueue = ARGV[4]\n\nlocal moved = 0\nfor i = 1, limit do\n  local id = redis.call(\"RPOP\", dead)\n  if not id then\n    break\n  end\n  local taskKey = taskPrefix .. id\n  if redis.call(\"EXISTS\", taskKey) == 1 then\n    local queue = redis.call(\"HGET\", taskKey, \"queue\")\n    if queue == false or queue == \"\" then\n      queue = defaultQueue\n    end\n    local readyKey = prefix .. \":ready:\" .. queue\n    redis.call(\"HSET\", taskKey, \"ready_at_ms\", now, \"updated_at_ms\", now)\n    redis.call(\"ZADD\", readyKey, now, id)\n    redis.call(\"SADD\", queuesKey, queue)\n    moved = moved + 1\n  end\nend\nreturn moved"

//nolint:revive,dupword
const adminDLQReplayByIDScript = "\nlocal dead = KEYS[1]\nlocal taskPrefix = KEYS[2]\nlocal queuesKey = KEYS[3]\nlocal prefix = ARGV[1]\nlocal defaultQueue = ARGV[2]\nlocal now = tonumber(ARGV[3])\n\nlocal moved = 0\nfor i = 4, #ARGV do\n  local id = ARGV[i]\n  local taskKey = taskPrefix .. id\n  if redis.call(\"EXISTS\", taskKey) == 1 then\n    local queue = redis.call(\"HGET\", taskKey, \"queue\")\n    if queue == false or queue == \"\" then\n      queue = defaultQueue\n    end\n    local readyKey = prefix .. \":ready:\" .. queue\n    local processingKey = prefix .. \":processing:\" .. queue\n    redis.call(\"LREM\", dead, 0, id)\n    redis.call(\"ZREM\", readyKey, id)\n    redis.call(\"ZREM\", processingKey, id)\n    redis.call(\"HSET\", taskKey, \"ready_at_ms\", now, \"updated_at_ms\", now)\n    redis.call(\"ZADD\", readyKey, now, id)\n    redis.call(\"SADD\", queuesKey, queue)\n    moved = moved + 1\n  end\nend\nreturn moved"

// AdminOverview retrieves an overview of the durable backend status.
func (b *RedisDurableBackend) AdminOverview(ctx context.Context) (AdminOverview, error) {
	if ctx == nil {
		return AdminOverview{}, ErrInvalidTaskContext
	}

	queues, weights, err := b.dequeueQueues(ctx)
	if err != nil {
		return AdminOverview{}, err
	}

	totalReady := int64(0)
	totalProcessing := int64(0)

	for _, queue := range queues {
		readyCount, processingCount, err := b.queueCounts(ctx, queue)
		if err != nil {
			return AdminOverview{}, err
		}

		totalReady += readyCount
		totalProcessing += processingCount
		_ = weights[queue]
	}

	paused, err := b.isPaused(ctx)
	if err != nil {
		return AdminOverview{}, err
	}

	globalRate := "disabled"

	if b.rateLimit != nil {
		active, err := b.hasKey(ctx, b.globalRateKey())
		if err != nil {
			return AdminOverview{}, err
		}

		if active {
			globalRate = "active"
		} else {
			globalRate = "idle"
		}
	}

	leaderLock := "disabled"
	lease := adminNotAvailable

	if b.leader != nil {
		ttl, err := b.keyTTL(ctx, b.leaderKey())
		if err != nil {
			return AdminOverview{}, err
		}

		if ttl > 0 {
			leaderLock = "active"
			lease = formatAdminDuration(ttl)
		} else {
			leaderLock = "idle"
		}
	}

	return AdminOverview{
		ActiveWorkers: -1,
		QueuedTasks:   totalReady,
		Queues:        len(queues),
		AvgLatencyMs:  -1,
		P95LatencyMs:  -1,
		Coordination: AdminCoordination{
			GlobalRateLimit: globalRate,
			LeaderLock:      leaderLock,
			Lease:           lease,
			Paused:          paused,
		},
	}, nil
}

// AdminQueues returns summaries for all queues.
func (b *RedisDurableBackend) AdminQueues(ctx context.Context) ([]AdminQueueSummary, error) {
	if ctx == nil {
		return nil, ErrInvalidTaskContext
	}

	queues, weights, err := b.dequeueQueues(ctx)
	if err != nil {
		return nil, err
	}

	queues = mergeQueueNames(queues, weights)

	pausedQueues, err := b.pausedQueues(ctx)
	if err != nil {
		return nil, err
	}

	deadCounts, err := b.deadCountsByQueue(ctx, queues, adminDLQScanLimit)
	if err != nil {
		return nil, err
	}

	results := make([]AdminQueueSummary, 0, len(queues))
	for _, queue := range queues {
		readyCount, processingCount, err := b.queueCounts(ctx, queue)
		if err != nil {
			return nil, err
		}

		deadCount, ok := deadCounts[queue]
		if !ok {
			deadCount = -1
		}

		results = append(results, AdminQueueSummary{
			Name:       queue,
			Ready:      readyCount,
			Processing: processingCount,
			Dead:       deadCount,
			Weight:     b.queueWeight(weights, queue),
			Paused:     pausedQueues[queue],
		})
	}

	return results, nil
}

// AdminQueue returns a summary for a single queue.
func (b *RedisDurableBackend) AdminQueue(ctx context.Context, name string) (AdminQueueSummary, error) {
	if ctx == nil {
		return AdminQueueSummary{}, ErrInvalidTaskContext
	}

	name = strings.TrimSpace(name)
	if name == "" {
		return AdminQueueSummary{}, ErrAdminQueueNameRequired
	}

	name = normalizeQueueName(name)

	queues, weights, err := b.dequeueQueues(ctx)
	if err != nil {
		return AdminQueueSummary{}, err
	}

	if !containsQueue(queues, name) {
		if _, ok := weights[name]; !ok {
			return AdminQueueSummary{}, ErrAdminQueueNotFound
		}
	}

	pausedQueues, err := b.pausedQueues(ctx)
	if err != nil {
		return AdminQueueSummary{}, err
	}

	readyCount, processingCount, err := b.queueCounts(ctx, name)
	if err != nil {
		return AdminQueueSummary{}, err
	}

	deadCounts, err := b.deadCountsByQueue(ctx, []string{name}, adminDLQScanLimit)
	if err != nil {
		return AdminQueueSummary{}, err
	}

	deadCount, ok := deadCounts[name]
	if !ok {
		deadCount = -1
	}

	return AdminQueueSummary{
		Name:       name,
		Ready:      readyCount,
		Processing: processingCount,
		Dead:       deadCount,
		Weight:     b.queueWeight(weights, name),
		Paused:     pausedQueues[name],
	}, nil
}

// AdminPauseQueue pauses or resumes a specific queue.
func (b *RedisDurableBackend) AdminPauseQueue(ctx context.Context, name string, paused bool) (AdminQueueSummary, error) {
	if ctx == nil {
		return AdminQueueSummary{}, ErrInvalidTaskContext
	}

	name = strings.TrimSpace(name)
	if name == "" {
		return AdminQueueSummary{}, ErrAdminQueueNameRequired
	}

	name = normalizeQueueName(name)

	queues, err := b.queueList(ctx)
	if err != nil {
		return AdminQueueSummary{}, err
	}

	queues = ensureQueueList(queues, b.defaultQueueName())
	if !containsQueue(queues, name) {
		return AdminQueueSummary{}, ErrAdminQueueNotFound
	}

	if paused {
		resp := b.client.Do(ctx, b.client.B().Sadd().Key(b.pausedQueuesKey()).Member(name).Build())

		err := resp.Error()
		if err != nil {
			return AdminQueueSummary{}, ewrap.Wrap(err, "pause queue")
		}
	} else {
		resp := b.client.Do(ctx, b.client.B().Srem().Key(b.pausedQueuesKey()).Member(name).Build())

		err := resp.Error()
		if err != nil {
			return AdminQueueSummary{}, ewrap.Wrap(err, "resume queue")
		}
	}

	return b.AdminQueue(ctx, name)
}

// AdminSetQueueWeight updates the scheduler weight for a queue.
func (b *RedisDurableBackend) AdminSetQueueWeight(ctx context.Context, name string, weight int) (AdminQueueSummary, error) {
	if ctx == nil {
		return AdminQueueSummary{}, ErrInvalidTaskContext
	}

	name = strings.TrimSpace(name)
	if name == "" {
		return AdminQueueSummary{}, ErrAdminQueueNameRequired
	}

	if weight <= 0 {
		return AdminQueueSummary{}, ErrAdminQueueWeightInvalid
	}

	weight = normalizeQueueWeight(weight)

	name = normalizeQueueName(name)

	b.queueMu.Lock()

	if b.queueWeights == nil {
		b.queueWeights = map[string]int{}
	}

	b.queueWeights[name] = weight
	b.queueMu.Unlock()

	resp := b.client.Do(ctx, b.client.B().Sadd().Key(b.queuesKey()).Member(name).Build())

	err := resp.Error()
	if err != nil {
		return AdminQueueSummary{}, ewrap.Wrap(err, "register queue")
	}

	return b.AdminQueue(ctx, name)
}

// AdminResetQueueWeight resets a queue weight to the default.
func (b *RedisDurableBackend) AdminResetQueueWeight(ctx context.Context, name string) (AdminQueueSummary, error) {
	if ctx == nil {
		return AdminQueueSummary{}, ErrInvalidTaskContext
	}

	name = strings.TrimSpace(name)
	if name == "" {
		return AdminQueueSummary{}, ErrAdminQueueNameRequired
	}

	name = normalizeQueueName(name)

	b.queueMu.Lock()
	delete(b.queueWeights, name)
	b.queueMu.Unlock()

	return b.AdminQueue(ctx, name)
}

// AdminSchedules returns cron schedule data if supported.
func (*RedisDurableBackend) AdminSchedules(ctx context.Context) ([]AdminSchedule, error) {
	if ctx == nil {
		return nil, ErrInvalidTaskContext
	}

	return nil, ErrAdminUnsupported
}

// AdminScheduleFactories is not supported by the Redis durable backend.
func (*RedisDurableBackend) AdminScheduleFactories(ctx context.Context) ([]AdminScheduleFactory, error) {
	if ctx == nil {
		return nil, ErrInvalidTaskContext
	}

	return nil, ErrAdminUnsupported
}

// AdminScheduleEvents is not supported by the Redis durable backend.
func (*RedisDurableBackend) AdminScheduleEvents(ctx context.Context, _ AdminScheduleEventFilter) (AdminScheduleEventPage, error) {
	if ctx == nil {
		return AdminScheduleEventPage{}, ErrInvalidTaskContext
	}

	return AdminScheduleEventPage{}, ErrAdminUnsupported
}

// AdminCreateSchedule is not supported by the Redis durable backend.
func (*RedisDurableBackend) AdminCreateSchedule(ctx context.Context, _ AdminScheduleSpec) (AdminSchedule, error) {
	if ctx == nil {
		return AdminSchedule{}, ErrInvalidTaskContext
	}

	return AdminSchedule{}, ErrAdminUnsupported
}

// AdminDeleteSchedule is not supported by the Redis durable backend.
func (*RedisDurableBackend) AdminDeleteSchedule(ctx context.Context, _ string) (bool, error) {
	if ctx == nil {
		return false, ErrInvalidTaskContext
	}

	return false, ErrAdminUnsupported
}

// AdminPauseSchedule is not supported by the Redis durable backend.
func (*RedisDurableBackend) AdminPauseSchedule(ctx context.Context, _ string, _ bool) (AdminSchedule, error) {
	if ctx == nil {
		return AdminSchedule{}, ErrInvalidTaskContext
	}

	return AdminSchedule{}, ErrAdminUnsupported
}

// AdminPauseSchedules is not supported by the Redis durable backend.
func (*RedisDurableBackend) AdminPauseSchedules(ctx context.Context, _ bool) (int, error) {
	if ctx == nil {
		return 0, ErrInvalidTaskContext
	}

	return 0, ErrAdminUnsupported
}

// AdminRunSchedule is not supported by the Redis durable backend.
func (*RedisDurableBackend) AdminRunSchedule(ctx context.Context, _ string) (string, error) {
	if ctx == nil {
		return "", ErrInvalidTaskContext
	}

	return "", ErrAdminUnsupported
}

// AdminDLQ returns entries from the dead letter queue.
func (b *RedisDurableBackend) AdminDLQ(ctx context.Context, filter AdminDLQFilter) (AdminDLQPage, error) {
	if ctx == nil {
		return AdminDLQPage{}, ErrInvalidTaskContext
	}

	filters := normalizeAdminDLQFilter(filter)

	total, err := b.dlqTotal(ctx)
	if err != nil {
		return AdminDLQPage{}, err
	}

	if total <= 0 {
		return AdminDLQPage{Entries: []AdminDLQEntry{}, Total: 0}, nil
	}

	if !filters.hasFilters() {
		return b.dlqPageNoFilters(ctx, filters, total)
	}

	return b.dlqPageWithFilters(ctx, filters, total)
}

// AdminDLQEntry returns detailed DLQ entry information.
func (b *RedisDurableBackend) AdminDLQEntry(ctx context.Context, id string) (AdminDLQEntryDetail, error) {
	if ctx == nil {
		return AdminDLQEntryDetail{}, ErrInvalidTaskContext
	}

	id = strings.TrimSpace(id)
	if id == "" {
		return AdminDLQEntryDetail{}, ErrAdminDLQEntryIDRequired
	}

	taskKey := b.taskPrefixKey() + id

	fields, err := b.dlqEntryFields(ctx, taskKey, id)
	if err != nil {
		return AdminDLQEntryDetail{}, err
	}

	payloadSize, err := b.dlqEntryPayloadSize(ctx, taskKey)
	if err != nil {
		return AdminDLQEntryDetail{}, err
	}

	age := dlqEntryAge(fields.failedAt, fields.updatedAt)

	return AdminDLQEntryDetail{
		ID:          id,
		Queue:       fields.queue,
		Handler:     fields.handler,
		Attempts:    fields.attempts,
		AgeMs:       age.Milliseconds(),
		FailedAtMs:  fields.failedAt,
		UpdatedAtMs: fields.updatedAt,
		LastError:   fields.lastError,
		PayloadSize: payloadSize,
		Metadata:    fields.metadata,
	}, nil
}

type dlqEntryFields struct {
	queue     string
	handler   string
	attempts  int
	failedAt  int64
	updatedAt int64
	lastError string
	metadata  map[string]string
}

func (b *RedisDurableBackend) dlqEntryFields(ctx context.Context, taskKey, id string) (dlqEntryFields, error) {
	err := b.ensureDLQEntryExists(ctx, taskKey, id)
	if err != nil {
		return dlqEntryFields{}, err
	}

	values, err := b.fetchDLQEntryValues(ctx, taskKey, id)
	if err != nil {
		return dlqEntryFields{}, err
	}

	queue := valueAt(values, adminDLQQueueIndex)
	if queue == "" {
		queue = b.defaultQueueName()
	}

	handler := valueAt(values, adminDLQHandlerIndex)
	attempts := parseInt(valueAt(values, adminDLQAttemptsIndex))
	failedAt := parseInt64(valueAt(values, adminDLQFailedAtIndex))
	updatedAt := parseInt64(valueAt(values, adminDLQUpdatedAtIndex))
	lastError := valueAt(values, adminDLQLastErrorIndex)
	metadataRaw := valueAt(values, adminDLQMetadataIndex)

	metadata, err := decodeMetadata(metadataRaw)
	if err != nil {
		return dlqEntryFields{}, err
	}

	return dlqEntryFields{
		queue:     queue,
		handler:   handler,
		attempts:  attempts,
		failedAt:  failedAt,
		updatedAt: updatedAt,
		lastError: lastError,
		metadata:  metadata,
	}, nil
}

func (b *RedisDurableBackend) ensureDLQEntryExists(ctx context.Context, taskKey, id string) error {
	existsResp := b.client.Do(ctx, b.client.B().Exists().Key(taskKey).Build())

	exists, err := existsResp.AsInt64()
	if err != nil {
		return ewrap.Wrapf(err, "read DLQ task %s", id)
	}

	if exists == 0 {
		return ErrAdminDLQEntryNotFound
	}

	return nil
}

func (b *RedisDurableBackend) fetchDLQEntryValues(ctx context.Context, taskKey, id string) ([]string, error) {
	fields := []string{
		redisFieldQueue,
		redisFieldHandler,
		redisFieldAttempts,
		redisFieldFailedAtMs,
		redisFieldUpdatedAtMs,
		redisFieldLastError,
		redisFieldMetadata,
	}

	resp := b.client.Do(ctx, b.client.B().Hmget().Key(taskKey).Field(fields...).Build())

	values, err := resp.AsStrSlice()
	if err != nil {
		return nil, ewrap.Wrapf(err, "read DLQ task %s", id)
	}

	return values, nil
}

func (b *RedisDurableBackend) dlqEntryPayloadSize(ctx context.Context, taskKey string) (int64, error) {
	sizeResp := b.client.Do(ctx, b.client.B().Hstrlen().Key(taskKey).Field(redisFieldPayload).Build())

	payloadSize, err := sizeResp.AsInt64()
	if err != nil {
		return 0, ewrap.Wrapf(err, "read DLQ task payload size %s", taskKey)
	}

	return payloadSize, nil
}

func dlqEntryAge(failedAt, updatedAt int64) time.Duration {
	if failedAt > 0 {
		return time.Since(time.UnixMilli(failedAt))
	}

	if updatedAt > 0 {
		return time.Since(time.UnixMilli(updatedAt))
	}

	return 0
}

func mergeQueueNames(queues []string, weights map[string]int) []string {
	if len(weights) == 0 {
		return queues
	}

	seen := map[string]struct{}{}
	out := make([]string, 0, len(queues)+len(weights))

	for _, queue := range queues {
		if queue == "" {
			continue
		}

		if _, ok := seen[queue]; ok {
			continue
		}

		seen[queue] = struct{}{}
		out = append(out, queue)
	}

	for queue := range weights {
		if queue == "" {
			continue
		}

		if _, ok := seen[queue]; ok {
			continue
		}

		seen[queue] = struct{}{}
		out = append(out, queue)
	}

	sort.Strings(out)

	return out
}

// AdminPause pauses dequeueing of tasks.
func (b *RedisDurableBackend) AdminPause(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidTaskContext
	}

	resp := b.client.Do(ctx, b.client.B().Set().Key(b.pausedKey()).Value("1").Build())
	if resp.Error() != nil {
		return ewrap.Wrap(resp.Error(), "pause durable dequeue")
	}

	return nil
}

// AdminResume resumes dequeueing of tasks.
func (b *RedisDurableBackend) AdminResume(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidTaskContext
	}

	resp := b.client.Do(ctx, b.client.B().Del().Key(b.pausedKey()).Build())
	if resp.Error() != nil {
		return ewrap.Wrap(resp.Error(), "resume durable dequeue")
	}

	return nil
}

// AdminReplayDLQ replays tasks from the dead letter queue back to their respective ready queues.
func (b *RedisDurableBackend) AdminReplayDLQ(ctx context.Context, limit int) (int, error) {
	if ctx == nil {
		return 0, ErrInvalidTaskContext
	}

	if limit <= 0 {
		limit = adminDLQDefaultSize
	}

	script := rueidis.NewLuaScript(adminDLQReplayScript)
	resp := script.Exec(
		ctx,
		b.client,
		[]string{b.deadKey(), b.taskPrefixKey(), b.queuesKey()},
		[]string{
			strconv.FormatInt(time.Now().UnixMilli(), 10),
			strconv.Itoa(limit),
			b.keyPrefix(),
			b.defaultQueueName(),
		},
	)

	if resp.Error() != nil {
		return 0, ewrap.Wrap(resp.Error(), "replay durable DLQ")
	}

	moved, err := resp.AsInt64()
	if err != nil {
		return 0, ewrap.Wrap(err, "parse replay result")
	}

	return int(moved), nil
}

// AdminReplayDLQByID replays specific DLQ entries by ID.
func (b *RedisDurableBackend) AdminReplayDLQByID(ctx context.Context, ids []string) (int, error) {
	if ctx == nil {
		return 0, ErrInvalidTaskContext
	}

	normalized := normalizeReplayIDs(ids)
	if len(normalized) == 0 {
		return 0, ErrAdminReplayIDsRequired
	}

	if len(normalized) > adminDLQReplayMaxIDs {
		return 0, ErrAdminReplayIDsTooLarge
	}

	args := make([]string, 0, adminDLQReplayByIDArgsPrefix+len(normalized))
	args = append(args,
		b.keyPrefix(),
		b.defaultQueueName(),
		strconv.FormatInt(time.Now().UnixMilli(), 10),
	)
	args = append(args, normalized...)

	script := rueidis.NewLuaScript(adminDLQReplayByIDScript)
	resp := script.Exec(
		ctx,
		b.client,
		[]string{b.deadKey(), b.taskPrefixKey(), b.queuesKey()},
		args,
	)

	if resp.Error() != nil {
		return 0, ewrap.Wrap(resp.Error(), "replay durable DLQ by id")
	}

	moved, err := resp.AsInt64()
	if err != nil {
		return 0, ewrap.Wrap(err, "parse replay by id result")
	}

	return int(moved), nil
}

func (b *RedisDurableBackend) queueCounts(ctx context.Context, queue string) (readyCount, processingCount int64, err error) {
	readyResp := b.client.Do(ctx, b.client.B().Zcard().Key(b.readyKey(queue)).Build())
	processingResp := b.client.Do(ctx, b.client.B().Zcard().Key(b.processingKey(queue)).Build())

	readyCount, err = readyResp.AsInt64()
	if err != nil {
		return 0, 0, ewrap.Wrapf(err, "ready count for queue %s", queue)
	}

	processingCount, err = processingResp.AsInt64()
	if err != nil {
		return 0, 0, ewrap.Wrapf(err, "processing count for queue %s", queue)
	}

	return readyCount, processingCount, nil
}

func (b *RedisDurableBackend) deadIDs(ctx context.Context, offset, limit int) ([]string, error) {
	if limit <= 0 {
		return []string{}, nil
	}

	if offset < 0 {
		offset = 0
	}

	start := int64(offset)
	stop := start + int64(limit) - 1

	resp := b.client.Do(ctx, b.client.B().Lrange().Key(b.deadKey()).Start(start).Stop(stop).Build())

	values, err := resp.AsStrSlice()
	if err != nil {
		return nil, ewrap.Wrap(err, "read DLQ ids")
	}

	return values, nil
}

func (b *RedisDurableBackend) dlqEntry(ctx context.Context, id string) (AdminDLQEntry, error) {
	fields := []string{
		redisFieldQueue,
		redisFieldHandler,
		redisFieldAttempts,
		redisFieldFailedAtMs,
		redisFieldUpdatedAtMs,
	}

	resp := b.client.Do(ctx, b.client.B().Hmget().Key(b.taskPrefixKey()+id).Field(fields...).Build())

	values, err := resp.AsStrSlice()
	if err != nil {
		return AdminDLQEntry{}, ewrap.Wrapf(err, "read DLQ task %s", id)
	}

	queue := valueAt(values, adminDLQQueueIndex)
	if queue == "" {
		queue = b.defaultQueueName()
	}

	handler := valueAt(values, adminDLQHandlerIndex)
	attempts := parseInt(valueAt(values, adminDLQAttemptsIndex))
	failedAt := parseInt64(valueAt(values, adminDLQFailedAtIndex))
	updatedAt := parseInt64(valueAt(values, adminDLQUpdatedAtIndex))

	age := time.Duration(0)
	if failedAt > 0 {
		age = time.Since(time.UnixMilli(failedAt))
	} else if updatedAt > 0 {
		age = time.Since(time.UnixMilli(updatedAt))
	}

	return AdminDLQEntry{
		ID:       id,
		Queue:    queue,
		Handler:  handler,
		Attempts: attempts,
		AgeMs:    age.Milliseconds(),
	}, nil
}

func (b *RedisDurableBackend) deadCountsByQueue(
	ctx context.Context,
	queues []string,
	limit int,
) (map[string]int64, error) {
	out := make(map[string]int64, len(queues))

	if limit <= 0 {
		return out, nil
	}

	countResp := b.client.Do(ctx, b.client.B().Llen().Key(b.deadKey()).Build())

	total, err := countResp.AsInt64()
	if err != nil {
		return nil, ewrap.Wrap(err, "read DLQ size")
	}

	if total <= 0 {
		return out, nil
	}

	if total > int64(limit) {
		return out, nil
	}

	totalInt, err := sectconv.ToInt(total)
	if err != nil {
		return nil, ewrap.Wrap(err, "convert DLQ size")
	}

	ids, err := b.deadIDs(ctx, 0, totalInt)
	if err != nil {
		return nil, err
	}

	for _, id := range ids {
		taskKey := b.taskPrefixKey() + id
		resp := b.client.Do(ctx, b.client.B().Hget().Key(taskKey).Field(redisFieldQueue).Build())

		queue, err := resp.ToString()
		if err != nil && !rueidis.IsRedisNil(err) {
			return nil, ewrap.Wrapf(err, "read DLQ task %s queue", id)
		}

		if err != nil {
			queue = b.defaultQueueName()
		}

		queue = normalizeQueueName(queue)
		out[queue]++
	}

	return out, nil
}

func (b *RedisDurableBackend) hasKey(ctx context.Context, key string) (bool, error) {
	resp := b.client.Do(ctx, b.client.B().Exists().Key(key).Build())

	count, err := resp.AsInt64()
	if err != nil {
		return false, ewrap.Wrapf(err, "check key %s", key)
	}

	return count > 0, nil
}

func (b *RedisDurableBackend) keyTTL(ctx context.Context, key string) (time.Duration, error) {
	resp := b.client.Do(ctx, b.client.B().Pttl().Key(key).Build())

	ttlMs, err := resp.AsInt64()
	if err != nil {
		return 0, ewrap.Wrapf(err, "read ttl for %s", key)
	}

	if ttlMs <= 0 {
		return 0, nil
	}

	return time.Duration(ttlMs) * time.Millisecond, nil
}

type adminDLQFilters struct {
	limit   int
	offset  int
	queue   string
	handler string
	query   string
}

func normalizeAdminDLQFilter(filter AdminDLQFilter) adminDLQFilters {
	limit := filter.Limit
	if limit <= 0 {
		limit = adminDLQDefaultSize
	}

	offset := max(filter.Offset, 0)

	queueFilter := strings.TrimSpace(filter.Queue)
	if queueFilter != "" {
		queueFilter = normalizeQueueName(queueFilter)
	}

	handlerFilter := strings.TrimSpace(filter.Handler)
	queryFilter := strings.ToLower(strings.TrimSpace(filter.Query))

	return adminDLQFilters{
		limit:   limit,
		offset:  offset,
		queue:   queueFilter,
		handler: handlerFilter,
		query:   queryFilter,
	}
}

func (filter adminDLQFilters) hasFilters() bool {
	return filter.queue != "" || filter.handler != "" || filter.query != ""
}

func (b *RedisDurableBackend) dlqTotal(ctx context.Context) (int64, error) {
	countResp := b.client.Do(ctx, b.client.B().Llen().Key(b.deadKey()).Build())

	total, err := countResp.AsInt64()
	if err != nil {
		return 0, ewrap.Wrap(err, "read DLQ size")
	}

	return total, nil
}

func (b *RedisDurableBackend) dlqPageNoFilters(
	ctx context.Context,
	filter adminDLQFilters,
	total int64,
) (AdminDLQPage, error) {
	ids, err := b.deadIDs(ctx, filter.offset, filter.limit)
	if err != nil {
		return AdminDLQPage{}, err
	}

	if len(ids) == 0 {
		return AdminDLQPage{Entries: []AdminDLQEntry{}, Total: total}, nil
	}

	entries, err := b.dlqEntries(ctx, ids)
	if err != nil {
		return AdminDLQPage{}, err
	}

	return AdminDLQPage{Entries: entries, Total: total}, nil
}

func (b *RedisDurableBackend) dlqPageWithFilters(
	ctx context.Context,
	filter adminDLQFilters,
	total int64,
) (AdminDLQPage, error) {
	if total > adminDLQScanLimit {
		return AdminDLQPage{}, ErrAdminDLQFilterTooLarge
	}

	totalInt, err := sectconv.ToInt(total)
	if err != nil {
		return AdminDLQPage{}, ewrap.Wrap(err, "convert DLQ size")
	}

	ids, err := b.deadIDs(ctx, 0, totalInt)
	if err != nil {
		return AdminDLQPage{}, err
	}

	filtered := make([]AdminDLQEntry, 0, len(ids))
	for _, id := range ids {
		entry, err := b.dlqEntry(ctx, id)
		if err != nil {
			return AdminDLQPage{}, err
		}

		if !matchAdminDLQFilter(entry, filter) {
			continue
		}

		filtered = append(filtered, entry)
	}

	return paginateAdminDLQ(filtered, filter.offset, filter.limit), nil
}

func (b *RedisDurableBackend) dlqEntries(ctx context.Context, ids []string) ([]AdminDLQEntry, error) {
	entries := make([]AdminDLQEntry, 0, len(ids))
	for _, id := range ids {
		entry, err := b.dlqEntry(ctx, id)
		if err != nil {
			return nil, err
		}

		entries = append(entries, entry)
	}

	return entries, nil
}

func normalizeReplayIDs(ids []string) []string {
	if len(ids) == 0 {
		return nil
	}

	out := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))

	for _, raw := range ids {
		id := strings.TrimSpace(raw)
		if id == "" {
			continue
		}

		if _, ok := seen[id]; ok {
			continue
		}

		seen[id] = struct{}{}
		out = append(out, id)
	}

	return out
}

func matchAdminDLQFilter(entry AdminDLQEntry, filter adminDLQFilters) bool {
	if filter.queue != "" && entry.Queue != filter.queue {
		return false
	}

	if filter.handler != "" && entry.Handler != filter.handler {
		return false
	}

	if filter.query == "" {
		return true
	}

	query := filter.query

	return strings.Contains(strings.ToLower(entry.ID), query) ||
		strings.Contains(strings.ToLower(entry.Queue), query) ||
		strings.Contains(strings.ToLower(entry.Handler), query)
}

func paginateAdminDLQ(entries []AdminDLQEntry, offset, limit int) AdminDLQPage {
	total := len(entries)
	if total == 0 || offset >= total {
		return AdminDLQPage{Entries: []AdminDLQEntry{}, Total: int64(total)}
	}

	end := min(offset+limit, total)

	return AdminDLQPage{Entries: entries[offset:end], Total: int64(total)}
}

type adminJobRecord struct {
	Name           string   `json:"name"`
	Description    string   `json:"description,omitempty"`
	Repo           string   `json:"repo"`
	Tag            string   `json:"tag"`
	Source         string   `json:"source,omitempty"`
	TarballURL     string   `json:"tarball_url,omitempty"`
	TarballPath    string   `json:"tarball_path,omitempty"`
	TarballSHA     string   `json:"tarball_sha256,omitempty"`
	Path           string   `json:"path,omitempty"`
	Dockerfile     string   `json:"dockerfile,omitempty"`
	Command        []string `json:"command,omitempty"`
	Env            []string `json:"env,omitempty"`
	Queue          string   `json:"queue,omitempty"`
	Retries        int      `json:"retries,omitempty"`
	TimeoutSeconds int64    `json:"timeout_seconds,omitempty"`
	CreatedAtMs    int64    `json:"created_at_ms"`
	UpdatedAtMs    int64    `json:"updated_at_ms"`
}

// AdminJobs lists registered job definitions.
func (b *RedisDurableBackend) AdminJobs(ctx context.Context) ([]AdminJob, error) {
	if ctx == nil {
		return nil, ErrInvalidTaskContext
	}

	resp := b.client.Do(ctx, b.client.B().Hgetall().Key(b.jobsKey()).Build())

	values, err := resp.AsStrMap()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			return []AdminJob{}, nil
		}

		return nil, ewrap.Wrap(err, "read admin jobs")
	}

	jobs := make([]AdminJob, 0, len(values))
	for _, raw := range values {
		job, err := parseAdminJobRecord(raw)
		if err != nil {
			return nil, err
		}

		jobs = append(jobs, job)
	}

	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].Name < jobs[j].Name
	})

	return jobs, nil
}

// AdminJob returns a job definition by name.
func (b *RedisDurableBackend) AdminJob(ctx context.Context, name string) (AdminJob, error) {
	if ctx == nil {
		return AdminJob{}, ErrInvalidTaskContext
	}

	name = strings.TrimSpace(name)
	if name == "" {
		return AdminJob{}, ErrAdminJobNameRequired
	}

	resp := b.client.Do(ctx, b.client.B().Hget().Key(b.jobsKey()).Field(name).Build())

	raw, err := resp.ToString()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			return AdminJob{}, ErrAdminJobNotFound
		}

		return AdminJob{}, ewrap.Wrap(err, "read admin job")
	}

	return parseAdminJobRecord(raw)
}

// AdminUpsertJob creates or updates a job definition.
func (b *RedisDurableBackend) AdminUpsertJob(ctx context.Context, spec AdminJobSpec) (AdminJob, error) {
	if ctx == nil {
		return AdminJob{}, ErrInvalidTaskContext
	}

	spec, err := b.normalizeJobSpec(spec)
	if err != nil {
		return AdminJob{}, err
	}

	now := time.Now()

	existing, err := b.AdminJob(ctx, spec.Name)
	if err != nil && !errors.Is(err, ErrAdminJobNotFound) {
		return AdminJob{}, err
	}

	createdAt := now
	if err == nil {
		createdAt = existing.CreatedAt
	}

	record := adminJobRecord{
		Name:           spec.Name,
		Description:    spec.Description,
		Repo:           spec.Repo,
		Tag:            spec.Tag,
		Source:         spec.Source,
		TarballURL:     spec.TarballURL,
		TarballPath:    spec.TarballPath,
		TarballSHA:     spec.TarballSHA,
		Path:           spec.Path,
		Dockerfile:     spec.Dockerfile,
		Command:        spec.Command,
		Env:            spec.Env,
		Queue:          spec.Queue,
		Retries:        spec.Retries,
		TimeoutSeconds: int64(spec.Timeout.Seconds()),
		CreatedAtMs:    createdAt.UnixMilli(),
		UpdatedAtMs:    now.UnixMilli(),
	}

	raw, err := json.Marshal(record)
	if err != nil {
		return AdminJob{}, ewrap.Wrap(err, "encode admin job")
	}

	resp := b.client.Do(ctx, b.client.B().Hset().Key(b.jobsKey()).FieldValue().FieldValue(record.Name, string(raw)).Build())
	if resp.Error() != nil {
		return AdminJob{}, ewrap.Wrap(resp.Error(), "write admin job")
	}

	return adminJobFromRecord(record), nil
}

// AdminDeleteJob removes a job definition.
func (b *RedisDurableBackend) AdminDeleteJob(ctx context.Context, name string) (bool, error) {
	if ctx == nil {
		return false, ErrInvalidTaskContext
	}

	name = strings.TrimSpace(name)
	if name == "" {
		return false, ErrAdminJobNameRequired
	}

	resp := b.client.Do(ctx, b.client.B().Hdel().Key(b.jobsKey()).Field(name).Build())

	count, err := resp.AsInt64()
	if err != nil {
		return false, ewrap.Wrap(err, "delete admin job")
	}

	return count > 0, nil
}

// AdminJobEvents returns job execution events; Redis backend does not persist them.
func (*RedisDurableBackend) AdminJobEvents(ctx context.Context, _ AdminJobEventFilter) (AdminJobEventPage, error) {
	if ctx == nil {
		return AdminJobEventPage{}, ErrInvalidTaskContext
	}

	return AdminJobEventPage{}, ErrAdminUnsupported
}

func (b *RedisDurableBackend) normalizeJobSpec(spec AdminJobSpec) (AdminJobSpec, error) {
	name := strings.TrimSpace(spec.Name)
	if name == "" {
		return AdminJobSpec{}, ErrAdminJobNameRequired
	}

	source, err := normalizeAdminJobSource(spec)
	if err != nil {
		return AdminJobSpec{}, err
	}

	queue := normalizeJobQueue(strings.TrimSpace(spec.Queue), b.defaultQueueName())

	pathValue, dockerfile, err := normalizeJobPaths(spec.Path, spec.Dockerfile)
	if err != nil {
		return AdminJobSpec{}, err
	}

	command := trimStringSlice(spec.Command)
	env := trimStringSlice(spec.Env)

	return AdminJobSpec{
		Name:        name,
		Description: strings.TrimSpace(spec.Description),
		Repo:        source.repo,
		Tag:         source.tag,
		Source:      source.source,
		TarballURL:  source.tarballURL,
		TarballPath: source.tarballPath,
		TarballSHA:  source.tarballSHA,
		Path:        pathValue,
		Dockerfile:  dockerfile,
		Command:     command,
		Env:         env,
		Queue:       queue,
		Retries:     max(0, spec.Retries),
		Timeout:     spec.Timeout,
	}, nil
}

type normalizedJobSource struct {
	source      string
	repo        string
	tag         string
	tarballURL  string
	tarballPath string
	tarballSHA  string
}

const tarballSHA256Len = 64

func normalizeAdminJobSource(spec AdminJobSpec) (normalizedJobSource, error) {
	sourceRaw := strings.TrimSpace(spec.Source)

	source := NormalizeJobSource(sourceRaw)
	if sourceRaw != "" && sourceRaw != source {
		return normalizedJobSource{}, ErrAdminJobSourceInvalid
	}

	repo := strings.TrimSpace(spec.Repo)
	tag := strings.TrimSpace(spec.Tag)
	tarballURL := strings.TrimSpace(spec.TarballURL)
	tarballPath := strings.TrimSpace(spec.TarballPath)
	tarballSHA := strings.TrimSpace(spec.TarballSHA)

	source, err := inferAdminJobSource(sourceRaw, source, tarballURL, tarballPath)
	if err != nil {
		return normalizedJobSource{}, err
	}

	err = validateAdminJobSource(source, repo, tag, tarballURL, tarballPath)
	if err != nil {
		return normalizedJobSource{}, err
	}

	tarballPath, err = normalizeAdminTarballPath(tarballPath)
	if err != nil {
		return normalizedJobSource{}, err
	}

	err = validateAdminTarballSHA(tarballSHA)
	if err != nil {
		return normalizedJobSource{}, err
	}

	return normalizedJobSource{
		source:      source,
		repo:        repo,
		tag:         tag,
		tarballURL:  tarballURL,
		tarballPath: tarballPath,
		tarballSHA:  tarballSHA,
	}, nil
}

func inferAdminJobSource(sourceRaw, source, tarballURL, tarballPath string) (string, error) {
	if sourceRaw != "" {
		return source, nil
	}

	if tarballURL != "" && tarballPath != "" {
		return "", ErrAdminJobSourceInvalid
	}

	if tarballURL != "" {
		return JobSourceTarballURL, nil
	}

	if tarballPath != "" {
		return JobSourceTarballPath, nil
	}

	return source, nil
}

func validateAdminJobSource(source, repo, tag, tarballURL, tarballPath string) error {
	switch source {
	case JobSourceGitTag:
		if repo == "" {
			return ErrAdminJobRepoRequired
		}

		if tag == "" || strings.Contains(tag, " ") {
			return ErrAdminJobTagRequired
		}

		return nil
	case JobSourceTarballURL:
		if tarballURL == "" {
			return ErrAdminJobTarballURLRequired
		}

		return nil
	case JobSourceTarballPath:
		if tarballPath == "" {
			return ErrAdminJobTarballPathRequired
		}

		return nil
	default:
		return ErrAdminJobSourceInvalid
	}
}

func normalizeAdminTarballPath(tarballPath string) (string, error) {
	if tarballPath == "" {
		return "", nil
	}

	clean, err := sanitizeJobPath(tarballPath)
	if err != nil {
		return "", err
	}

	return clean, nil
}

func validateAdminTarballSHA(tarballSHA string) error {
	if tarballSHA != "" && len(tarballSHA) != tarballSHA256Len {
		return ErrAdminJobTarballSHAInvalid
	}

	return nil
}

func normalizeJobPaths(pathValue, dockerfile string) (normalizedPath, normalizedDockerfile string, err error) {
	normalizedPath, err = sanitizeJobPath(pathValue)
	if err != nil {
		return "", "", err
	}

	dockerfile = strings.TrimSpace(dockerfile)
	if dockerfile == "" {
		dockerfile = adminJobDefaultDockerfile
	}

	normalizedDockerfile, err = sanitizeJobPath(dockerfile)
	if err != nil {
		return "", "", err
	}

	return normalizedPath, normalizedDockerfile, nil
}

func normalizeJobQueue(queue, fallback string) string {
	if queue == "" {
		return fallback
	}

	return queue
}

func (b *RedisDurableBackend) jobsKey() string {
	return b.keyPrefix() + redisKVSeparator + adminJobsKeyName
}

func parseAdminJobRecord(raw string) (AdminJob, error) {
	var record adminJobRecord

	err := json.Unmarshal([]byte(raw), &record)
	if err != nil {
		return AdminJob{}, ewrap.Wrap(err, "decode admin job")
	}

	return adminJobFromRecord(record), nil
}

func adminJobFromRecord(record adminJobRecord) AdminJob {
	job := AdminJob{
		AdminJobSpec: AdminJobSpec{
			Name:        record.Name,
			Description: record.Description,
			Repo:        record.Repo,
			Tag:         record.Tag,
			Source:      record.Source,
			TarballURL:  record.TarballURL,
			TarballPath: record.TarballPath,
			TarballSHA:  record.TarballSHA,
			Path:        record.Path,
			Dockerfile:  record.Dockerfile,
			Command:     record.Command,
			Env:         record.Env,
			Queue:       record.Queue,
			Retries:     record.Retries,
		},
		CreatedAt: time.UnixMilli(record.CreatedAtMs),
		UpdatedAt: time.UnixMilli(record.UpdatedAtMs),
	}

	if record.TimeoutSeconds > 0 {
		job.Timeout = time.Duration(record.TimeoutSeconds) * time.Second
	}

	return job
}

func sanitizeJobPath(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", nil
	}

	if strings.HasPrefix(value, "/") {
		return "", ewrap.New("job path must be relative")
	}

	clean := filepath.Clean(value)
	if strings.HasPrefix(clean, "..") {
		return "", ewrap.New("job path must not escape repository")
	}

	return clean, nil
}

func trimStringSlice(values []string) []string {
	if len(values) == 0 {
		return nil
	}

	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}

		out = append(out, value)
	}

	return out
}

func formatAdminDuration(duration time.Duration) string {
	if duration <= 0 {
		return adminNotAvailable
	}

	if duration < time.Second {
		return strconv.FormatInt(max(1, duration.Milliseconds()), 10) + "ms"
	}

	seconds := int(duration.Seconds())
	if seconds < adminSecondsPerMinute {
		return strconv.Itoa(seconds) + "s"
	}

	minutes := seconds / adminSecondsPerMinute
	if minutes < adminMinutesPerHour {
		return strconv.Itoa(minutes) + "m"
	}

	hours := minutes / adminMinutesPerHour
	if hours < adminHoursPerDay {
		return strconv.Itoa(hours) + "h"
	}

	days := hours / adminHoursPerDay

	return strconv.Itoa(days) + "d"
}

func valueAt(values []string, idx int) string {
	if idx < 0 || idx >= len(values) {
		return ""
	}

	return values[idx]
}

func parseInt(value string) int {
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0
	}

	return parsed
}

func parseInt64(value string) int64 {
	parsed, err := strconv.ParseInt(value, 10, parseIntBitSize)
	if err != nil {
		return 0
	}

	return parsed
}
