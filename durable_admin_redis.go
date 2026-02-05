package worker

import (
	"context"
	"strconv"
	"time"

	"github.com/hyp3rd/ewrap"
	"github.com/redis/rueidis"
)

const (
	adminDLQScanLimit      = 2000
	adminDLQDefaultSize    = 100
	adminDLQQueueIndex     = 0
	adminDLQHandlerIndex   = 1
	adminDLQAttemptsIndex  = 2
	adminDLQFailedAtIndex  = 3
	adminDLQUpdatedAtIndex = 4
	adminSecondsPerMinute  = 60
	adminMinutesPerHour    = 60
	adminHoursPerDay       = 24
	parseIntBitSize        = 64
)

//nolint:revive
const adminDLQReplayScript = "\nlocal dead = KEYS[1]\nlocal taskPrefix = KEYS[2]\nlocal queuesKey = KEYS[3]\nlocal now = tonumber(ARGV[1])\nlocal limit = tonumber(ARGV[2])\nlocal prefix = ARGV[3]\nlocal defaultQueue = ARGV[4]\n\nlocal moved = 0\nfor i = 1, limit do\n  local id = redis.call(\"RPOP\", dead)\n  if not id then\n    break\n  end\n  local taskKey = taskPrefix .. id\n  if redis.call(\"EXISTS\", taskKey) == 1 then\n    local queue = redis.call(\"HGET\", taskKey, \"queue\")\n    if queue == false or queue == \"\" then\n      queue = defaultQueue\n    end\n    local readyKey = prefix .. \":ready:\" .. queue\n    redis.call(\"HSET\", taskKey, \"ready_at_ms\", now, \"updated_at_ms\", now)\n    redis.call(\"ZADD\", readyKey, now, id)\n    redis.call(\"SADD\", queuesKey, queue)\n    moved = moved + 1\n  end\nreturn moved"

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
	lease := "n/a"

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
			Weight:     weights[queue],
		})
	}

	return results, nil
}

// AdminDLQ returns entries from the dead letter queue.
func (b *RedisDurableBackend) AdminDLQ(ctx context.Context, limit int) ([]AdminDLQEntry, error) {
	if ctx == nil {
		return nil, ErrInvalidTaskContext
	}

	if limit <= 0 {
		limit = adminDLQDefaultSize
	}

	ids, err := b.deadIDs(ctx, limit)
	if err != nil {
		return nil, err
	}

	if len(ids) == 0 {
		return []AdminDLQEntry{}, nil
	}

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

func (b *RedisDurableBackend) deadIDs(ctx context.Context, limit int) ([]string, error) {
	resp := b.client.Do(ctx, b.client.B().Lrange().Key(b.deadKey()).Start(0).Stop(int64(limit-1)).Build())

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

	ids, err := b.deadIDs(ctx, int(total))
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

func formatAdminDuration(duration time.Duration) string {
	if duration <= 0 {
		return "n/a"
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
