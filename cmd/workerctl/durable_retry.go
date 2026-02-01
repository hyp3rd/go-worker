package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/hyp3rd/ewrap"
	"github.com/redis/rueidis"
	"github.com/spf13/cobra"
)

const (
	defaultRetryLimit = 100
	retrySourceDLQ    = "dlq"
	retrySourceReady  = "ready"
	retrySourceProc   = "processing"
)

//nolint:revive
const retryScript = "\nlocal dead = KEYS[1]\nlocal queuesKey = KEYS[2]\nlocal taskPrefix = KEYS[3]\nlocal now = tonumber(ARGV[1])\nlocal delayMs = tonumber(ARGV[2])\nlocal prefix = ARGV[3]\nlocal defaultQueue = ARGV[4]\nlocal overrideQueue = ARGV[5]\n\nlocal readyAt = now + delayMs\nlocal moved = 0\nfor i = 6, #ARGV do\n  local id = ARGV[i]\n  local taskKey = taskPrefix .. id\n  if redis.call(\"EXISTS\", taskKey) == 1 then\n    local queue = overrideQueue\n    if queue == \"\" then\n      queue = redis.call(\"HGET\", taskKey, \"queue\")\n      if queue == false or queue == \"\" then\n        queue = defaultQueue\n      end\n    else\n      redis.call(\"HSET\", taskKey, \"queue\", queue)\n    end\n    local readyKey = prefix .. \":ready:\" .. queue\n    local processingKey = prefix .. \":processing:\" .. queue\n    redis.call(\"LREM\", dead, 0, id)\n    redis.call(\"ZREM\", readyKey, id)\n    redis.call(\"ZREM\", processingKey, id)\n    redis.call(\"HSET\", taskKey, \"ready_at_ms\", readyAt, \"updated_at_ms\", now)\n    redis.call(\"ZADD\", readyKey, readyAt, id)\n    redis.call(\"SADD\", queuesKey, queue)\n    moved = moved + 1\n  end\nreturn"

type retryOptions struct {
	ids          []string
	queue        string
	fromQueue    string
	source       string
	delay        time.Duration
	limit        int
	apply        bool
	defaultQueue string
}

func newDurableRetryCmd(cfg *redisConfig) *cobra.Command {
	opts := &retryOptions{
		defaultQueue: defaultQueueName,
		source:       retrySourceDLQ,
		limit:        defaultRetryLimit,
	}

	cmd := &cobra.Command{
		Use:   "retry",
		Short: "Requeue durable tasks by ID or source",
		RunE: func(_ *cobra.Command, _ []string) error {
			return runDurableRetry(cfg, *opts)
		},
	}

	flags := cmd.Flags()
	flags.StringArrayVar(&opts.ids, "id", nil, "task ID to requeue (repeatable)")
	flags.StringVar(&opts.queue, "queue", "", "override queue name for requeue")
	flags.StringVar(&opts.fromQueue, "from-queue", "", "queue name when using --source ready|processing")
	flags.StringVar(&opts.source, "source", opts.source, "source set: dlq, ready, processing")
	flags.IntVar(&opts.limit, "limit", opts.limit, "max items to requeue when --id is not set (0 = all)")
	flags.DurationVar(&opts.delay, "delay", 0, "delay before requeue (e.g. 5s)")
	flags.BoolVar(&opts.apply, "apply", false, "apply requeue (default is dry-run)")
	flags.StringVar(&opts.defaultQueue, "default-queue", opts.defaultQueue, "fallback queue when task queue is missing")

	return cmd
}

func runDurableRetry(cfg *redisConfig, opts retryOptions) error {
	ids := flattenIDs(opts.ids)

	err := validateRetryOptions(ids, opts)
	if err != nil {
		return err
	}

	if !opts.apply {
		fmt.Fprintf(os.Stdout, "dry-run: use --apply to requeue %s\n", retryTargetLabel(ids, opts))

		return nil
	}

	client, err := cfg.client()
	if err != nil {
		return ewrap.Wrap(err, "redis client")
	}
	defer client.Close()

	ctx, cancel := cfg.context()
	defer cancel()

	prefix := keyPrefix(cfg.prefix)
	keys := retryKeys(prefix)
	delayMs := normalizeDelay(opts.delay)

	if len(ids) == 0 {
		ids, err = loadRetryIDs(ctx, client, prefix, opts)
		if err != nil {
			return err
		}
	}

	if len(ids) == 0 {
		fmt.Fprintln(os.Stdout, "no tasks found to requeue")

		return nil
	}

	moved, err := retryTasks(ctx, client, keys, prefix, opts, delayMs, ids)
	if err != nil {
		return err
	}

	fmt.Fprintf(os.Stdout, "requeued %d task(s)\n", moved)

	return nil
}

func validateRetryOptions(ids []string, opts retryOptions) error {
	if len(ids) > 0 {
		return nil
	}

	if opts.source == "" {
		return ewrap.New("source is required when --id is not set")
	}

	switch opts.source {
	case retrySourceDLQ:
		return nil
	case retrySourceReady, retrySourceProc:
		if strings.TrimSpace(opts.fromQueue) == "" {
			return ewrap.New("--from-queue is required for source ready/processing")
		}

		return nil
	default:
		return ewrap.New("source must be dlq, ready, or processing")
	}
}

func retryTargetLabel(ids []string, opts retryOptions) string {
	if len(ids) > 0 {
		return fmt.Sprintf("%d task(s)", len(ids))
	}

	limit := normalizeRetryLimit(opts.limit)
	if limit <= 0 {
		return "all matching tasks"
	}

	return fmt.Sprintf("up to %d task(s)", limit)
}

func loadRetryIDs(
	ctx context.Context,
	client rueidis.Client,
	prefix string,
	opts retryOptions,
) ([]string, error) {
	limit := normalizeRetryLimit(opts.limit)
	switch opts.source {
	case retrySourceDLQ:
		return fetchListIDs(ctx, client, prefix+":dead", limit)
	case retrySourceReady:
		queue := strings.TrimSpace(opts.fromQueue)
		key := queueKey(prefix, "ready", queue)

		return fetchZSetIDs(ctx, client, key, limit)
	case retrySourceProc:
		queue := strings.TrimSpace(opts.fromQueue)
		key := queueKey(prefix, "processing", queue)

		return fetchZSetIDs(ctx, client, key, limit)
	default:
		return nil, ewrap.New("unknown retry source")
	}
}

func retryKeys(prefix string) dlqReplayKeys {
	return dlqReplayKeys{
		deadKey:    prefix + ":dead",
		queuesKey:  prefix + ":queues",
		taskPrefix: prefix + ":task:",
	}
}

func normalizeDelay(delay time.Duration) int64 {
	if delay <= 0 {
		return 0
	}

	return delay.Milliseconds()
}

func normalizeRetryLimit(limit int) int64 {
	if limit <= 0 {
		return 0
	}

	return int64(limit)
}

func retryTasks(
	ctx context.Context,
	client rueidis.Client,
	keys dlqReplayKeys,
	prefix string,
	opts retryOptions,
	delayMs int64,
	ids []string,
) (int64, error) {
	const retryArgsBase = 5

	args := make([]string, 0, retryArgsBase+len(ids))
	args = append(args,
		strconv.FormatInt(time.Now().UnixMilli(), 10),
		strconv.FormatInt(delayMs, 10),
		prefix,
		opts.defaultQueue,
		strings.TrimSpace(opts.queue),
	)
	args = append(args, ids...)

	script := rueidis.NewLuaScript(retryScript)

	resp := script.Exec(
		ctx,
		client,
		[]string{keys.deadKey, keys.queuesKey, keys.taskPrefix},
		args,
	)

	err := resp.Error()
	if err != nil {
		return 0, ewrap.Wrap(err, "requeue")
	}

	moved, err := resp.AsInt64()
	if err != nil {
		return 0, ewrap.Wrap(err, "requeue result")
	}

	return moved, nil
}

func flattenIDs(ids []string) []string {
	out := make([]string, 0, len(ids))
	for _, entry := range ids {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}

		if strings.Contains(entry, ",") {
			for part := range strings.SplitSeq(entry, ",") {
				part = strings.TrimSpace(part)
				if part != "" {
					out = append(out, part)
				}
			}

			continue
		}

		out = append(out, entry)
	}

	return out
}
