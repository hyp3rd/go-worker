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

//nolint:revive
const retryScript = "\nlocal dead = KEYS[1]\nlocal queuesKey = KEYS[2]\nlocal taskPrefix = KEYS[3]\nlocal now = tonumber(ARGV[1])\nlocal delayMs = tonumber(ARGV[2])\nlocal prefix = ARGV[3]\nlocal defaultQueue = ARGV[4]\nlocal overrideQueue = ARGV[5]\n\nlocal readyAt = now + delayMs\nlocal moved = 0\nfor i = 6, #ARGV do\n  local id = ARGV[i]\n  local taskKey = taskPrefix .. id\n  if redis.call(\"EXISTS\", taskKey) == 1 then\n    local queue = overrideQueue\n    if queue == \"\" then\n      queue = redis.call(\"HGET\", taskKey, \"queue\")\n      if queue == false or queue == \"\" then\n        queue = defaultQueue\n      end\n    else\n      redis.call(\"HSET\", taskKey, \"queue\", queue)\n    end\n    local readyKey = prefix .. \":ready:\" .. queue\n    local processingKey = prefix .. \":processing:\" .. queue\n    redis.call(\"LREM\", dead, 0, id)\n    redis.call(\"ZREM\", readyKey, id)\n    redis.call(\"ZREM\", processingKey, id)\n    redis.call(\"HSET\", taskKey, \"ready_at_ms\", readyAt, \"updated_at_ms\", now)\n    redis.call(\"ZADD\", readyKey, readyAt, id)\n    redis.call(\"SADD\", queuesKey, queue)\n    moved = moved + 1\n  end\nreturn"

type retryOptions struct {
	ids          []string
	queue        string
	delay        time.Duration
	apply        bool
	defaultQueue string
}

func newDurableRetryCmd(cfg *redisConfig) *cobra.Command {
	opts := &retryOptions{
		defaultQueue: defaultQueueName,
	}

	cmd := &cobra.Command{
		Use:   "retry",
		Short: "Requeue durable tasks by ID",
		RunE: func(_ *cobra.Command, _ []string) error {
			return runDurableRetry(cfg, *opts)
		},
	}

	flags := cmd.Flags()
	flags.StringArrayVar(&opts.ids, "id", nil, "task ID to requeue (repeatable)")
	flags.StringVar(&opts.queue, "queue", "", "override queue name for requeue")
	flags.DurationVar(&opts.delay, "delay", 0, "delay before requeue (e.g. 5s)")
	flags.BoolVar(&opts.apply, "apply", false, "apply requeue (default is dry-run)")
	flags.StringVar(&opts.defaultQueue, "default-queue", opts.defaultQueue, "fallback queue when task queue is missing")

	return cmd
}

func runDurableRetry(cfg *redisConfig, opts retryOptions) error {
	ids := flattenIDs(opts.ids)
	if len(ids) == 0 {
		return ewrap.New("at least one --id is required")
	}

	if !opts.apply {
		fmt.Fprintf(os.Stdout, "dry-run: use --apply to requeue %d task(s)\n", len(ids))

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

	moved, err := retryTasks(ctx, client, keys, prefix, opts, delayMs, ids)
	if err != nil {
		return err
	}

	fmt.Fprintf(os.Stdout, "requeued %d task(s)\n", moved)

	return nil
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
		return 0, fmt.Errorf("requeue: %w", err)
	}

	moved, err := resp.AsInt64()
	if err != nil {
		return 0, fmt.Errorf("requeue result: %w", err)
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
