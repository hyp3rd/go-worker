package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/hyp3rd/ewrap"
	"github.com/redis/rueidis"
	"github.com/spf13/cobra"
)

const defaultDLQBatchSize = 100

//nolint:revive
const replayScript = "\nlocal dead = KEYS[1]\nlocal taskPrefix = KEYS[2]\nlocal queuesKey = KEYS[3]\nlocal now = tonumber(ARGV[1])\nlocal limit = tonumber(ARGV[2])\nlocal prefix = ARGV[3]\nlocal defaultQueue = ARGV[4]\n\nlocal moved = 0\nfor i = 1, limit do\n  local id = redis.call(\"RPOP\", dead)\n  if not id then\n    break\n  end\n  local taskKey = taskPrefix .. id\n  if redis.call(\"EXISTS\", taskKey) == 1 then\n    local queue = redis.call(\"HGET\", taskKey, \"queue\")\n    if queue == false or queue == \"\" then\n      queue = defaultQueue\n    end\n    local readyKey = prefix .. \":ready:\" .. queue\n    redis.call(\"HSET\", taskKey, \"ready_at_ms\", now, \"updated_at_ms\", now)\n    redis.call(\"ZADD\", readyKey, now, id)\n    redis.call(\"SADD\", queuesKey, queue)\n    moved = moved + 1\n  end\nreturn"

func newDurableDLQCmd(cfg *redisConfig) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dlq",
		Short: "Dead letter queue tools",
	}

	cmd.AddCommand(newDurableDLQReplayCmd(cfg))

	return cmd
}

func newDurableDLQReplayCmd(cfg *redisConfig) *cobra.Command {
	opts := &dlqReplayOptions{
		batch:        defaultDLQBatchSize,
		showCounts:   true,
		defaultQueue: defaultQueueName,
	}

	cmd := &cobra.Command{
		Use:   "replay",
		Short: "Replay dead letter queue items back into ready queues",
		RunE: func(_ *cobra.Command, _ []string) error {
			return runDLQReplay(cfg, *opts)
		},
	}

	flags := cmd.Flags()
	flags.IntVar(&opts.batch, "batch", opts.batch, "max DLQ items to replay")
	flags.BoolVar(&opts.apply, "apply", false, "apply replay (default is dry-run)")
	flags.BoolVar(&opts.showCounts, "show-counts", opts.showCounts, "print DLQ size before/after replay")
	flags.StringVar(&opts.defaultQueue, "default-queue", opts.defaultQueue, "queue name used when tasks are missing a queue")

	return cmd
}

type dlqReplayOptions struct {
	batch        int
	apply        bool
	showCounts   bool
	defaultQueue string
}

type dlqReplayKeys struct {
	deadKey    string
	queuesKey  string
	taskPrefix string
}

func runDLQReplay(cfg *redisConfig, opts dlqReplayOptions) error {
	client, err := cfg.client()
	if err != nil {
		return err
	}
	defer client.Close()

	ctx, cancel := cfg.context()
	defer cancel()

	prefix := keyPrefix(cfg.prefix)
	keys := dlqKeys(prefix)
	limit := normalizeDLQBatch(opts.batch)

	if opts.showCounts {
		err := printDLQCount(ctx, client, keys.deadKey, "before")
		if err != nil {
			return err
		}
	}

	if !opts.apply {
		fmt.Fprintf(os.Stdout, "dry-run: use --apply to replay up to %d DLQ item(s)\n", limit)

		return nil
	}

	moved, err := replayDLQ(ctx, client, keys, prefix, opts.defaultQueue, limit)
	if err != nil {
		return err
	}

	fmt.Fprintf(os.Stdout, "replayed %d DLQ item(s)\n", moved)

	if opts.showCounts {
		err := printDLQCount(ctx, client, keys.deadKey, "after")
		if err != nil {
			return err
		}
	}

	return nil
}

func dlqKeys(prefix string) dlqReplayKeys {
	return dlqReplayKeys{
		deadKey:    prefix + ":dead",
		queuesKey:  prefix + ":queues",
		taskPrefix: prefix + ":task:",
	}
}

func normalizeDLQBatch(batch int) int {
	if batch <= 0 {
		return defaultDLQBatchSize
	}

	return batch
}

func printDLQCount(ctx context.Context, client rueidis.Client, key, label string) error {
	count, err := client.Do(ctx, client.B().Llen().Key(key).Build()).AsInt64()
	if err != nil {
		return ewrap.Newf("dlq size %s: %v", label, err)
	}

	fmt.Fprintf(os.Stdout, "DLQ size %s: %d\n", label, count)

	return nil
}

func replayDLQ(
	ctx context.Context,
	client rueidis.Client,
	keys dlqReplayKeys,
	prefix string,
	defaultQueue string,
	limit int,
) (int64, error) {
	script := rueidis.NewLuaScript(replayScript)

	resp := script.Exec(
		ctx,
		client,
		[]string{keys.deadKey, keys.taskPrefix, keys.queuesKey},
		[]string{
			strconv.FormatInt(time.Now().UnixMilli(), 10),
			strconv.Itoa(limit),
			prefix,
			defaultQueue,
		},
	)

	err := resp.Error()
	if err != nil {
		return 0, ewrap.Newf("dlq replay: %v", err)
	}

	moved, err := resp.AsInt64()
	if err != nil {
		return 0, ewrap.Newf("dlq replay result: %v", err)
	}

	return moved, nil
}
