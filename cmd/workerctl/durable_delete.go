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

const deleteScript = `
local dead = KEYS[1]
local ready = KEYS[2]
local processing = KEYS[3]
local taskKey = KEYS[4]
local now = tonumber(ARGV[1])
local id = ARGV[2]
local deleteHash = ARGV[3]

redis.call("ZREM", ready, id)
redis.call("ZREM", processing, id)
redis.call("LREM", dead, 0, id)
redis.call("HSET", taskKey, "updated_at_ms", now)

if deleteHash == "1" then
  redis.call("DEL", taskKey)
end

return 1
`

type deleteOptions struct {
	id           string
	queue        string
	deleteHash   bool
	apply        bool
	defaultQueue string
}

func newDurableDeleteCmd(cfg *redisConfig) *cobra.Command {
	opts := &deleteOptions{
		defaultQueue: defaultQueueName,
	}

	cmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete a durable task from queues and optionally its hash",
		RunE: func(_ *cobra.Command, _ []string) error {
			return runDurableDelete(cfg, *opts)
		},
	}

	flags := cmd.Flags()
	flags.StringVar(&opts.id, "id", "", "task ID to delete")
	flags.StringVar(&opts.queue, "queue", "", "queue name override")
	flags.BoolVar(&opts.deleteHash, "delete-hash", false, "delete the task hash after removal")
	flags.BoolVar(&opts.apply, "apply", false, "apply delete (default is dry-run)")
	flags.StringVar(&opts.defaultQueue, "default-queue", opts.defaultQueue, "fallback queue when task queue is missing")

	return cmd
}

func runDurableDelete(cfg *redisConfig, opts deleteOptions) error {
	if strings.TrimSpace(opts.id) == "" {
		return ewrap.New("--id is required")
	}

	if !opts.apply {
		fmt.Fprintf(os.Stdout, "dry-run: use --apply to delete task %s\n", opts.id)

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

	queueName, err := resolveTaskQueue(ctx, client, prefix, opts.id, opts.queue, opts.defaultQueue)
	if err != nil {
		return err
	}

	readyKey := queueKey(prefix, "ready", queueName)
	processingKey := queueKey(prefix, "processing", queueName)
	deadKey := prefix + ":dead"
	taskKey := prefix + ":task:" + opts.id

	resp := rueidis.NewLuaScript(deleteScript).Exec(
		ctx,
		client,
		[]string{deadKey, readyKey, processingKey, taskKey},
		[]string{
			strconv.FormatInt(timeNowMillis(), 10),
			opts.id,
			boolToFlag(opts.deleteHash),
		},
	)

	err = resp.Error()
	if err != nil {
		return ewrap.Wrap(err, "delete task")
	}

	fmt.Fprintf(os.Stdout, "deleted task %s\n", opts.id)

	return nil
}

func timeNowMillis() int64 {
	return time.Now().UnixMilli()
}

func boolToFlag(value bool) string {
	if value {
		return "1"
	}

	return "0"
}

func resolveTaskQueue(
	ctx context.Context,
	client rueidis.Client,
	prefix string,
	id string,
	override string,
	fallback string,
) (string, error) {
	override = strings.TrimSpace(override)
	if override != "" {
		return override, nil
	}

	taskKey := prefix + ":task:" + id
	resp := client.Do(ctx, client.B().Hget().Key(taskKey).Field("queue").Build())

	queue, err := resp.ToString()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			if fallback == "" {
				return defaultQueueName, nil
			}

			return fallback, nil
		}

		return "", ewrap.Wrap(err, "read task queue")
	}

	queue = strings.TrimSpace(queue)
	if queue == "" {
		if fallback == "" {
			return defaultQueueName, nil
		}

		return fallback, nil
	}

	return queue, nil
}
