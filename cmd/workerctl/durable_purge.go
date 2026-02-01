package main

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/hyp3rd/ewrap"
	"github.com/redis/rueidis"
	"github.com/spf13/cobra"
)

type purgeOptions struct {
	queue      string
	limit      int
	ready      bool
	processing bool
	dlq        bool
	apply      bool
}

func newDurablePurgeCmd(cfg *redisConfig) *cobra.Command {
	opts := &purgeOptions{}

	cmd := &cobra.Command{
		Use:   "purge",
		Short: "Purge durable queue items",
		RunE: func(_ *cobra.Command, _ []string) error {
			return runDurablePurge(cfg, *opts)
		},
	}

	flags := cmd.Flags()
	flags.StringVar(&opts.queue, "queue", "", "queue name (empty for all queues)")
	flags.IntVar(&opts.limit, "limit", 0, "max items to purge per queue (0 = all)")
	flags.BoolVar(&opts.ready, "ready", false, "purge ready queue")
	flags.BoolVar(&opts.processing, "processing", false, "purge processing queue")
	flags.BoolVar(&opts.dlq, "dlq", false, "purge dead letter queue")
	flags.BoolVar(&opts.apply, "apply", false, "apply purge (default is dry-run)")

	return cmd
}

func runDurablePurge(cfg *redisConfig, opts purgeOptions) error {
	if !opts.ready && !opts.processing && !opts.dlq {
		return ewrap.New("at least one of --ready, --processing, or --dlq is required")
	}

	if !opts.apply {
		fmt.Fprintln(os.Stdout, "dry-run: use --apply to purge")

		return nil
	}

	client, err := cfg.client()
	if err != nil {
		return err
	}
	defer client.Close()

	ctx, cancel := cfg.context()
	defer cancel()

	prefix := keyPrefix(cfg.prefix)

	if opts.ready || opts.processing {
		queues, err := resolveQueues(ctx, client, prefix+":queues", opts.queue)
		if err != nil {
			return err
		}

		err = purgeQueues(ctx, client, prefix, queues, opts)
		if err != nil {
			return err
		}
	}

	if opts.dlq {
		err := purgeDLQ(ctx, client, prefix, opts.limit)
		if err != nil {
			return err
		}
	}

	return nil
}

func purgeQueues(
	ctx context.Context,
	client rueidis.Client,
	prefix string,
	queues []string,
	opts purgeOptions,
) error {
	limit := normalizePurgeLimit(opts.limit)

	for _, name := range queues {
		if opts.ready {
			readyKey := queueKey(prefix, "ready", name)

			removed, err := purgeZSet(ctx, client, readyKey, limit)
			if err != nil {
				return err
			}

			fmt.Fprintf(os.Stdout, "purged ready queue=%s removed=%d\n", name, removed)
		}

		if opts.processing {
			processingKey := queueKey(prefix, "processing", name)

			removed, err := purgeZSet(ctx, client, processingKey, limit)
			if err != nil {
				return err
			}

			fmt.Fprintf(os.Stdout, "purged processing queue=%s removed=%d\n", name, removed)
		}
	}

	return nil
}

func purgeZSet(ctx context.Context, client rueidis.Client, key string, limit int64) (int64, error) {
	start := int64(0)

	stop := int64(-1)
	if limit > 0 {
		stop = limit - 1
	}

	removed, err := client.Do(
		ctx,
		client.B().Zremrangebyrank().Key(key).Start(start).Stop(stop).Build(),
	).AsInt64()
	if err != nil {
		return 0, ewrap.Wrapf(err, "purge %s", key)
	}

	return removed, nil
}

func purgeDLQ(ctx context.Context, client rueidis.Client, prefix string, limit int) error {
	deadKey := prefix + ":dead"

	count := limit
	if count <= 0 {
		length, err := client.Do(ctx, client.B().Llen().Key(deadKey).Build()).AsInt64()
		if err != nil {
			return ewrap.Wrap(err, "dlq size")
		}

		count = int(length)
	}

	if count <= 0 {
		fmt.Fprintln(os.Stdout, "purged dlq removed=0")

		return nil
	}

	removed, err := purgeList(ctx, client, deadKey, count)
	if err != nil {
		return err
	}

	fmt.Fprintf(os.Stdout, "purged dlq removed=%d\n", removed)

	return nil
}

func purgeList(ctx context.Context, client rueidis.Client, key string, count int) (int, error) {
	script := rueidis.NewLuaScript(purgeListScript)

	resp := script.Exec(
		ctx,
		client,
		[]string{key},
		[]string{strconv.Itoa(count)},
	)

	err := resp.Error()
	if err != nil {
		return 0, ewrap.Wrap(err, "purge list")
	}

	removed, err := resp.AsInt64()
	if err != nil {
		return 0, ewrap.Wrap(err, "purge list result")
	}

	return int(removed), nil
}

func normalizePurgeLimit(limit int) int64 {
	if limit <= 0 {
		return 0
	}

	return int64(limit)
}

const purgeListScript = `
local key = KEYS[1]
local limit = tonumber(ARGV[1])

local removed = 0
for i = 1, limit do
  local id = redis.call("LPOP", key)
  if not id then
    break
  end
  removed = removed + 1
end

return removed
`
