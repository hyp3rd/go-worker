package main

import (
	"context"
	"encoding/json"
	"os"

	"github.com/hyp3rd/ewrap"
	"github.com/redis/rueidis"
	"github.com/spf13/cobra"
)

type getOptions struct {
	id              string
	includeMetadata bool
}

func newDurableGetCmd(cfg *redisConfig) *cobra.Command {
	opts := &getOptions{}

	cmd := &cobra.Command{
		Use:   "get",
		Short: "Get durable task metadata by ID",
		RunE: func(_ *cobra.Command, _ []string) error {
			return runDurableGet(cfg, *opts)
		},
	}

	flags := cmd.Flags()
	flags.StringVar(&opts.id, "id", "", "task ID to fetch")
	flags.BoolVar(&opts.includeMetadata, "include-metadata", false, "include metadata payload (JSON)")

	return cmd
}

func runDurableGet(cfg *redisConfig, opts getOptions) error {
	if opts.id == "" {
		return ewrap.New("--id is required")
	}

	client, err := cfg.client()
	if err != nil {
		return err
	}
	defer client.Close()

	ctx, cancel := cfg.context()
	defer cancel()

	prefix := keyPrefix(cfg.prefix)

	exists, err := taskExists(ctx, client, prefix, opts.id)
	if err != nil {
		return err
	}

	if !exists {
		return ewrap.Newf("task %s not found", opts.id)
	}

	record, err := fetchTaskDump(ctx, client, prefix, opts.id, "unknown", dumpOptions{includeMetadata: opts.includeMetadata})
	if err != nil {
		return err
	}

	status, err := detectTaskStatus(ctx, client, prefix, record.Queue, opts.id)
	if err != nil {
		return err
	}

	record.Status = status

	encoder := json.NewEncoder(os.Stdout)

	err = encoder.Encode(record)
	if err != nil {
		return ewrap.Wrap(err, "encode task")
	}

	return nil
}

func taskExists(ctx context.Context, client rueidis.Client, prefix, id string) (bool, error) {
	taskKey := prefix + ":task:" + id

	count, err := client.Do(ctx, client.B().Exists().Key(taskKey).Build()).AsInt64()
	if err != nil {
		return false, ewrap.Wrap(err, "task exists")
	}

	return count > 0, nil
}

func detectTaskStatus(ctx context.Context, client rueidis.Client, prefix, queue, id string) (string, error) {
	if queue == "" {
		queue = defaultQueueName
	}

	readyKey := queueKey(prefix, "ready", queue)

	ready, err := zsetHasMember(ctx, client, readyKey, id)
	if err != nil {
		return "", err
	}

	if ready {
		return "ready", nil
	}

	processingKey := queueKey(prefix, "processing", queue)

	processing, err := zsetHasMember(ctx, client, processingKey, id)
	if err != nil {
		return "", err
	}

	if processing {
		return "processing", nil
	}

	deadKey := prefix + ":dead"

	inDead, err := listHasMember(ctx, client, deadKey, id)
	if err != nil {
		return "", err
	}

	if inDead {
		return "dead", nil
	}

	return "unknown", nil
}

func zsetHasMember(ctx context.Context, client rueidis.Client, key, member string) (bool, error) {
	resp := client.Do(ctx, client.B().Zscore().Key(key).Member(member).Build())

	err := resp.Error()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			return false, nil
		}

		return false, ewrap.Wrapf(err, "read %s", key)
	}

	return true, nil
}

func listHasMember(ctx context.Context, client rueidis.Client, key, member string) (bool, error) {
	resp := client.Do(ctx, client.B().Lpos().Key(key).Element(member).Build())

	err := resp.Error()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			return false, nil
		}

		return false, ewrap.Wrapf(err, "read %s", key)
	}

	return true, nil
}
