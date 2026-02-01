package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/redis/rueidis"
	"github.com/spf13/cobra"
)

const defaultDumpLimit = 100
const (
	dumpFieldHandler = iota
	dumpFieldQueue
	dumpFieldPriority
	dumpFieldWeight
	dumpFieldRetries
	dumpFieldAttempts
	dumpFieldReadyAt
	dumpFieldUpdatedAt
	dumpFieldMetadata
)

const (
	parseIntBase10  = 10
	parseIntBitSize = 64
)

type dumpOptions struct {
	queue             string
	limit             int
	includeReady      bool
	includeProcessing bool
	includeDLQ        bool
	includeMetadata   bool
}

type taskDump struct {
	ID        string            `json:"id"`
	Status    string            `json:"status"`
	Handler   string            `json:"handler,omitempty"`
	Queue     string            `json:"queue,omitempty"`
	Priority  int               `json:"priority,omitempty"`
	Weight    int               `json:"weight,omitempty"`
	Retries   int               `json:"retries,omitempty"`
	Attempts  int               `json:"attempts,omitempty"`
	ReadyAt   string            `json:"ready_at,omitempty"`
	UpdatedAt string            `json:"updated_at,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

func newDurableDumpCmd(cfg *redisConfig) *cobra.Command {
	opts := &dumpOptions{
		includeReady: true,
		limit:        defaultDumpLimit,
	}

	cmd := &cobra.Command{
		Use:   "dump",
		Short: "Dump durable task metadata",
		RunE: func(_ *cobra.Command, _ []string) error {
			return runDurableDump(cfg, *opts)
		},
	}

	flags := cmd.Flags()
	flags.StringVar(&opts.queue, "queue", "", "queue name (empty for all queues)")
	flags.IntVar(&opts.limit, "limit", opts.limit, "max items per queue/status (0 = all)")
	flags.BoolVar(&opts.includeReady, "ready", opts.includeReady, "include ready tasks")
	flags.BoolVar(&opts.includeProcessing, "processing", false, "include processing tasks")
	flags.BoolVar(&opts.includeDLQ, "dlq", false, "include dead letter queue items")
	flags.BoolVar(&opts.includeMetadata, "include-metadata", false, "include metadata payload (JSON)")

	return cmd
}

func runDurableDump(cfg *redisConfig, opts dumpOptions) error {
	if !opts.includeReady && !opts.includeProcessing && !opts.includeDLQ {
		opts.includeReady = true
	}

	client, err := cfg.client()
	if err != nil {
		return err
	}
	defer client.Close()

	ctx, cancel := cfg.context()
	defer cancel()

	prefix := keyPrefix(cfg.prefix)

	queues, err := resolveQueues(ctx, client, prefix+":queues", opts.queue)
	if err != nil {
		return err
	}

	encoder := json.NewEncoder(os.Stdout)
	limit := normalizeDumpLimit(opts.limit)

	if opts.includeReady {
		err := dumpQueues(ctx, client, encoder, prefix, queues, "ready", limit, opts)
		if err != nil {
			return err
		}
	}

	if opts.includeProcessing {
		err := dumpQueues(ctx, client, encoder, prefix, queues, "processing", limit, opts)
		if err != nil {
			return err
		}
	}

	if opts.includeDLQ {
		err := dumpDLQ(ctx, client, encoder, prefix, limit, opts)
		if err != nil {
			return err
		}
	}

	return nil
}

func dumpQueues(
	ctx context.Context,
	client rueidis.Client,
	encoder *json.Encoder,
	prefix string,
	queues []string,
	status string,
	limit int64,
	opts dumpOptions,
) error {
	for _, name := range queues {
		key := queueKey(prefix, status, name)

		ids, err := fetchZSetIDs(ctx, client, key, limit)
		if err != nil {
			return err
		}

		err = dumpIDs(ctx, client, encoder, prefix, ids, status, opts)
		if err != nil {
			return err
		}
	}

	return nil
}

func dumpDLQ(
	ctx context.Context,
	client rueidis.Client,
	encoder *json.Encoder,
	prefix string,
	limit int64,
	opts dumpOptions,
) error {
	deadKey := prefix + ":dead"

	ids, err := fetchListIDs(ctx, client, deadKey, limit)
	if err != nil {
		return err
	}

	return dumpIDs(ctx, client, encoder, prefix, ids, "dead", opts)
}

func dumpIDs(
	ctx context.Context,
	client rueidis.Client,
	encoder *json.Encoder,
	prefix string,
	ids []string,
	status string,
	opts dumpOptions,
) error {
	for _, id := range ids {
		record, err := fetchTaskDump(ctx, client, prefix, id, status, opts)
		if err != nil {
			return err
		}

		err = encoder.Encode(record)
		if err != nil {
			return fmt.Errorf("encode dump: %w", err)
		}
	}

	return nil
}

func fetchZSetIDs(ctx context.Context, client rueidis.Client, key string, limit int64) ([]string, error) {
	start := int64(0)

	stop := int64(-1)
	if limit > 0 {
		stop = limit - 1
	}

	ids, err := client.Do(
		ctx,
		client.B().Zrange().Key(key).Min(strconv.FormatInt(start, 10)).Max(strconv.FormatInt(stop, 10)).Build(),
	).AsStrSlice()
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", key, err)
	}

	return ids, nil
}

func fetchListIDs(ctx context.Context, client rueidis.Client, key string, limit int64) ([]string, error) {
	start := int64(0)

	stop := int64(-1)
	if limit > 0 {
		stop = limit - 1
	}

	ids, err := client.Do(
		ctx,
		client.B().Lrange().Key(key).Start(start).Stop(stop).Build(),
	).AsStrSlice()
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", key, err)
	}

	return ids, nil
}

func fetchTaskDump(
	ctx context.Context,
	client rueidis.Client,
	prefix string,
	id string,
	status string,
	opts dumpOptions,
) (taskDump, error) {
	taskKey := prefix + ":task:" + id

	fields := []string{
		"handler",
		"queue",
		"priority",
		"weight",
		"retries",
		"attempts",
		"ready_at_ms",
		"updated_at_ms",
	}
	if opts.includeMetadata {
		fields = append(fields, "metadata")
	}

	resp := client.Do(ctx, client.B().Hmget().Key(taskKey).Field(fields...).Build())

	values, err := resp.AsStrSlice()
	if err != nil {
		return taskDump{}, fmt.Errorf("read task %s: %w", id, err)
	}

	record := taskDump{
		ID:     id,
		Status: status,
	}

	assignTaskFields(&record, values, opts)

	if record.Queue == "" {
		record.Queue = defaultQueueName
	}

	return record, nil
}

func assignTaskFields(record *taskDump, values []string, opts dumpOptions) {
	get := func(idx int) string {
		if idx < 0 || idx >= len(values) {
			return ""
		}

		return values[idx]
	}

	record.Handler = get(dumpFieldHandler)
	record.Queue = get(dumpFieldQueue)
	record.Priority = parseInt(get(dumpFieldPriority))
	record.Weight = parseInt(get(dumpFieldWeight))
	record.Retries = parseInt(get(dumpFieldRetries))
	record.Attempts = parseInt(get(dumpFieldAttempts))
	record.ReadyAt = parseMillis(get(dumpFieldReadyAt))
	record.UpdatedAt = parseMillis(get(dumpFieldUpdatedAt))

	if opts.includeMetadata && len(values) > dumpFieldMetadata {
		record.Metadata = parseMetadata(get(dumpFieldMetadata))
	}
}

func parseInt(raw string) int {
	if raw == "" {
		return 0
	}

	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0
	}

	return value
}

func parseMillis(raw string) string {
	if raw == "" {
		return ""
	}

	value, err := strconv.ParseInt(raw, parseIntBase10, parseIntBitSize)
	if err != nil || value <= 0 {
		return ""
	}

	return time.UnixMilli(value).UTC().Format(time.RFC3339)
}

func parseMetadata(raw string) map[string]string {
	if raw == "" {
		return nil
	}

	out := map[string]string{}

	err := json.Unmarshal([]byte(raw), &out)
	if err != nil {
		return nil
	}

	return out
}

func normalizeDumpLimit(limit int) int64 {
	if limit <= 0 {
		return 0
	}

	return int64(limit)
}
