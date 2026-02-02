package main

import (
	"context"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-json"
	"github.com/hyp3rd/ewrap"
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
	handler           string
	minRetries        int
	maxRetries        int
	olderThan         time.Duration
	newerThan         time.Duration
}

type taskDump struct {
	ID          string            `json:"id"`
	Status      string            `json:"status"`
	Handler     string            `json:"handler,omitempty"`
	Queue       string            `json:"queue,omitempty"`
	Priority    int               `json:"priority,omitempty"`
	Weight      int               `json:"weight,omitempty"`
	Retries     int               `json:"retries,omitempty"`
	Attempts    int               `json:"attempts,omitempty"`
	ReadyAt     string            `json:"ready_at,omitempty"`
	UpdatedAt   string            `json:"updated_at,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	readyAtMs   int64
	updatedAtMs int64
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
	flags.StringVar(&opts.handler, "handler", "", "filter by handler name")
	flags.IntVar(&opts.minRetries, "min-retries", 0, "minimum retries to include")
	flags.IntVar(&opts.maxRetries, "max-retries", 0, "maximum retries to include")
	flags.DurationVar(&opts.olderThan, "older-than", 0, "include tasks older than this duration")
	flags.DurationVar(&opts.newerThan, "newer-than", 0, "include tasks newer than this duration")

	return cmd
}

func runDurableDump(cfg *redisConfig, opts dumpOptions) error {
	if !opts.includeReady && !opts.includeProcessing && !opts.includeDLQ {
		opts.includeReady = true
	}

	opts.handler = strings.TrimSpace(opts.handler)

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

	dumpCtx := newDumpContext(client, opts, prefix)
	dumpCtx.encoder = json.NewEncoder(os.Stdout)
	dumpCtx.limit = normalizeDumpLimit(opts.limit)

	if opts.includeReady {
		err := dumpQueues(ctx, dumpCtx, queues, "ready")
		if err != nil {
			return err
		}
	}

	if opts.includeProcessing {
		err := dumpQueues(ctx, dumpCtx, queues, "processing")
		if err != nil {
			return err
		}
	}

	if opts.includeDLQ {
		err := dumpDLQ(ctx, dumpCtx)
		if err != nil {
			return err
		}
	}

	return nil
}

type dumpContext struct {
	client    rueidis.Client
	encoder   *json.Encoder
	prefix    string
	opts      dumpOptions
	limit     int64
	nowMillis int64
}

func newDumpContext(client rueidis.Client, opts dumpOptions, prefix string) dumpContext {
	return dumpContext{
		client:    client,
		opts:      opts,
		prefix:    prefix,
		nowMillis: time.Now().UnixMilli(),
	}
}

func dumpQueues(ctx context.Context, dumpCtx dumpContext, queues []string, status string) error {
	for _, name := range queues {
		key := queueKey(dumpCtx.prefix, status, name)

		ids, err := fetchZSetIDs(ctx, dumpCtx.client, key, dumpCtx.limit)
		if err != nil {
			return err
		}

		err = dumpIDs(ctx, dumpCtx, ids, status)
		if err != nil {
			return err
		}
	}

	return nil
}

func dumpDLQ(ctx context.Context, dumpCtx dumpContext) error {
	deadKey := dumpCtx.prefix + ":dead"

	ids, err := fetchListIDs(ctx, dumpCtx.client, deadKey, dumpCtx.limit)
	if err != nil {
		return err
	}

	return dumpIDs(ctx, dumpCtx, ids, "dead")
}

func dumpIDs(ctx context.Context, dumpCtx dumpContext, ids []string, status string) error {
	for _, id := range ids {
		record, err := fetchTaskDump(ctx, dumpCtx.client, dumpCtx.prefix, id, status, dumpCtx.opts)
		if err != nil {
			return err
		}

		if !shouldIncludeDump(record, dumpCtx) {
			continue
		}

		err = dumpCtx.encoder.Encode(record)
		if err != nil {
			return ewrap.Wrap(err, "encode dump")
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
		return nil, ewrap.Wrapf(err, "read %s", key)
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
		return nil, ewrap.Wrapf(err, "read %s", key)
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
		return taskDump{}, ewrap.Wrapf(err, "read task %s", id)
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
	record.readyAtMs = parseMillisInt(get(dumpFieldReadyAt))
	record.updatedAtMs = parseMillisInt(get(dumpFieldUpdatedAt))
	record.ReadyAt = formatMillis(record.readyAtMs)
	record.UpdatedAt = formatMillis(record.updatedAtMs)

	if opts.includeMetadata && len(values) > dumpFieldMetadata {
		record.Metadata = parseMetadata(get(dumpFieldMetadata))
	}
}

func shouldIncludeDump(record taskDump, dumpCtx dumpContext) bool {
	if !matchesHandler(record, dumpCtx.opts) {
		return false
	}

	if !matchesRetryBounds(record, dumpCtx.opts) {
		return false
	}

	return matchesAgeWindow(record, dumpCtx)
}

func matchesHandler(record taskDump, opts dumpOptions) bool {
	if opts.handler == "" {
		return true
	}

	return record.Handler == opts.handler
}

func matchesRetryBounds(record taskDump, opts dumpOptions) bool {
	if opts.minRetries > 0 && record.Retries < opts.minRetries {
		return false
	}

	if opts.maxRetries > 0 && record.Retries > opts.maxRetries {
		return false
	}

	return true
}

func matchesAgeWindow(record taskDump, dumpCtx dumpContext) bool {
	timestamp := record.updatedAtMs
	if timestamp == 0 {
		timestamp = record.readyAtMs
	}

	if !withinOlderThan(timestamp, dumpCtx) {
		return false
	}

	return withinNewerThan(timestamp, dumpCtx)
}

func withinOlderThan(timestamp int64, dumpCtx dumpContext) bool {
	if dumpCtx.opts.olderThan <= 0 || timestamp <= 0 {
		return true
	}

	cutoff := dumpCtx.nowMillis - dumpCtx.opts.olderThan.Milliseconds()

	return timestamp <= cutoff
}

func withinNewerThan(timestamp int64, dumpCtx dumpContext) bool {
	if dumpCtx.opts.newerThan <= 0 || timestamp <= 0 {
		return true
	}

	cutoff := dumpCtx.nowMillis - dumpCtx.opts.newerThan.Milliseconds()

	return timestamp >= cutoff
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

func parseMillisInt(raw string) int64 {
	if raw == "" {
		return 0
	}

	value, err := strconv.ParseInt(raw, parseIntBase10, parseIntBitSize)
	if err != nil || value <= 0 {
		return 0
	}

	return value
}

func formatMillis(value int64) string {
	if value <= 0 {
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
