package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/goccy/go-json"
	"github.com/hyp3rd/ewrap"
	"github.com/redis/rueidis"
	"github.com/spf13/cobra"
)

const (
	taskStatusReady      = "ready"
	taskStatusProcessing = "processing"
	taskStatusDead       = "dead"
)

type snapshotEntry struct {
	ID     string            `json:"id"`
	Status string            `json:"status"`
	Queue  string            `json:"queue"`
	Score  int64             `json:"score,omitempty"`
	Fields map[string]string `json:"fields"`
}

const (
	snapshotEntriesInitCap  = 256
	parseFloatBitSize       = 64
	snapshotParseIntBase10  = 10
	snapshotParseIntBitSize = 64
)

type snapshotExportOptions struct {
	out               string
	queue             string
	limit             int
	includeReady      bool
	includeProcessing bool
	includeDLQ        bool
}

type snapshotImportOptions struct {
	in    string
	apply bool
}

func newDurableSnapshotCmd(cfg *redisConfig) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "snapshot",
		Short: "Export/import durable queue snapshots",
	}

	cmd.AddCommand(newDurableSnapshotExportCmd(cfg))
	cmd.AddCommand(newDurableSnapshotImportCmd(cfg))

	return cmd
}

func newDurableSnapshotExportCmd(cfg *redisConfig) *cobra.Command {
	opts := &snapshotExportOptions{
		out:          "-",
		limit:        defaultDumpLimit,
		includeReady: true,
	}

	cmd := &cobra.Command{
		Use:   "export",
		Short: "Export queue snapshot as JSONL",
		RunE: func(_ *cobra.Command, _ []string) error {
			return runSnapshotExport(cfg, *opts)
		},
	}

	flags := cmd.Flags()
	flags.StringVar(&opts.out, "out", opts.out, "output file path or '-' for stdout")
	flags.StringVar(&opts.queue, "queue", "", "queue name filter")
	flags.IntVar(&opts.limit, "limit", opts.limit, "max items per source (0 = all)")
	flags.BoolVar(&opts.includeReady, taskStatusReady, opts.includeReady, "include ready")
	flags.BoolVar(&opts.includeProcessing, taskStatusProcessing, false, "include processing")
	flags.BoolVar(&opts.includeDLQ, "dlq", false, "include dead letter queue")

	return cmd
}

func newDurableSnapshotImportCmd(cfg *redisConfig) *cobra.Command {
	opts := &snapshotImportOptions{
		in: "-",
	}

	cmd := &cobra.Command{
		Use:   "import",
		Short: "Import queue snapshot from JSONL",
		RunE: func(_ *cobra.Command, _ []string) error {
			return runSnapshotImport(cfg, *opts)
		},
	}

	flags := cmd.Flags()
	flags.StringVar(&opts.in, "in", opts.in, "input file path or '-' for stdin")
	flags.BoolVar(&opts.apply, "apply", false, "apply import (default is dry-run)")

	return cmd
}

func runSnapshotExport(cfg *redisConfig, opts snapshotExportOptions) error {
	opts = normalizeSnapshotExportOptions(opts)

	client, err := cfg.client()
	if err != nil {
		return err
	}
	defer client.Close()

	ctx, cancel := cfg.context()
	defer cancel()

	writer, closeFn, err := openOutput(opts.out)
	if err != nil {
		return err
	}
	defer closeFn()

	prefix := keyPrefix(cfg.prefix)

	queues, err := resolveQueues(ctx, client, prefix+":queues", opts.queue)
	if err != nil {
		return err
	}

	limit := normalizeDumpLimit(opts.limit)
	encoder := json.NewEncoder(writer)

	return exportSnapshotData(ctx, client, prefix, queues, limit, encoder, opts)
}

func runSnapshotImport(cfg *redisConfig, opts snapshotImportOptions) error {
	reader, closeFn, err := openInput(opts.in)
	if err != nil {
		return err
	}
	defer closeFn()

	scanner := bufio.NewScanner(reader)
	entries := make([]snapshotEntry, 0, snapshotEntriesInitCap)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var entry snapshotEntry

		err := json.Unmarshal([]byte(line), &entry)
		if err != nil {
			return ewrap.Wrapf(err, "decode snapshot entry")
		}

		entries = append(entries, entry)
	}

	err = scanner.Err()
	if err != nil {
		return ewrap.Wrap(err, "read snapshot")
	}

	if !opts.apply {
		fmt.Fprintf(os.Stdout, "dry-run: use --apply to import %d snapshot item(s)\n", len(entries))

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
	imported := 0

	for _, entry := range entries {
		err := importSnapshotEntry(ctx, client, prefix, entry)
		if err != nil {
			return err
		}

		imported++
	}

	fmt.Fprintf(os.Stdout, "imported %d snapshot item(s)\n", imported)

	return nil
}

func exportZSetStatus(
	ctx context.Context,
	client rueidis.Client,
	prefix string,
	queues []string,
	status string,
	limit int64,
	encoder *json.Encoder,
) error {
	for _, queue := range queues {
		key := queueKey(prefix, status, queue)

		ids, err := fetchZSetIDs(ctx, client, key, limit)
		if err != nil {
			return err
		}

		for _, id := range ids {
			entry, err := snapshotFromTask(ctx, client, prefix, id, status, queue)
			if err != nil {
				return err
			}

			err = encoder.Encode(entry)
			if err != nil {
				return ewrap.Wrap(err, "encode snapshot")
			}
		}
	}

	return nil
}

func exportDLQ(
	ctx context.Context,
	client rueidis.Client,
	prefix string,
	limit int64,
	encoder *json.Encoder,
) error {
	ids, err := fetchListIDs(ctx, client, prefix+":dead", limit)
	if err != nil {
		return err
	}

	for _, id := range ids {
		entry, err := snapshotFromTask(ctx, client, prefix, id, taskStatusDead, "")
		if err != nil {
			return err
		}

		err = encoder.Encode(entry)
		if err != nil {
			return ewrap.Wrap(err, "encode snapshot")
		}
	}

	return nil
}

func snapshotFromTask(
	ctx context.Context,
	client rueidis.Client,
	prefix string,
	id string,
	status string,
	queue string,
) (snapshotEntry, error) {
	fields, err := fetchTaskFields(ctx, client, prefix, id)
	if err != nil {
		return snapshotEntry{}, err
	}

	if queue == "" {
		queue = strings.TrimSpace(fields["queue"])
	}

	if queue == "" {
		queue = defaultQueueName
	}

	score := int64(0)
	if status == taskStatusReady || status == taskStatusProcessing {
		score, err = fetchZSetScore(ctx, client, queueKey(prefix, status, queue), id)
		if err != nil {
			return snapshotEntry{}, err
		}
	}

	return snapshotEntry{
		ID:     id,
		Status: status,
		Queue:  queue,
		Score:  score,
		Fields: fields,
	}, nil
}

func fetchTaskFields(ctx context.Context, client rueidis.Client, prefix, id string) (map[string]string, error) {
	taskKey := prefix + ":task:" + id

	values, err := client.Do(ctx, client.B().Hgetall().Key(taskKey).Build()).AsStrMap()
	if err != nil {
		return nil, ewrap.Wrap(err, "read task fields")
	}

	return values, nil
}

func fetchZSetScore(ctx context.Context, client rueidis.Client, key, member string) (int64, error) {
	resp := client.Do(ctx, client.B().Zscore().Key(key).Member(member).Build())

	score, err := resp.ToString()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			return 0, nil
		}

		return 0, ewrap.Wrap(err, "read score")
	}

	if score == "" {
		return 0, nil
	}

	fscore, err := strconv.ParseFloat(score, parseFloatBitSize)
	if err != nil {
		return 0, ewrap.Wrap(err, "parse score")
	}

	return int64(fscore), nil
}

func importSnapshotEntry(ctx context.Context, client rueidis.Client, prefix string, entry snapshotEntry) error {
	if strings.TrimSpace(entry.ID) == "" {
		return ewrap.New("snapshot entry missing id")
	}

	taskKey, queue := normalizeImportEntry(prefix, &entry)

	err := writeTaskHash(ctx, client, taskKey, entry.Fields)
	if err != nil {
		return err
	}

	err = clearTaskMembership(ctx, client, prefix, entry.ID, queue)
	if err != nil {
		return err
	}

	err = addImportedEntry(ctx, client, prefix, entry, queue)
	if err != nil {
		return err
	}

	err = client.Do(ctx, client.B().Sadd().Key(prefix+":queues").Member(queue).Build()).Error()
	if err != nil {
		return ewrap.Wrap(err, "track queue")
	}

	return nil
}

func normalizeSnapshotExportOptions(opts snapshotExportOptions) snapshotExportOptions {
	if !opts.includeReady && !opts.includeProcessing && !opts.includeDLQ {
		opts.includeReady = true
	}

	return opts
}

func exportSnapshotData(
	ctx context.Context,
	client rueidis.Client,
	prefix string,
	queues []string,
	limit int64,
	encoder *json.Encoder,
	opts snapshotExportOptions,
) error {
	if opts.includeReady {
		err := exportZSetStatus(ctx, client, prefix, queues, taskStatusReady, limit, encoder)
		if err != nil {
			return err
		}
	}

	if opts.includeProcessing {
		err := exportZSetStatus(ctx, client, prefix, queues, taskStatusProcessing, limit, encoder)
		if err != nil {
			return err
		}
	}

	if opts.includeDLQ {
		err := exportDLQ(ctx, client, prefix, limit, encoder)
		if err != nil {
			return err
		}
	}

	return nil
}

func normalizeImportEntry(prefix string, entry *snapshotEntry) (taskKey, queue string) {
	taskKey = prefix + ":task:" + entry.ID

	queue = strings.TrimSpace(entry.Queue)
	if queue == "" {
		queue = strings.TrimSpace(entry.Fields["queue"])
	}

	if queue == "" {
		queue = defaultQueueName
	}

	if entry.Fields == nil {
		entry.Fields = map[string]string{}
	}

	entry.Fields["queue"] = queue

	return taskKey, queue
}

func addImportedEntry(
	ctx context.Context,
	client rueidis.Client,
	prefix string,
	entry snapshotEntry,
	queue string,
) error {
	switch entry.Status {
	case taskStatusDead:
		return addImportedDead(ctx, client, prefix, entry.ID)
	case taskStatusProcessing:
		score := entry.Score
		if score <= 0 {
			score = timeNowMillis()
		}

		return addImportedZSet(ctx, client, prefix, taskStatusProcessing, queue, entry.ID, score)
	default:
		score := entry.Score
		if score <= 0 {
			score = parseInt64Or(entry.Fields["ready_at_ms"], timeNowMillis())
		}

		return addImportedZSet(ctx, client, prefix, taskStatusReady, queue, entry.ID, score)
	}
}

func addImportedDead(ctx context.Context, client rueidis.Client, prefix, id string) error {
	err := client.Do(ctx, client.B().Rpush().Key(prefix+":dead").Element(id).Build()).Error()
	if err != nil {
		return ewrap.Wrap(err, "push dead")
	}

	return nil
}

func addImportedZSet(
	ctx context.Context,
	client rueidis.Client,
	prefix string,
	status string,
	queue string,
	id string,
	score int64,
) error {
	err := client.Do(
		ctx,
		client.B().Zadd().Key(queueKey(prefix, status, queue)).ScoreMember().ScoreMember(float64(score), id).Build(),
	).Error()
	if err != nil {
		return ewrap.Wrap(err, "add "+status)
	}

	return nil
}

func writeTaskHash(ctx context.Context, client rueidis.Client, taskKey string, fields map[string]string) error {
	if len(fields) == 0 {
		return nil
	}

	builder := client.B().Hset().Key(taskKey).FieldValue()
	for key, value := range fields {
		builder = builder.FieldValue(key, value)
	}

	err := client.Do(ctx, builder.Build()).Error()
	if err != nil {
		return ewrap.Wrap(err, "write task hash")
	}

	return nil
}

func clearTaskMembership(ctx context.Context, client rueidis.Client, prefix, id, queue string) error {
	err := client.Do(ctx, client.B().Zrem().Key(queueKey(prefix, taskStatusReady, queue)).Member(id).Build()).Error()
	if err != nil {
		return ewrap.Wrap(err, "clear ready")
	}

	err = client.Do(ctx, client.B().Zrem().Key(queueKey(prefix, taskStatusProcessing, queue)).Member(id).Build()).Error()
	if err != nil {
		return ewrap.Wrap(err, "clear processing")
	}

	err = client.Do(ctx, client.B().Lrem().Key(prefix+":dead").Count(0).Element(id).Build()).Error()
	if err != nil {
		return ewrap.Wrap(err, "clear dead")
	}

	return nil
}

func parseInt64Or(raw string, fallback int64) int64 {
	value, err := strconv.ParseInt(raw, snapshotParseIntBase10, snapshotParseIntBitSize)
	if err != nil || value <= 0 {
		return fallback
	}

	return value
}

func openOutput(path string) (*os.File, func(), error) {
	if path == "" || path == "-" {
		return os.Stdout, func() {}, nil
	}

	file, err := os.Create(filepath.Base(path))
	if err != nil {
		return nil, nil, ewrap.Wrap(err, "open output")
	}

	return file, func() {
		err = file.Close()
		if err != nil {
			fmt.Fprintf(os.Stderr, "close output: %v\n", err)
		}
	}, nil
}

func openInput(path string) (*os.File, func(), error) {
	if path == "" || path == "-" {
		return os.Stdin, func() {}, nil
	}

	file, err := os.Open(filepath.Base(path))
	if err != nil {
		return nil, nil, ewrap.Wrap(err, "open input")
	}

	return file, func() {
		err = file.Close()
		if err != nil {
			fmt.Fprintf(os.Stderr, "close output: %v\n", err)
		}
	}, nil
}
