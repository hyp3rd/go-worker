package main

import (
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/goccy/go-json"
	"github.com/google/uuid"
	"github.com/hyp3rd/ewrap"
	"github.com/redis/rueidis"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	worker "github.com/hyp3rd/go-worker"
)

const (
	payloadFormatJSON = "json"
	payloadFormatYAML = "yaml"
)

type enqueueOptions struct {
	id             string
	handler        string
	queue          string
	payload        string
	payloadFile    string
	payloadB64     string
	payloadB64File string
	payloadFmt     string
	priority       int
	weight         int
	retries        int
	retryDelay     time.Duration
	delay          time.Duration
	runAt          string
	metadata       []string
	apply          bool
	defaultQueue   string
}

func newDurableEnqueueCmd(cfg *redisConfig) *cobra.Command {
	opts := &enqueueOptions{
		defaultQueue: defaultQueueName,
	}

	cmd := &cobra.Command{
		Use:   "enqueue",
		Short: "Create a durable task from JSON/YAML payload",
		RunE: func(_ *cobra.Command, _ []string) error {
			return runDurableEnqueue(cfg, *opts)
		},
	}

	flags := cmd.Flags()
	flags.StringVar(&opts.id, "id", "", "task ID (optional, UUID)")
	flags.StringVar(&opts.handler, "handler", "", "handler name (required)")
	flags.StringVar(&opts.queue, "queue", "", "queue name")
	flags.StringVar(&opts.payload, "payload", "", "inline payload (JSON or YAML)")
	flags.StringVar(&opts.payloadFile, "payload-file", "", "payload file path (JSON or YAML, use '-' for stdin)")
	flags.StringVar(&opts.payloadB64, "payload-b64", "", "base64 payload (raw bytes)")
	flags.StringVar(&opts.payloadB64File, "payload-b64-file", "", "file with base64 payload (raw bytes)")
	flags.StringVar(&opts.payloadFmt, "payload-format", "", "payload format: json or yaml")
	flags.IntVar(&opts.priority, "priority", 1, "task priority (default 1)")
	flags.IntVar(&opts.weight, "weight", 0, "task weight (default from server)")
	flags.IntVar(&opts.retries, "retries", 0, "max retry attempts")
	flags.DurationVar(&opts.retryDelay, "retry-delay", 0, "retry delay (e.g. 5s)")
	flags.DurationVar(&opts.delay, "delay", 0, "delay before enqueue (e.g. 30s)")
	flags.StringVar(&opts.runAt, "run-at", "", "run at time (RFC3339)")
	flags.StringArrayVar(&opts.metadata, "meta", nil, "metadata key=value (repeatable)")
	flags.BoolVar(&opts.apply, "apply", false, "apply enqueue (default is dry-run)")
	flags.StringVar(&opts.defaultQueue, "default-queue", opts.defaultQueue, "fallback queue when task queue is missing")

	return cmd
}

func runDurableEnqueue(cfg *redisConfig, opts enqueueOptions) error {
	task, err := buildDurableTask(opts)
	if err != nil {
		return err
	}

	if !opts.apply {
		fmt.Fprintf(os.Stdout, "dry-run: use --apply to enqueue task %s\n", task.ID.String())

		return nil
	}

	client, err := cfg.client()
	if err != nil {
		return ewrap.Wrap(err, "redis client")
	}
	defer client.Close()

	ctx, cancel := cfg.context()
	defer cancel()

	backend, err := newDurableBackend(client, cfg.prefix, opts.defaultQueue)
	if err != nil {
		return err
	}

	err = backend.Enqueue(ctx, task)
	if err != nil {
		return err
	}

	fmt.Fprintf(os.Stdout, "enqueued durable task id=%s\n", task.ID.String())

	return nil
}

func buildDurableTask(opts enqueueOptions) (worker.DurableTask, error) {
	handler := strings.TrimSpace(opts.handler)
	if handler == "" {
		return worker.DurableTask{}, ewrap.New("handler is required")
	}

	payload, err := loadPayload(opts)
	if err != nil {
		return worker.DurableTask{}, err
	}

	taskID, err := parseTaskID(opts.id)
	if err != nil {
		return worker.DurableTask{}, err
	}

	queue := strings.TrimSpace(opts.queue)
	if queue == "" {
		queue = opts.defaultQueue
	}

	runAt, err := parseRunAt(opts.runAt, opts.delay)
	if err != nil {
		return worker.DurableTask{}, err
	}

	meta, err := parseMetadataPairs(opts.metadata)
	if err != nil {
		return worker.DurableTask{}, err
	}

	return worker.DurableTask{
		ID:         taskID,
		Handler:    handler,
		Payload:    payload,
		Priority:   opts.priority,
		Queue:      queue,
		Weight:     opts.weight,
		Retries:    opts.retries,
		RetryDelay: opts.retryDelay,
		RunAt:      runAt,
		Metadata:   meta,
	}, nil
}

func loadPayload(opts enqueueOptions) ([]byte, error) {
	err := validatePayloadSource(opts)
	if err != nil {
		return nil, err
	}

	if isBase64Payload(opts) {
		return loadBase64Payload(opts)
	}

	return loadStructuredPayload(opts)
}

func validatePayloadSource(opts enqueueOptions) error {
	if payloadSourceCount(opts) != 1 {
		return ewrap.New("use exactly one of --payload, --payload-file, --payload-b64, or --payload-b64-file")
	}

	if isBase64Payload(opts) && strings.TrimSpace(opts.payloadFmt) != "" {
		return ewrap.New("payload format is not applicable with base64 payloads")
	}

	return nil
}

func isBase64Payload(opts enqueueOptions) bool {
	return opts.payloadB64 != "" || opts.payloadB64File != ""
}

func loadBase64Payload(opts enqueueOptions) ([]byte, error) {
	if opts.payloadB64 != "" {
		return decodePayloadBase64([]byte(opts.payloadB64))
	}

	reader, closeFn, err := openInput(opts.payloadB64File)
	if err != nil {
		return nil, err
	}
	defer closeFn()

	raw, err := readAll(reader)
	if err != nil {
		return nil, err
	}

	return decodePayloadBase64(raw)
}

func loadStructuredPayload(opts enqueueOptions) ([]byte, error) {
	format, err := resolvePayloadFormat(opts.payloadFmt, opts.payloadFile)
	if err != nil {
		return nil, err
	}

	if opts.payloadFile != "" {
		reader, closeFn, err := openInput(opts.payloadFile)
		if err != nil {
			return nil, err
		}
		defer closeFn()

		raw, err := readAll(reader)
		if err != nil {
			return nil, err
		}

		return decodePayload(raw, format)
	}

	return decodePayload([]byte(opts.payload), format)
}

func resolvePayloadFormat(format, payloadFile string) (string, error) {
	format = strings.ToLower(strings.TrimSpace(format))
	if format == "" {
		return detectPayloadFormat(payloadFile), nil
	}

	switch format {
	case payloadFormatJSON, payloadFormatYAML:
		return format, nil
	default:
		return "", ewrap.New("payload format must be json or yaml")
	}
}

func detectPayloadFormat(payloadFile string) string {
	ext := strings.ToLower(filepath.Ext(payloadFile))
	switch ext {
	case ".yaml", ".yml":
		return payloadFormatYAML
	default:
		return payloadFormatJSON
	}
}

func decodePayload(raw []byte, format string) ([]byte, error) {
	switch format {
	case payloadFormatJSON:
		if !json.Valid(raw) {
			return nil, ewrap.New("payload is not valid JSON")
		}

		return raw, nil
	case payloadFormatYAML:
		var value any

		err := yaml.Unmarshal(raw, &value)
		if err != nil {
			return nil, ewrap.Wrap(err, "parse yaml payload")
		}

		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, ewrap.Wrap(err, "encode yaml payload")
		}

		return encoded, nil
	default:
		return nil, ewrap.New("payload format must be json or yaml")
	}
}

func decodePayloadBase64(raw []byte) ([]byte, error) {
	clean := stripWhitespace(string(raw))
	if clean == "" {
		return nil, ewrap.New("base64 payload is empty")
	}

	decoded, err := base64.StdEncoding.DecodeString(clean)
	if err != nil {
		return nil, ewrap.Wrap(err, "decode base64 payload")
	}

	return decoded, nil
}

func stripWhitespace(value string) string {
	return strings.Map(func(r rune) rune {
		switch r {
		case ' ', '\n', '\r', '\t':
			return -1
		default:
			return r
		}
	}, value)
}

func parseTaskID(raw string) (uuid.UUID, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return uuid.New(), nil
	}

	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.UUID{}, ewrap.Wrap(err, "parse task id")
	}

	return id, nil
}

func parseRunAt(raw string, delay time.Duration) (time.Time, error) {
	if raw != "" && delay > 0 {
		return time.Time{}, ewrap.New("use only one of --run-at or --delay")
	}

	if raw != "" {
		timestamp, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			return time.Time{}, ewrap.Wrap(err, "parse run-at")
		}

		return timestamp, nil
	}

	if delay > 0 {
		return time.Now().Add(delay), nil
	}

	return time.Time{}, nil
}

func payloadSourceCount(opts enqueueOptions) int {
	count := 0
	if opts.payload != "" {
		count++
	}

	if opts.payloadFile != "" {
		count++
	}

	if opts.payloadB64 != "" {
		count++
	}

	if opts.payloadB64File != "" {
		count++
	}

	return count
}

func parseMetadataPairs(entries []string) (map[string]string, error) {
	if len(entries) == 0 {
		return map[string]string{}, nil
	}

	meta := make(map[string]string, len(entries))
	for _, entry := range entries {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}

		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 {
			return nil, ewrap.New("metadata must be key=value")
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		if key == "" {
			return nil, ewrap.New("metadata key is required")
		}

		meta[key] = value
	}

	if len(meta) == 0 {
		return map[string]string{}, nil
	}

	return meta, nil
}

func newDurableBackend(client rueidis.Client, prefix, defaultQueue string) (*worker.RedisDurableBackend, error) {
	return worker.NewRedisDurableBackend(
		client,
		worker.WithRedisDurablePrefix(prefix),
		worker.WithRedisDurableDefaultQueue(defaultQueue),
	)
}

func readAll(reader io.Reader) ([]byte, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, ewrap.Wrap(err, "read payload")
	}

	return data, nil
}
