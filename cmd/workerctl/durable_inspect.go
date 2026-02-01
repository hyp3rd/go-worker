package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/hyp3rd/ewrap"
	"github.com/redis/rueidis"
	"github.com/spf13/cobra"
)

const defaultPeekCount = 5

type inspectOptions struct {
	queue   string
	peek    int
	showIDs bool
}

type queueCounts struct {
	name       string
	ready      int64
	processing int64
}

func newDurableInspectCmd(cfg *redisConfig) *cobra.Command {
	opts := &inspectOptions{
		peek: defaultPeekCount,
	}

	cmd := &cobra.Command{
		Use:   "inspect",
		Short: "Inspect durable queue counts",
		RunE: func(_ *cobra.Command, _ []string) error {
			return runDurableInspect(cfg, *opts)
		},
	}

	flags := cmd.Flags()
	flags.StringVar(&opts.queue, "queue", "", "queue name (empty for all queues)")
	flags.IntVar(&opts.peek, "peek", opts.peek, "number of ready task IDs to display")
	flags.BoolVar(&opts.showIDs, "show-ids", false, "print ready task IDs (off by default)")

	return cmd
}

func runDurableInspect(cfg *redisConfig, opts inspectOptions) error {
	client, err := cfg.client()
	if err != nil {
		return err
	}
	defer client.Close()

	ctx, cancel := cfg.context()
	defer cancel()

	prefix := keyPrefix(cfg.prefix)

	deadCount, err := fetchDeadCount(ctx, client, prefix)
	if err != nil {
		return err
	}

	queues, err := resolveQueues(ctx, client, prefix+":queues", opts.queue)
	if err != nil {
		return err
	}

	counts, totals, err := fetchQueueCounts(ctx, client, prefix, queues)
	if err != nil {
		return err
	}

	printQueueCounts(counts, totals, deadCount)

	if !opts.showIDs || opts.peek <= 0 {
		return nil
	}

	return printReadyIDs(ctx, client, prefix, opts.queue, opts.peek)
}

func fetchDeadCount(ctx context.Context, client rueidis.Client, prefix string) (int64, error) {
	deadKey := prefix + ":dead"

	deadCount, err := client.Do(ctx, client.B().Llen().Key(deadKey).Build()).AsInt64()
	if err != nil {
		return 0, ewrap.Wrap(err, "dead count")
	}

	return deadCount, nil
}

type queueTotals struct {
	ready      int64
	processing int64
}

func fetchQueueCounts(
	ctx context.Context,
	client rueidis.Client,
	prefix string,
	queues []string,
) ([]queueCounts, queueTotals, error) {
	counts := make([]queueCounts, 0, len(queues))
	totals := queueTotals{}

	for _, name := range queues {
		readyKey := queueKey(prefix, "ready", name)
		processingKey := queueKey(prefix, "processing", name)

		readyCount, readyErr := client.Do(ctx, client.B().Zcard().Key(readyKey).Build()).AsInt64()

		processingCount, processingErr := client.Do(ctx, client.B().Zcard().Key(processingKey).Build()).AsInt64()
		if readyErr != nil || processingErr != nil {
			return nil, queueTotals{}, ewrap.Newf("count error queue=%s ready=%v processing=%v", name, readyErr, processingErr)
		}

		totals.ready += readyCount
		totals.processing += processingCount
		counts = append(counts, queueCounts{name: name, ready: readyCount, processing: processingCount})
	}

	return counts, totals, nil
}

func printQueueCounts(counts []queueCounts, totals queueTotals, deadCount int64) {
	for _, entry := range counts {
		fmt.Fprintf(os.Stdout, "queue=%s ready=%d processing=%d\n", entry.name, entry.ready, entry.processing)
	}

	fmt.Fprintf(os.Stdout, "ready=%d processing=%d dead=%d\n", totals.ready, totals.processing, deadCount)
}

func printReadyIDs(ctx context.Context, client rueidis.Client, prefix, queue string, peek int) error {
	if strings.TrimSpace(queue) == "" {
		fmt.Fprintln(os.Stdout, "ready IDs: specify --queue to display IDs")

		return nil
	}

	readyKey := queueKey(prefix, "ready", queue)

	ids, err := client.Do(
		ctx,
		client.B().Zrange().Key(readyKey).Min("0").Max(strconv.Itoa(peek-1)).Build(),
	).AsStrSlice()
	if err != nil {
		return ewrap.Wrap(err, "peek ready")
	}

	if len(ids) == 0 {
		fmt.Fprintln(os.Stdout, "ready IDs: none")

		return nil
	}

	fmt.Fprintf(os.Stdout, "ready IDs: %s\n", strings.Join(ids, ", "))

	return nil
}

func resolveQueues(ctx context.Context, client rueidis.Client, queuesKey, filter string) ([]string, error) {
	if strings.TrimSpace(filter) != "" {
		return []string{filter}, nil
	}

	list, err := client.Do(ctx, client.B().Smembers().Key(queuesKey).Build()).AsStrSlice()
	if err != nil {
		return nil, ewrap.Wrap(err, "queue list")
	}

	if len(list) == 0 {
		return []string{defaultQueueName}, nil
	}

	sort.Strings(list)

	return list, nil
}
