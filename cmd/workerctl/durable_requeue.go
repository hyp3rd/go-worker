package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/hyp3rd/ewrap"
	"github.com/spf13/cobra"
)

type requeueOptions struct {
	queue        string
	processing   bool
	limit        int
	delay        time.Duration
	apply        bool
	defaultQueue string
}

func newDurableRequeueCmd(cfg *redisConfig) *cobra.Command {
	opts := &requeueOptions{
		defaultQueue: defaultQueueName,
		limit:        defaultRetryLimit,
	}

	cmd := &cobra.Command{
		Use:   "requeue",
		Short: "Requeue tasks from ready or processing queues",
		RunE: func(_ *cobra.Command, _ []string) error {
			return runDurableRequeue(cfg, *opts)
		},
	}

	flags := cmd.Flags()
	flags.StringVar(&opts.queue, "queue", "", "queue name to requeue from")
	flags.BoolVar(&opts.processing, "processing", false, "requeue from processing instead of ready")
	flags.IntVar(&opts.limit, "limit", opts.limit, "max tasks to requeue (0 = all)")
	flags.DurationVar(&opts.delay, "delay", 0, "delay before requeue (e.g. 5s)")
	flags.BoolVar(&opts.apply, "apply", false, "apply requeue (default is dry-run)")
	flags.StringVar(&opts.defaultQueue, "default-queue", opts.defaultQueue, "fallback queue when task queue is missing")

	return cmd
}

func runDurableRequeue(cfg *redisConfig, opts requeueOptions) error {
	queue := strings.TrimSpace(opts.queue)
	if queue == "" {
		return ewrap.New("--queue is required")
	}

	source := retrySourceReady
	if opts.processing {
		source = retrySourceProc
	}

	retryOpts := retryOptions{
		source:       source,
		fromQueue:    queue,
		limit:        opts.limit,
		delay:        opts.delay,
		apply:        opts.apply,
		defaultQueue: opts.defaultQueue,
	}

	if !opts.apply {
		fmt.Fprintf(os.Stdout, "dry-run: use --apply to requeue from %s\n", source)

		return nil
	}

	return runDurableRetry(cfg, retryOpts)
}
