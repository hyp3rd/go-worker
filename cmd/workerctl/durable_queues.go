package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

type queueListOptions struct {
	withCounts bool
}

func newDurableQueuesCmd(cfg *redisConfig) *cobra.Command {
	opts := &queueListOptions{}

	cmd := &cobra.Command{
		Use:   "queues",
		Short: "List known durable queues",
		RunE: func(_ *cobra.Command, _ []string) error {
			return runDurableQueues(cfg, *opts)
		},
	}

	flags := cmd.Flags()
	flags.BoolVar(&opts.withCounts, "with-counts", false, "include ready/processing counts")

	return cmd
}

func runDurableQueues(cfg *redisConfig, opts queueListOptions) error {
	client, err := cfg.client()
	if err != nil {
		return err
	}
	defer client.Close()

	ctx, cancel := cfg.context()
	defer cancel()

	prefix := keyPrefix(cfg.prefix)

	queues, err := resolveQueues(ctx, client, prefix+":queues", "")
	if err != nil {
		return err
	}

	if !opts.withCounts {
		for _, name := range queues {
			fmt.Fprintf(os.Stdout, "%s\n", name)
		}

		return nil
	}

	counts, totals, err := fetchQueueCounts(ctx, client, prefix, queues)
	if err != nil {
		return err
	}

	printQueueCounts(counts, totals, 0)

	return nil
}
