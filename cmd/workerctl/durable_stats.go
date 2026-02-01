package main

import (
	"os"

	"github.com/goccy/go-json"
	"github.com/hyp3rd/ewrap"
	"github.com/spf13/cobra"
)

type statsOptions struct {
	jsonOutput bool
}

type statsSnapshot struct {
	Queues          []queueCounts `json:"queues"`
	TotalReady      int64         `json:"total_ready"`
	TotalProcessing int64         `json:"total_processing"`
	Dead            int64         `json:"dead"`
}

func newDurableStatsCmd(cfg *redisConfig) *cobra.Command {
	opts := &statsOptions{}

	cmd := &cobra.Command{
		Use:   "stats",
		Short: "Show durable queue stats",
		RunE: func(_ *cobra.Command, _ []string) error {
			return runDurableStats(cfg, *opts)
		},
	}

	flags := cmd.Flags()
	flags.BoolVar(&opts.jsonOutput, "json", false, "output JSON")

	return cmd
}

func runDurableStats(cfg *redisConfig, opts statsOptions) error {
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

	queues, err := resolveQueues(ctx, client, prefix+":queues", "")
	if err != nil {
		return err
	}

	counts, totals, err := fetchQueueCounts(ctx, client, prefix, queues)
	if err != nil {
		return err
	}

	if opts.jsonOutput {
		payload := statsSnapshot{
			Queues:          counts,
			TotalReady:      totals.ready,
			TotalProcessing: totals.processing,
			Dead:            deadCount,
		}

		err := json.NewEncoder(os.Stdout).Encode(payload)
		if err != nil {
			return ewrap.Wrap(err, "encode json")
		}
	}

	printQueueCounts(counts, totals, deadCount)

	return nil
}
