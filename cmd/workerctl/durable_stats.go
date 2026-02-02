package main

import (
	"context"
	"os"
	"os/signal"
	"time"

	"github.com/goccy/go-json"
	"github.com/hyp3rd/ewrap"
	"github.com/redis/rueidis"
	"github.com/spf13/cobra"
)

type statsOptions struct {
	jsonOutput bool
	watch      time.Duration
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
	flags.DurationVar(&opts.watch, "watch", 0, "refresh interval (e.g. 2s)")

	return cmd
}

func runDurableStats(cfg *redisConfig, opts statsOptions) error {
	client, err := cfg.client()
	if err != nil {
		return err
	}
	defer client.Close()

	snapshot, err := collectDurableStats(cfg, client)
	if err != nil {
		return err
	}

	err = outputStats(snapshot, opts.jsonOutput)
	if err != nil {
		return ewrap.Wrap(err, "output stats")
	}

	if opts.watch <= 0 {
		return nil
	}

	shutdownCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	ticker := time.NewTicker(opts.watch)
	defer ticker.Stop()

	for {
		select {
		case <-shutdownCtx.Done():
			return nil
		case <-ticker.C:
			next, collectErr := collectDurableStats(cfg, client)
			if collectErr != nil {
				return collectErr
			}

			outErr := outputStats(next, opts.jsonOutput)
			if outErr != nil {
				return ewrap.Wrap(outErr, "output stats")
			}
		}
	}
}

func collectDurableStats(cfg *redisConfig, client rueidis.Client) (statsSnapshot, error) {
	ctx, cancel := cfg.context()
	defer cancel()

	prefix := keyPrefix(cfg.prefix)

	deadCount, err := fetchDeadCount(ctx, client, prefix)
	if err != nil {
		return statsSnapshot{}, err
	}

	queues, err := resolveQueues(ctx, client, prefix+":queues", "")
	if err != nil {
		return statsSnapshot{}, err
	}

	counts, totals, err := fetchQueueCounts(ctx, client, prefix, queues)
	if err != nil {
		return statsSnapshot{}, err
	}

	return statsSnapshot{
		Queues:          counts,
		TotalReady:      totals.ready,
		TotalProcessing: totals.processing,
		Dead:            deadCount,
	}, nil
}

func outputStats(snapshot statsSnapshot, jsonOutput bool) error {
	if jsonOutput {
		err := json.NewEncoder(os.Stdout).Encode(snapshot)
		if err != nil {
			return ewrap.Wrap(err, "encode json")
		}

		return nil
	}

	printQueueCounts(snapshot.Queues, queueTotals{
		ready:      snapshot.TotalReady,
		processing: snapshot.TotalProcessing,
	}, snapshot.Dead)

	return nil
}
