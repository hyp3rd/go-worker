package main

import (
	"fmt"
	"os"

	"github.com/hyp3rd/ewrap"
	"github.com/redis/rueidis"
	"github.com/spf13/cobra"
)

func newDurablePausedCmd(cfg *redisConfig) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "paused",
		Short: "Show durable pause status",
		RunE: func(_ *cobra.Command, _ []string) error {
			return runDurablePaused(cfg)
		},
	}

	return cmd
}

func runDurablePaused(cfg *redisConfig) error {
	client, err := cfg.client()
	if err != nil {
		return ewrap.Wrap(err, "redis client")
	}
	defer client.Close()

	ctx, cancel := cfg.context()
	defer cancel()

	key := pausedKey(keyPrefix(cfg.prefix))
	resp := client.Do(ctx, client.B().Get().Key(key).Build())

	value, err := resp.ToString()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			fmt.Fprintln(os.Stdout, "paused=false")

			return nil
		}

		return ewrap.Wrap(err, "read pause state")
	}

	if value == "1" {
		fmt.Fprintln(os.Stdout, "paused=true")

		return nil
	}

	fmt.Fprintln(os.Stdout, "paused=false")

	return nil
}
