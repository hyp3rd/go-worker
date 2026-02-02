package main

import (
	"fmt"
	"os"

	"github.com/hyp3rd/ewrap"
	"github.com/spf13/cobra"
)

const applyFlag = "apply"

func newDurablePauseCmd(cfg *redisConfig) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pause",
		Short: "Pause durable dequeue on all workers",
		RunE: func(cmd *cobra.Command, _ []string) error {
			apply, err := cmd.Flags().GetBool(applyFlag)
			if err != nil {
				return ewrap.Wrap(err, "get apply flag")
			}

			return setDurablePauseState(cfg, true, apply)
		},
	}

	cmd.Flags().Bool(applyFlag, false, "apply pause (default is dry-run)")

	return cmd
}

func newDurableResumeCmd(cfg *redisConfig) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "resume",
		Short: "Resume durable dequeue on all workers",
		RunE: func(cmd *cobra.Command, _ []string) error {
			apply, err := cmd.Flags().GetBool(applyFlag)
			if err != nil {
				return ewrap.Wrap(err, "get apply flag")
			}

			return setDurablePauseState(cfg, false, apply)
		},
	}

	cmd.Flags().Bool(applyFlag, false, "apply resume (default is dry-run)")

	return cmd
}

func setDurablePauseState(cfg *redisConfig, paused, apply bool) error {
	action := "resume"
	if paused {
		action = "pause"
	}

	if !apply {
		fmt.Fprintf(os.Stdout, "dry-run: use --apply to %s durable dequeue\n", action)

		return nil
	}

	client, err := cfg.client()
	if err != nil {
		return ewrap.Wrap(err, "redis client")
	}
	defer client.Close()

	ctx, cancel := cfg.context()
	defer cancel()

	key := pausedKey(keyPrefix(cfg.prefix))
	if paused {
		err = client.Do(ctx, client.B().Set().Key(key).Value("1").Build()).Error()
	} else {
		err = client.Do(ctx, client.B().Del().Key(key).Build()).Error()
	}

	if err != nil {
		return ewrap.Wrapf(err, "%s durable dequeue", action)
	}

	fmt.Fprintf(os.Stdout, "durable dequeue %sd\n", action)

	return nil
}
