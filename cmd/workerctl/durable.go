package main

import "github.com/spf13/cobra"

func newDurableCmd(cfg *redisConfig) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "durable",
		Short: "Durable backend tools",
	}

	cmd.AddCommand(
		newDurableInspectCmd(cfg),
		newDurablePauseCmd(cfg),
		newDurableResumeCmd(cfg),
		newDurablePausedCmd(cfg),
		newDurableDLQCmd(cfg),
		newDurableRetryCmd(cfg),
		newDurableRequeueCmd(cfg),
		newDurablePurgeCmd(cfg),
		newDurableQueuesCmd(cfg),
		newDurableStatsCmd(cfg),
		newDurableDeleteCmd(cfg),
		newDurableGetCmd(cfg),
		newDurableSnapshotCmd(cfg),
		newDurableDumpCmd(cfg),
	)

	return cmd
}
