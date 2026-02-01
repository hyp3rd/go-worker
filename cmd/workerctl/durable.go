package main

import "github.com/spf13/cobra"

func newDurableCmd(cfg *redisConfig) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "durable",
		Short: "Durable backend tools",
	}

	cmd.AddCommand(
		newDurableInspectCmd(cfg),
		newDurableDLQCmd(cfg),
	)

	return cmd
}
