package main

import (
	"fmt"
	"runtime"

	"github.com/spf13/cobra"
)

//nolint:gochecknoglobals
var (
	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

func versionString() string {
	return fmt.Sprintf("%s (commit=%s, date=%s, go=%s, os=%s/%s)", version, commit, date, runtime.Version(), runtime.GOOS, runtime.GOARCH)
}

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Run: func(cmd *cobra.Command, _ []string) {
			cmd.Println(versionString())
		},
	}
}
