package main

import (
	"time"

	"github.com/spf13/cobra"
)

const (
	defaultRedisAddr     = "localhost:6380"
	defaultRedisPassword = "supersecret"
	defaultRedisPrefix   = "go-worker"
	defaultTimeout       = 5 * time.Second
)

func newRootCmd() *cobra.Command {
	redisCfg := &redisConfig{
		addr:     defaultRedisAddr,
		password: defaultRedisPassword,
		prefix:   defaultRedisPrefix,
		timeout:  defaultTimeout,
	}

	cmd := &cobra.Command{
		Use:          "workerctl",
		Short:        "go-worker admin CLI",
		SilenceUsage: true,
	}

	flags := cmd.PersistentFlags()
	flags.StringVar(&redisCfg.addr, "redis-addr", redisCfg.addr, "redis host:port")
	flags.StringVar(&redisCfg.password, "redis-password", redisCfg.password, "redis password (empty for no auth)")
	flags.StringVar(&redisCfg.prefix, "redis-prefix", redisCfg.prefix, "redis key prefix")
	flags.DurationVar(&redisCfg.timeout, "timeout", redisCfg.timeout, "redis operation timeout")
	flags.BoolVar(&redisCfg.useTLS, "tls", false, "use TLS for Redis connection")
	flags.BoolVar(&redisCfg.skipTLSVerify, "tls-insecure", false, "skip TLS certificate verification")

	cmd.AddCommand(newDurableCmd(redisCfg))
	cmd.AddCommand(newCompletionCmd(cmd))

	return cmd
}
