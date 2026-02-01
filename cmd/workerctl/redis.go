package main

import (
	"context"
	"crypto/tls"
	"strings"
	"time"

	"github.com/hyp3rd/ewrap"
	"github.com/redis/rueidis"
)

const defaultQueueName = "default"

type redisConfig struct {
	addr          string
	password      string
	prefix        string
	timeout       time.Duration
	useTLS        bool
	skipTLSVerify bool
}

func (cfg *redisConfig) client() (rueidis.Client, error) {
	options := rueidis.ClientOption{
		InitAddress: []string{cfg.addr},
		Password:    cfg.password,
	}

	if cfg.useTLS {
		options.TLSConfig = &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: cfg.skipTLSVerify, //nolint:gosec
		}
	}

	client, err := rueidis.NewClient(options)
	if err != nil {
		return nil, ewrap.Wrap(err, "failed to create redis client")
	}

	return client, nil
}

func (cfg *redisConfig) context() (context.Context, context.CancelFunc) {
	timeout := cfg.timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}

	return context.WithTimeout(context.Background(), timeout)
}

func keyPrefix(prefix string) string {
	if prefix == "" {
		return "go-worker"
	}

	if strings.Contains(prefix, "{") {
		return prefix
	}

	return "{" + prefix + "}"
}

func queueKey(prefix, base, queue string) string {
	return prefix + ":" + base + ":" + queue
}
