package main

import (
	"context"
	"flag"
	"log"
	"strconv"
	"strings"

	"github.com/redis/rueidis"
)

const (
	defaultRedisAddr     = "localhost:6380"
	defaultRedisPassword = "supersecret"
	defaultRedisPrefix   = "go-worker"
	defaultPeekCount     = 5
	defaultTimeout       = 5 * time.Second
)

func main() {
	redisAddr := flag.String("redis-addr", defaultRedisAddr, "redis host:port")
	redisPassword := flag.String("redis-password", defaultRedisPassword, "redis password (empty for no auth)")
	redisPrefix := flag.String("redis-prefix", defaultRedisPrefix, "redis key prefix")
	peek := flag.Int("peek", defaultPeekCount, "number of ready task IDs to display")
	timeout := flag.Duration("timeout", defaultTimeout, "redis operation timeout")
	showIDs := flag.Bool("show-ids", false, "print ready task IDs (off by default)")
	flag.Parse()

	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{*redisAddr},
		Password:    *redisPassword,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	prefix := keyPrefix(*redisPrefix)
	readyKey := prefix + ":ready"
	processingKey := prefix + ":processing"
	deadKey := prefix + ":dead"

	readyCount, readyErr := client.Do(ctx, client.B().Zcard().Key(readyKey).Build()).AsInt64()
	processingCount, processingErr := client.Do(ctx, client.B().Zcard().Key(processingKey).Build()).AsInt64()
	deadCount, deadErr := client.Do(ctx, client.B().Llen().Key(deadKey).Build()).AsInt64()

	if readyErr != nil || processingErr != nil || deadErr != nil {
		log.Fatalf("count error ready=%v processing=%v dead=%v", readyErr, processingErr, deadErr)
	}

	log.Printf("ready=%d processing=%d dead=%d", readyCount, processingCount, deadCount)

	if !*showIDs || *peek <= 0 {
		return
	}

	ids, err := client.Do(ctx, client.B().Zrange().Key(readyKey).Min("0").Max(strconv.Itoa(*peek-1)).Build()).AsStrSlice()
	if err != nil {
		log.Fatalf("peek ready: %v", err)
	}

	if len(ids) == 0 {
		log.Printf("ready IDs: none")
		return
	}

	log.Printf("ready IDs: %s", strings.Join(ids, ", "))
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
