package main

import (
	"context"
	"flag"
	"log"
	"strconv"
	"strings"
	"time"

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
	queue := flag.String("queue", "", "queue name (empty for all queues)")
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
	deadKey := prefix + ":dead"
	queuesKey := prefix + ":queues"

	deadCount, deadErr := client.Do(ctx, client.B().Llen().Key(deadKey).Build()).AsInt64()
	if deadErr != nil {
		log.Fatalf("dead count error: %v", deadErr)
	}

	queues := []string{}
	if strings.TrimSpace(*queue) != "" {
		queues = []string{*queue}
	} else {
		list, err := client.Do(ctx, client.B().Smembers().Key(queuesKey).Build()).AsStrSlice()
		if err != nil {
			log.Fatalf("queue list error: %v", err)
		}
		if len(list) == 0 {
			queues = []string{defaultQueueName}
		} else {
			queues = list
		}
	}

	totalReady := int64(0)
	totalProcessing := int64(0)
	for _, name := range queues {
		readyKey := queueKey(prefix, "ready", name)
		processingKey := queueKey(prefix, "processing", name)

		readyCount, readyErr := client.Do(ctx, client.B().Zcard().Key(readyKey).Build()).AsInt64()
		processingCount, processingErr := client.Do(ctx, client.B().Zcard().Key(processingKey).Build()).AsInt64()
		if readyErr != nil || processingErr != nil {
			log.Fatalf("count error queue=%s ready=%v processing=%v", name, readyErr, processingErr)
		}

		totalReady += readyCount
		totalProcessing += processingCount
		log.Printf("queue=%s ready=%d processing=%d", name, readyCount, processingCount)
	}

	log.Printf("ready=%d processing=%d dead=%d", totalReady, totalProcessing, deadCount)

	if !*showIDs || *peek <= 0 {
		return
	}

	if strings.TrimSpace(*queue) == "" {
		log.Printf("ready IDs: specify -queue to display IDs")
		return
	}

	readyKey := queueKey(prefix, "ready", *queue)
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

func queueKey(prefix, base, queue string) string {
	queue = strings.TrimSpace(queue)
	if queue == "" {
		queue = defaultQueueName
	}

	return prefix + ":" + base + ":" + queue
}

const defaultQueueName = "default"
