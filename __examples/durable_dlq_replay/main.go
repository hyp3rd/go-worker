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
	defaultBatchSize     = 100
)

const replayScript = `
local dead = KEYS[1]
local ready = KEYS[2]
local taskPrefix = KEYS[3]
local now = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])

local moved = 0
for i = 1, limit do
  local id = redis.call("RPOP", dead)
  if not id then
    break
  end
  local taskKey = taskPrefix .. id
  if redis.call("EXISTS", taskKey) == 1 then
    redis.call("HSET", taskKey, "ready_at_ms", now, "updated_at_ms", now)
    redis.call("ZADD", ready, now, id)
    moved = moved + 1
  end
end

return moved
`

func main() {
	redisAddr := flag.String("redis-addr", defaultRedisAddr, "redis host:port")
	redisPassword := flag.String("redis-password", defaultRedisPassword, "redis password (empty for no auth)")
	redisPrefix := flag.String("redis-prefix", defaultRedisPrefix, "redis key prefix")
	batch := flag.Int("batch", defaultBatchSize, "max DLQ items to replay")
	showCounts := flag.Bool("show-counts", true, "print DLQ size before/after replay")
	flag.Parse()

	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{*redisAddr},
		Password:    *redisPassword,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	prefix := keyPrefix(*redisPrefix)
	deadKey := prefix + ":dead"
	readyKey := prefix + ":ready"
	taskPrefix := prefix + ":task:"

	now := time.Now().UnixMilli()
	limit := *batch
	if limit <= 0 {
		limit = defaultBatchSize
	}

	if *showCounts {
		before, err := client.Do(context.Background(), client.B().Llen().Key(deadKey).Build()).AsInt64()
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("DLQ size before: %d", before)
	}

	script := rueidis.NewLuaScript(replayScript)
	resp := script.Exec(
		context.Background(),
		client,
		[]string{deadKey, readyKey, taskPrefix},
		[]string{
			int64ToString(now),
			intToString(limit),
		},
	)
	err := resp.Error()
	if err != nil {
		log.Fatal(err)
	}

	moved, err := resp.AsInt64()
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("replayed %d DLQ item(s)", moved)

	if *showCounts {
		after, err := client.Do(context.Background(), client.B().Llen().Key(deadKey).Build()).AsInt64()
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("DLQ size after: %d", after)
	}
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

func int64ToString(v int64) string {
	return strconv.FormatInt(v, 10)
}

func intToString(v int) string {
	return strconv.Itoa(v)
}
