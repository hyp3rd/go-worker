package main

import (
	"context"
	"crypto/tls"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/hyp3rd/ewrap"
	"github.com/redis/rueidis"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	worker "github.com/hyp3rd/go-worker"
	workerpb "github.com/hyp3rd/go-worker/pkg/worker/v1"
)

const (
	defaultRedisAddr      = "redis:6379"
	defaultRedisPrefix    = "go-worker"
	defaultGRPCAddr       = "0.0.0.0:50052"
	defaultLease          = 30 * time.Second
	defaultCronSpec       = "@every 1m"
	parseFloatBitSize     = 64
	workerShutdownTimeout = 5 * time.Second
	defaultJobOutputBytes = 64 * 1024
	defaultJobTarballMax  = 64 * 1024 * 1024
	defaultJobTarballWait = 30 * time.Second
)

func defaultDurableCronHandlers() []string {
	return []string{
		"cron_handler",
		"metrics_rollup",
		"retention_sweep",
		"dlq_sweep",
	}
}

func defaultDurableCronPresets() map[string]string {
	return map[string]string{
		"cron_handler":    "@every 5m",
		"metrics_rollup":  "@every 1m",
		"retention_sweep": "@every 6h",
		"dlq_sweep":       "@every 10m",
	}
}

type config struct {
	redisAddr     string
	redisPassword string
	redisPrefix   string
	redisTLS      bool
	redisTLSInsec bool

	grpcAddr string

	durableLease time.Duration
	globalRate   float64
	globalBurst  int
	leaderLease  time.Duration

	cronHandlers        []string
	durableCronHandlers []string
	cronSpec            string
	durableCronSpecs    map[string]string
	cronSpecs           map[string]string

	jobRepoAllowlist    []string
	jobDockerBin        string
	jobGitBin           string
	jobNetwork          string
	jobWorkDir          string
	jobOutputBytes      int
	jobTarballAllowlist []string
	jobTarballDir       string
	jobTarballMaxBytes  int64
	jobTarballTimeout   time.Duration
}

func main() {
	err := run()
	if err != nil {
		log.Fatal(err)
	}
}

func run() error {
	cfg := loadConfig()

	client, backend, err := newDurableBackend(cfg)
	if err != nil {
		return err
	}
	defer client.Close()

	jobRunner := newJobRunner(cfg)

	handlers, durableHandlers := buildHandlerMaps(cfg, jobRunner)
	tm := worker.NewTaskManagerWithOptions(
		context.Background(),
		worker.WithDurableBackend(backend),
		worker.WithDurableLease(cfg.durableLease),
		worker.WithDurableHandlers(durableHandlers),
	)
	tm.StartWorkers(context.Background())

	err = registerCronFactories(tm, cfg)
	if err != nil {
		return err
	}

	err = tm.SyncJobFactories(context.Background())
	if err != nil {
		log.Printf("job factories: %v", err)
	}

	grpcServer := grpc.NewServer()
	grpcSvc := worker.NewGRPCServer(tm, handlers)
	workerpb.RegisterWorkerServiceServer(grpcServer, grpcSvc)
	workerpb.RegisterAdminServiceServer(grpcServer, grpcSvc)

	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", cfg.grpcAddr)
	if err != nil {
		return ewrap.Wrap(err, "grpc listen")
	}

	go func() {
		log.Printf("worker gRPC listening on %s", cfg.grpcAddr)

		serveErr := grpcServer.Serve(listener)
		if serveErr != nil {
			log.Printf("grpc serve: %v", serveErr)
		}
	}()

	waitForSignal()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), workerShutdownTimeout)
	defer cancel()

	grpcServer.GracefulStop()

	stopErr := tm.StopGraceful(shutdownCtx)
	if stopErr != nil {
		return ewrap.Wrap(stopErr, "task manager shutdown")
	}

	return nil
}

func newDurableBackend(cfg config) (rueidis.Client, *worker.RedisDurableBackend, error) {
	client, err := newRedisClient(cfg)
	if err != nil {
		return nil, nil, ewrap.Wrap(err, "redis client")
	}

	backendOptions := []worker.RedisDurableOption{
		worker.WithRedisDurablePrefix(cfg.redisPrefix),
		worker.WithRedisDurableLeaderLock(cfg.leaderLease),
	}
	if cfg.globalRate > 0 && cfg.globalBurst > 0 {
		backendOptions = append(backendOptions, worker.WithRedisDurableGlobalRateLimit(cfg.globalRate, cfg.globalBurst))
	}

	backend, err := worker.NewRedisDurableBackend(client, backendOptions...)
	if err != nil {
		client.Close()

		return nil, nil, ewrap.Wrap(err, "durable backend")
	}

	return client, backend, nil
}

func loadConfig() config {
	cfg := config{
		redisAddr:           getenv("WORKER_REDIS_ADDR", defaultRedisAddr),
		redisPassword:       os.Getenv("WORKER_REDIS_PASSWORD"),
		redisPrefix:         getenv("WORKER_REDIS_PREFIX", defaultRedisPrefix),
		redisTLS:            parseBool(os.Getenv("WORKER_REDIS_TLS")),
		redisTLSInsec:       parseBool(os.Getenv("WORKER_REDIS_TLS_INSECURE")),
		grpcAddr:            getenv("WORKER_GRPC_ADDR", defaultGRPCAddr),
		durableLease:        defaultDuration(os.Getenv("WORKER_DURABLE_LEASE"), defaultLease),
		globalRate:          parseFloat(os.Getenv("WORKER_GLOBAL_RATE")),
		globalBurst:         parseInt(os.Getenv("WORKER_GLOBAL_BURST")),
		leaderLease:         defaultDuration(os.Getenv("WORKER_LEADER_LEASE"), 0),
		cronHandlers:        parseList(os.Getenv("WORKER_CRON_HANDLERS")),
		cronSpec:            getenv("WORKER_CRON_DEFAULT_SPEC", defaultCronSpec),
		jobRepoAllowlist:    parseList(os.Getenv("WORKER_JOB_REPO_ALLOWLIST")),
		jobDockerBin:        getenv("WORKER_JOB_DOCKER_BIN", "docker"),
		jobGitBin:           getenv("WORKER_JOB_GIT_BIN", "git"),
		jobNetwork:          strings.TrimSpace(os.Getenv("WORKER_JOB_NETWORK")),
		jobWorkDir:          strings.TrimSpace(os.Getenv("WORKER_JOB_WORKDIR")),
		jobOutputBytes:      parseIntWithDefault(os.Getenv("WORKER_JOB_OUTPUT_BYTES"), defaultJobOutputBytes),
		jobTarballAllowlist: parseList(os.Getenv("WORKER_JOB_TARBALL_ALLOWLIST")),
		jobTarballDir:       strings.TrimSpace(os.Getenv("WORKER_JOB_TARBALL_DIR")),
		jobTarballMaxBytes:  int64(parseIntWithDefault(os.Getenv("WORKER_JOB_TARBALL_MAX_BYTES"), defaultJobTarballMax)),
		jobTarballTimeout:   defaultDuration(os.Getenv("WORKER_JOB_TARBALL_TIMEOUT"), defaultJobTarballWait),
	}

	cfg.durableCronHandlers = append([]string{}, defaultDurableCronHandlers()...)
	cfg.durableCronHandlers = mergeUnique(cfg.durableCronHandlers, parseList(os.Getenv("WORKER_DURABLE_CRON_HANDLERS")))

	cfg.durableCronSpecs = parseSchedulePresets(os.Getenv("WORKER_DURABLE_CRON_PRESETS"))
	for name, preset := range defaultDurableCronPresets() {
		if _, ok := cfg.durableCronSpecs[name]; !ok {
			cfg.durableCronSpecs[name] = preset
		}
	}

	cfg.cronSpecs = parseSchedulePresets(os.Getenv("WORKER_CRON_PRESETS"))

	cfg.durableCronHandlers = mergeUnique(cfg.durableCronHandlers, presetKeys(cfg.durableCronSpecs))
	cfg.cronHandlers = mergeUnique(cfg.cronHandlers, presetKeys(cfg.cronSpecs))

	return cfg
}

func registerCronFactories(tm *worker.TaskManager, cfg config) error {
	for _, name := range cfg.durableCronHandlers {
		handlerName := name

		spec := cfg.cronSpec
		if override, ok := cfg.durableCronSpecs[handlerName]; ok && override != "" {
			spec = override
		}

		err := tm.RegisterDurableCronTask(context.Background(), handlerName, spec, func(_ context.Context) (worker.DurableTask, error) {
			payload := defaultSendEmailPayload(handlerName)

			return worker.DurableTask{
				ID:      uuid.New(),
				Handler: handlerName,
				Message: payload,
			}, nil
		})
		if err != nil {
			return ewrap.Wrapf(err, "register durable cron handler %s", handlerName)
		}
	}

	for _, name := range cfg.cronHandlers {
		handlerName := name

		spec := cfg.cronSpec
		if override, ok := cfg.cronSpecs[handlerName]; ok && override != "" {
			spec = override
		}

		err := tm.RegisterCronTask(context.Background(), handlerName, spec, func(ctx context.Context) (*worker.Task, error) {
			payload := defaultSendEmailPayload(handlerName)

			task, err := worker.NewTask(ctx, func(execCtx context.Context, _ ...any) (any, error) {
				return sendEmail(execCtx, payload)
			})
			if err != nil {
				return nil, err
			}

			task.Name = handlerName

			return task, nil
		})
		if err != nil {
			return ewrap.Wrapf(err, "register cron handler %s", handlerName)
		}
	}

	return nil
}

func buildHandlerMaps(
	cfg config,
	jobRunner *jobRunner,
) (map[string]worker.HandlerSpec, map[string]worker.DurableHandlerSpec) {
	handlers := map[string]worker.HandlerSpec{
		"send_email": sendEmailHandlerSpec(),
	}
	durableHandlers := map[string]worker.DurableHandlerSpec{
		"send_email": sendEmailDurableHandlerSpec(),
	}

	if jobRunner != nil {
		durableHandlers[worker.JobHandlerName] = jobDurableHandlerSpec(jobRunner)
	}

	addNames := append([]string{}, cfg.cronHandlers...)
	addNames = append(addNames, cfg.durableCronHandlers...)

	for _, name := range addNames {
		if _, ok := handlers[name]; !ok {
			handlers[name] = sendEmailHandlerSpec()
		}

		if _, ok := durableHandlers[name]; !ok {
			durableHandlers[name] = sendEmailDurableHandlerSpec()
		}
	}

	return handlers, durableHandlers
}

func sendEmailHandlerSpec() worker.HandlerSpec {
	return worker.HandlerSpec{
		Make: func() protoreflect.ProtoMessage { return &workerpb.SendEmailPayload{} },
		Fn: func(ctx context.Context, payload protoreflect.ProtoMessage) (any, error) {
			msg, ok := payload.(*workerpb.SendEmailPayload)
			if !ok {
				return nil, ewrap.New("invalid send_email payload")
			}

			return sendEmail(ctx, msg)
		},
	}
}

func sendEmailDurableHandlerSpec() worker.DurableHandlerSpec {
	return worker.DurableHandlerSpec{
		Make: func() proto.Message { return &workerpb.SendEmailPayload{} },
		Fn: func(ctx context.Context, payload proto.Message) (any, error) {
			msg, ok := payload.(*workerpb.SendEmailPayload)
			if !ok {
				return nil, ewrap.New("invalid send_email payload")
			}

			return sendEmail(ctx, msg)
		},
	}
}

func defaultSendEmailPayload(handler string) *workerpb.SendEmailPayload {
	return &workerpb.SendEmailPayload{
		To:      "ops@example.com",
		Subject: "Cron handler " + handler,
		Body:    "Automated schedule trigger",
	}
}

func sendEmail(_ context.Context, payload *workerpb.SendEmailPayload) (any, error) {
	log.Printf("send email to=%s subject=%s", payload.GetTo(), payload.GetSubject())

	return "ok", nil
}

func newRedisClient(cfg config) (rueidis.Client, error) {
	options := rueidis.ClientOption{
		InitAddress: []string{cfg.redisAddr},
		Password:    cfg.redisPassword,
	}

	if cfg.redisTLS {
		tlsCfg := &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
		if cfg.redisTLSInsec {
			tlsCfg.InsecureSkipVerify = true
		}

		options.TLSConfig = tlsCfg
	}

	client, err := rueidis.NewClient(options)
	if err != nil {
		return nil, ewrap.Wrap(err, "new redis client")
	}

	return client, nil
}

func parseBool(raw string) bool {
	raw = strings.TrimSpace(strings.ToLower(raw))

	return raw == "true" || raw == "1" || raw == "yes"
}

func parseFloat(raw string) float64 {
	value, err := strconv.ParseFloat(strings.TrimSpace(raw), parseFloatBitSize)
	if err != nil || value <= 0 {
		return 0
	}

	return value
}

func parseInt(raw string) int {
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || value <= 0 {
		return 0
	}

	return value
}

func parseIntWithDefault(raw string, fallback int) int {
	value := parseInt(raw)
	if value <= 0 {
		return fallback
	}

	return value
}

func defaultDuration(raw string, fallback time.Duration) time.Duration {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback
	}

	value, err := time.ParseDuration(raw)
	if err != nil || value <= 0 {
		return fallback
	}

	return value
}

func getenv(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}

	return value
}

func parseList(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}

	seen := map[string]struct{}{}

	var out []string

	for part := range strings.SplitSeq(raw, ",") {
		name := strings.TrimSpace(part)
		if name == "" {
			continue
		}

		if _, ok := seen[name]; ok {
			continue
		}

		seen[name] = struct{}{}
		out = append(out, name)
	}

	return out
}

func parseSchedulePresets(raw string) map[string]string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return map[string]string{}
	}

	out := map[string]string{}

	for entry := range strings.SplitSeq(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}

		name, spec, ok := strings.Cut(entry, "=")
		name = strings.TrimSpace(name)

		spec = strings.TrimSpace(spec)
		if !ok || name == "" || spec == "" {
			continue
		}

		out[name] = spec
	}

	return out
}

func mergeUnique(base, extra []string) []string {
	if len(extra) == 0 {
		return base
	}

	seen := map[string]struct{}{}

	out := make([]string, 0, len(base)+len(extra))
	for _, name := range base {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}

		if _, ok := seen[name]; ok {
			continue
		}

		seen[name] = struct{}{}
		out = append(out, name)
	}

	for _, name := range extra {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}

		if _, ok := seen[name]; ok {
			continue
		}

		seen[name] = struct{}{}
		out = append(out, name)
	}

	return out
}

func presetKeys(values map[string]string) []string {
	if len(values) == 0 {
		return nil
	}

	out := make([]string, 0, len(values))
	for key := range values {
		out = append(out, key)
	}

	return out
}

func waitForSignal() {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
}
