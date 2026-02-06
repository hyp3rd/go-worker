package main

import (
	"context"
	"crypto/tls"
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/hyp3rd/ewrap"
	"github.com/redis/rueidis"
	"google.golang.org/grpc"

	worker "github.com/hyp3rd/go-worker"
	workerpb "github.com/hyp3rd/go-worker/pkg/worker/v1"
)

const (
	defaultRedisAddr     = "redis:6379"
	defaultRedisPrefix   = "go-worker"
	defaultGRPCAddr      = "0.0.0.0:50052"
	defaultHTTPAddr      = "0.0.0.0:8081"
	defaultBatchSize     = 50
	defaultLeaseDuration = 30 * time.Second
	adminShutdownTimeout = 5 * time.Second
	parseFloatBitSize    = 64
)

type config struct {
	redisAddr     string
	redisPassword string
	redisPrefix   string
	redisTLS      bool
	redisTLSInsec bool

	grpcAddr string
	httpAddr string

	tlsCert string
	tlsKey  string
	tlsCA   string

	globalRate  float64
	globalBurst int
	leaderLease time.Duration
}

func main() {
	err := run()
	if err != nil {
		log.Fatal(err)
	}
}

func run() error {
	cfg, err := loadConfig()
	if err != nil {
		return ewrap.Wrap(err, "config")
	}

	client, err := newRedisClient(cfg)
	if err != nil {
		return ewrap.Wrap(err, "redis client")
	}
	defer client.Close()

	backend, err := worker.NewRedisDurableBackend(
		client,
		worker.WithRedisDurablePrefix(cfg.redisPrefix),
		worker.WithRedisDurableBatchSize(defaultBatchSize),
		worker.WithRedisDurableLeaderLock(cfg.leaderLease),
		worker.WithRedisDurableGlobalRateLimit(cfg.globalRate, cfg.globalBurst),
	)
	if err != nil {
		return ewrap.Wrap(err, "durable backend")
	}

	tm := worker.NewTaskManagerWithOptions(
		context.Background(),
		worker.WithDurableBackend(backend),
		worker.WithDurableLease(defaultLeaseDuration),
	)

	grpcServer, err := startAdminGRPCServer(cfg, tm)
	if err != nil {
		return err
	}

	gatewayServer, err := startAdminGateway(cfg)
	if err != nil {
		return err
	}

	waitForSignal()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), adminShutdownTimeout)
	defer cancel()

	grpcServer.GracefulStop()

	err = gatewayServer.Shutdown(shutdownCtx)
	if err != nil {
		return ewrap.Wrap(err, "gateway shutdown")
	}

	return nil
}

func startAdminGRPCServer(cfg config, tm *worker.TaskManager) (*grpc.Server, error) {
	grpcServer := grpc.NewServer()
	adminServer := worker.NewGRPCServer(tm, nil)
	workerpb.RegisterAdminServiceServer(grpcServer, adminServer)

	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", cfg.grpcAddr)
	if err != nil {
		return nil, ewrap.Wrap(err, "grpc listen")
	}

	go func() {
		log.Printf("admin gRPC listening on %s", cfg.grpcAddr)

		err := grpcServer.Serve(listener)
		if err != nil {
			log.Printf("grpc serve: %v", err)
		}
	}()

	return grpcServer, nil
}

func startAdminGateway(cfg config) (*http.Server, error) {
	gatewayServer, err := worker.NewAdminGatewayServer(worker.AdminGatewayConfig{
		GRPCAddr:    cfg.grpcAddr,
		HTTPAddr:    cfg.httpAddr,
		TLSCertFile: cfg.tlsCert,
		TLSKeyFile:  cfg.tlsKey,
		TLSCAFile:   cfg.tlsCA,
	})
	if err != nil {
		return nil, ewrap.Wrap(err, "admin gateway")
	}

	go func() {
		log.Printf("admin gateway listening on https://%s", cfg.httpAddr)

		err := gatewayServer.ListenAndServeTLS("", "")
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("gateway serve: %v", err)
		}
	}()

	return gatewayServer, nil
}

func loadConfig() (config, error) {
	cfg := config{
		redisAddr:     getenv("WORKER_REDIS_ADDR", defaultRedisAddr),
		redisPassword: os.Getenv("WORKER_REDIS_PASSWORD"),
		redisPrefix:   getenv("WORKER_REDIS_PREFIX", defaultRedisPrefix),
		redisTLS:      parseBool(os.Getenv("WORKER_REDIS_TLS")),
		redisTLSInsec: parseBool(os.Getenv("WORKER_REDIS_TLS_INSECURE")),
		grpcAddr:      getenv("WORKER_ADMIN_GRPC_ADDR", defaultGRPCAddr),
		httpAddr:      getenv("WORKER_ADMIN_HTTP_ADDR", defaultHTTPAddr),
		tlsCert:       os.Getenv("WORKER_ADMIN_TLS_CERT"),
		tlsKey:        os.Getenv("WORKER_ADMIN_TLS_KEY"),
		tlsCA:         os.Getenv("WORKER_ADMIN_TLS_CA"),
	}

	cfg.globalRate = parseFloat(os.Getenv("WORKER_ADMIN_GLOBAL_RATE"))
	cfg.globalBurst = parseInt(os.Getenv("WORKER_ADMIN_GLOBAL_BURST"))
	cfg.leaderLease = parseDuration(os.Getenv("WORKER_ADMIN_LEADER_LEASE"))

	if cfg.tlsCert == "" || cfg.tlsKey == "" || cfg.tlsCA == "" {
		return cfg, ewrap.New("WORKER_ADMIN_TLS_CERT/KEY/CA are required")
	}

	return cfg, nil
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

func parseDuration(raw string) time.Duration {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0
	}

	value, err := time.ParseDuration(raw)
	if err != nil || value <= 0 {
		return 0
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

func waitForSignal() {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
}
