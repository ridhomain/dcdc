//go:build wireinject
// +build wireinject

package bootstrap

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/google/wire"
	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
	"gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/adapters/config"
	adapterlogger "gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/adapters/logger"
	adaptermetrics "gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/adapters/metrics"
	adapternats "gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/adapters/nats"
	adapterredis "gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/adapters/redis"
	"gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/application"
	"gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/domain"
	"go.uber.org/zap"
)

// ServiceName is the name of this service.
const ServiceName = "daisi-cdc-consumer-service"

// --- Core Infrastructure Providers ---

func provideConfig() domain.ConfigProvider {
	return config.NewViperConfigProvider()
}

func provideLogger(cfg domain.ConfigProvider) (domain.Logger, func(), error) {
	loggerImpl, err := adapterlogger.NewZapAdapter(cfg, ServiceName)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create logger: %w", err)
	}
	cleanup := func() {
		if zapLogger, ok := loggerImpl.(*adapterlogger.ZapAdapter); ok {
			_ = zapLogger.Sync()
		} else {
			fmt.Println("Warning: Logger type assertion for sync failed.")
		}
	}
	return loggerImpl, cleanup, nil
}

func provideNATSConnection(cfg domain.ConfigProvider, log domain.Logger) (*nats.Conn, func(), error) {
	natsURL := cfg.GetString(config.KeyNatsURL)
	if natsURL == "" {
		return nil, nil, fmt.Errorf("NATS_URL is not configured (key: %s)", config.KeyNatsURL)
	}

	nc, err := nats.Connect(
		natsURL,
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(5*time.Second),
		nats.DisconnectErrHandler(func(conn *nats.Conn, err error) {
			log.Warn(context.Background(), "NATS disconnected", zap.Error(err))
		}),
		nats.ReconnectHandler(func(conn *nats.Conn) {
			log.Info(context.Background(), "NATS reconnected", zap.String("url", conn.ConnectedUrl()))
		}),
		nats.ClosedHandler(func(conn *nats.Conn) {
			log.Info(context.Background(), "NATS connection closed")
		}),
		nats.Name(ServiceName),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to NATS at %s: %w", natsURL, err)
	}

	cleanup := func() {
		if nc != nil && !nc.IsClosed() {
			log.Info(context.Background(), "Draining NATS connection...")
			if err := nc.Drain(); err != nil {
				log.Error(context.Background(), "Error draining NATS connection", zap.Error(err))
			} else {
				log.Info(context.Background(), "NATS connection drained successfully.")
			}
		}
	}
	return nc, cleanup, nil
}

func provideJetStreamContext(nc *nats.Conn) (nats.JetStreamContext, error) {
	js, err := nc.JetStream()
	if err != nil {
		return nil, fmt.Errorf("failed to get JetStream context: %w", err)
	}
	return js, nil
}

func provideRedisClient(cfg domain.ConfigProvider, log domain.Logger) (*redis.Client, func(), error) {
	redisAddr := cfg.GetString(config.KeyRedisAddr)
	if redisAddr == "" {
		return nil, nil, fmt.Errorf("Redis address is not configured (key: %s)", config.KeyRedisAddr)
	}

	opts, err := redis.ParseURL(redisAddr)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse Redis URL '%s': %w", redisAddr, err)
	}

	client := redis.NewClient(opts)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if status := client.Ping(ctx); status.Err() != nil {
		_ = client.Close()
		return nil, nil, fmt.Errorf("failed to ping Redis at %s: %w", redisAddr, status.Err())
	}
	log.Info(context.Background(), "Successfully connected to Redis (shared client)", zap.String("address", redisAddr))

	cleanup := func() {
		if client != nil {
			if err := client.Close(); err != nil {
				log.Error(context.Background(), "Failed to close shared Redis client", zap.Error(err))
			} else {
				log.Info(context.Background(), "Shared Redis client closed successfully.")
			}
		}
	}
	return client, cleanup, nil
}

func provideMetricsSink() (domain.MetricsSink, error) {
	sink, err := adaptermetrics.NewPrometheusMetricsSink()
	if err != nil {
		return nil, fmt.Errorf("failed to create prometheus metrics sink: %w", err)
	}
	return sink, nil
}

func provideMetricsServer(cfg domain.ConfigProvider, log domain.Logger) *http.Server {
	metricsPort := cfg.GetString(config.KeyMetricsPort)
	log.Info(context.Background(), "Starting metrics server", zap.String("port", metricsPort))
	return adaptermetrics.StartMetricsServer(metricsPort)
}

var CoreInfraSet = wire.NewSet(
	provideConfig,
	provideLogger,
	provideNATSConnection,
	provideJetStreamContext,
	provideRedisClient,
	provideMetricsSink,
	provideMetricsServer,
)

// --- Application Service and Adapter Providers ---

func provideEventTransformer(log domain.Logger, metrics domain.MetricsSink) application.EventTransformer {
	return application.NewTransformService(log, metrics)
}

func provideWorkerPool(cfg domain.ConfigProvider, log domain.Logger) (*application.WorkerPool, func(), error) {
	pool, err := application.NewWorkerPool(cfg, log)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create worker pool: %w", err)
	}
	cleanup := func() {
		pool.Release()
	}
	return pool, cleanup, nil
}

func provideDedupStore(cfg domain.ConfigProvider, log domain.Logger, metrics domain.MetricsSink) (domain.DedupStore, func(), error) {
	store, err := adapterredis.NewDedupStore(cfg, log, metrics)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create dedup store: %w", err)
	}
	cleanup := func() {
		if err := store.Shutdown(); err != nil {
			log.Error(context.Background(), "Failed to shutdown dedup store", zap.Error(err))
		}
	}
	return store, cleanup, nil
}

func providePublisher(cfg domain.ConfigProvider, log domain.Logger, metrics domain.MetricsSink, nc *nats.Conn) (domain.Publisher, error) {
	pub, err := adapternats.NewJetStreamPublisher(cfg, log, metrics, nc)
	if err != nil {
		return nil, fmt.Errorf("failed to create jetstream publisher: %w", err)
	}
	return pub, nil
}

func provideConsumer(
	cfg domain.ConfigProvider,
	log domain.Logger,
	dedup domain.DedupStore,
	pub domain.Publisher,
	metrics domain.MetricsSink,
	pool *application.WorkerPool,
	transformer application.EventTransformer,
) *application.Consumer {
	return application.NewConsumer(cfg, log, dedup, pub, metrics, pool, transformer)
}

func provideIngester(
	cfg domain.ConfigProvider,
	log domain.Logger,
	appConsumer *application.Consumer,
	nc *nats.Conn,
	jsCtx nats.JetStreamContext,
) (*adapternats.JetStreamIngester, func(), error) {
	ingester, err := adapternats.NewJetStreamIngester(cfg, log, appConsumer, nc, jsCtx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create jetstream ingester: %w", err)
	}
	cleanup := func() {
		if err := ingester.Shutdown(); err != nil {
			log.Error(context.Background(), "Error shutting down jetstream ingester", zap.Error(err))
		}
	}
	return ingester, cleanup, nil
}

var ApplicationServicesSet = wire.NewSet(
	provideEventTransformer,
	provideWorkerPool,
	provideDedupStore,
	providePublisher,
	provideConsumer,
	provideIngester,
)

type App struct {
	Logger        domain.Logger
	Cfg           domain.ConfigProvider
	NatsConn      *nats.Conn
	MetricsSink   domain.MetricsSink
	MetricsServer *http.Server
	WorkerPool    *application.WorkerPool
	Ingester      *adapternats.JetStreamIngester
	Publisher     domain.Publisher
	DedupStore    domain.DedupStore
	Consumer      *application.Consumer
}

func NewApp(
	logger domain.Logger,
	cfg domain.ConfigProvider,
	natsConn *nats.Conn,
	metricsSink domain.MetricsSink,
	metricsServer *http.Server,
	workerPool *application.WorkerPool,
	ingester *adapternats.JetStreamIngester,
	publisher domain.Publisher,
	dedupStore domain.DedupStore,
	consumer *application.Consumer,
) *App {
	return &App{
		Logger:        logger,
		Cfg:           cfg,
		NatsConn:      natsConn,
		MetricsSink:   metricsSink,
		MetricsServer: metricsServer,
		WorkerPool:    workerPool,
		Ingester:      ingester,
		Publisher:     publisher,
		DedupStore:    dedupStore,
		Consumer:      consumer,
	}
}

var FullAppSet = wire.NewSet(
	CoreInfraSet,
	ApplicationServicesSet,
	NewApp,
)

func InitializeApp() (*App, func(), error) {
	wire.Build(
		FullAppSet,
	)
	return nil, nil, nil
}
