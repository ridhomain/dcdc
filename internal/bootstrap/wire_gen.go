// Code generated by Wire. DO NOT EDIT.

//go:generate go run -mod=mod github.com/google/wire/cmd/wire
//go:build !wireinject
// +build !wireinject

package bootstrap

import (
	"context"
	"fmt"
	"github.com/google/wire"
	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
	"gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/adapters/config"
	"gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/adapters/logger"
	"gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/adapters/metrics"
	nats2 "gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/adapters/nats"
	redis2 "gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/adapters/redis"
	"gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/application"
	"gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/domain"
	"go.uber.org/zap"
	"net/http"
	"time"
)

// Injectors from container.go:

func InitializeApp() (*App, func(), error) {
	configProvider := provideConfig()
	logger, cleanup, err := provideLogger(configProvider)
	if err != nil {
		return nil, nil, err
	}
	conn, cleanup2, err := provideNATSConnection(configProvider, logger)
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	metricsSink, err := provideMetricsSink()
	if err != nil {
		cleanup2()
		cleanup()
		return nil, nil, err
	}
	server := provideMetricsServer(configProvider, logger)
	workerPool, cleanup3, err := provideWorkerPool(configProvider, logger)
	if err != nil {
		cleanup2()
		cleanup()
		return nil, nil, err
	}
	dedupStore, cleanup4, err := provideDedupStore(configProvider, logger, metricsSink)
	if err != nil {
		cleanup3()
		cleanup2()
		cleanup()
		return nil, nil, err
	}
	publisher, err := providePublisher(configProvider, logger, metricsSink, conn)
	if err != nil {
		cleanup4()
		cleanup3()
		cleanup2()
		cleanup()
		return nil, nil, err
	}
	eventTransformer := provideEventTransformer(logger, metricsSink)
	consumer := provideConsumer(configProvider, logger, dedupStore, publisher, metricsSink, workerPool, eventTransformer)
	jetStreamContext, err := provideJetStreamContext(conn)
	if err != nil {
		cleanup4()
		cleanup3()
		cleanup2()
		cleanup()
		return nil, nil, err
	}
	jetStreamIngester, cleanup5, err := provideIngester(configProvider, logger, consumer, conn, jetStreamContext)
	if err != nil {
		cleanup4()
		cleanup3()
		cleanup2()
		cleanup()
		return nil, nil, err
	}
	app := NewApp(logger, configProvider, conn, metricsSink, server, workerPool, jetStreamIngester, publisher, dedupStore, consumer)
	return app, func() {
		cleanup5()
		cleanup4()
		cleanup3()
		cleanup2()
		cleanup()
	}, nil
}

// container.go:

// ServiceName is the name of this service.
const ServiceName = "daisi-cdc-consumer-service"

func provideConfig() domain.ConfigProvider {
	return config.NewViperConfigProvider()
}

func provideLogger(cfg domain.ConfigProvider) (domain.Logger, func(), error) {
	loggerImpl, err := logger.NewZapAdapter(cfg, ServiceName)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create logger: %w", err)
	}
	cleanup := func() {
		if zapLogger, ok := loggerImpl.(*logger.ZapAdapter); ok {
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
		natsURL, nats.RetryOnFailedConnect(true), nats.MaxReconnects(-1), nats.ReconnectWait(5*time.Second), nats.DisconnectErrHandler(func(conn *nats.Conn, err error) {
			log.Warn(context.Background(), "NATS disconnected", zap.Error(err))
		}), nats.ReconnectHandler(func(conn *nats.Conn) {
			log.Info(context.Background(), "NATS reconnected", zap.String("url", conn.ConnectedUrl()))
		}), nats.ClosedHandler(func(conn *nats.Conn) {
			log.Info(context.Background(), "NATS connection closed")
		}), nats.Name(ServiceName),
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
	sink, err := metrics.NewPrometheusMetricsSink()
	if err != nil {
		return nil, fmt.Errorf("failed to create prometheus metrics sink: %w", err)
	}
	return sink, nil
}

func provideMetricsServer(cfg domain.ConfigProvider, log domain.Logger) *http.Server {
	metricsPort := cfg.GetString(config.KeyMetricsPort)
	log.Info(context.Background(), "Starting metrics server", zap.String("port", metricsPort))
	return metrics.StartMetricsServer(metricsPort)
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

func provideEventTransformer(log domain.Logger, metrics2 domain.MetricsSink) application.EventTransformer {
	return application.NewTransformService(log, metrics2)
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

func provideDedupStore(cfg domain.ConfigProvider, log domain.Logger, metrics2 domain.MetricsSink) (domain.DedupStore, func(), error) {
	store, err := redis2.NewDedupStore(cfg, log, metrics2)
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

func providePublisher(cfg domain.ConfigProvider, log domain.Logger, metrics2 domain.MetricsSink, nc *nats.Conn) (domain.Publisher, error) {
	pub, err := nats2.NewJetStreamPublisher(cfg, log, metrics2, nc)
	if err != nil {
		return nil, fmt.Errorf("failed to create jetstream publisher: %w", err)
	}
	return pub, nil
}

func provideConsumer(
	cfg domain.ConfigProvider,
	log domain.Logger,
	dedup domain.DedupStore,
	pub domain.Publisher, metrics2 domain.MetricsSink,

	pool *application.WorkerPool,
	transformer application.EventTransformer,
) *application.Consumer {
	return application.NewConsumer(cfg, log, dedup, pub, metrics2, pool, transformer)
}

func provideIngester(
	cfg domain.ConfigProvider,
	log domain.Logger,
	appConsumer *application.Consumer,
	nc *nats.Conn,
	jsCtx nats.JetStreamContext,
) (*nats2.JetStreamIngester, func(), error) {
	ingester, err := nats2.NewJetStreamIngester(cfg, log, appConsumer, nc, jsCtx)
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
	Ingester      *nats2.JetStreamIngester
	Publisher     domain.Publisher
	DedupStore    domain.DedupStore
	Consumer      *application.Consumer
}

func NewApp(logger2 domain.Logger,

	cfg domain.ConfigProvider,
	natsConn *nats.Conn,
	metricsSink domain.MetricsSink,
	metricsServer *http.Server,
	workerPool *application.WorkerPool,
	ingester *nats2.JetStreamIngester,
	publisher domain.Publisher,
	dedupStore domain.DedupStore,
	consumer *application.Consumer,
) *App {
	return &App{
		Logger:        logger2,
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
