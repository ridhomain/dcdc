package metrics

import (
	"context"
	"fmt"
	"net/http" // For converting bool to string for label
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/domain"
	"go.uber.org/zap"
)

// prometheusMetricsSink implements the domain.MetricsSink interface using Prometheus.
type prometheusMetricsSink struct {
	cdcConsumerEventsTotal          *prometheus.CounterVec
	cdcConsumerPublishErrorsTotal   prometheus.Counter
	cdcConsumerRedisHitTotal        *prometheus.CounterVec
	cdcConsumerProcessingSeconds    *prometheus.HistogramVec
	cdcConsumerLagSeconds           prometheus.Gauge
	cdcConsumerUnhandledFieldsTotal *prometheus.CounterVec
	// We might need a configProvider if port/path is configurable here, or handle that in bootstrap
}

// NewPrometheusMetricsSink creates a new Prometheus-backed MetricsSink and registers the collectors.
func NewPrometheusMetricsSink() (domain.MetricsSink, error) {
	sink := &prometheusMetricsSink{
		cdcConsumerEventsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "cdc_consumer_events_total",
				Help: "Total number of CDC events processed, labeled by table and result.",
			},
			[]string{"table", "result"}, // result: processed, skipped, duplicate, error types
		),
		cdcConsumerPublishErrorsTotal: promauto.NewCounter(
			prometheus.CounterOpts{
				Name: "cdc_consumer_publish_errors_total",
				Help: "Total number of errors encountered while publishing events.",
			},
		),
		cdcConsumerRedisHitTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "cdc_consumer_redis_hit_total",
				Help: "Total number of Redis deduplication cache hits and misses.",
			},
			[]string{"status"}, // status: "hit", "miss"
		),
		cdcConsumerProcessingSeconds: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "cdc_consumer_processing_seconds",
				Help:    "Histogram of event processing times, labeled by table.",
				Buckets: prometheus.DefBuckets, // Default buckets, can be customized
			},
			[]string{"table"},
		),
		cdcConsumerLagSeconds: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: "cdc_consumer_lag_seconds",
				Help: "Current consumer lag in seconds.",
			},
		),
		cdcConsumerUnhandledFieldsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "cdc_consumer_unhandled_fields_total",
				Help: "Total number of unhandled fields detected in CDC records, labeled by table and field name.",
			},
			[]string{"table", "field_name"},
		),
	}
	return sink, nil
}

// IncEventsTotal increments the counter for processed events.
func (s *prometheusMetricsSink) IncEventsTotal(table, result string) {
	s.cdcConsumerEventsTotal.WithLabelValues(table, result).Inc()
}

// ObserveProcessingDuration records the duration of event processing.
func (s *prometheusMetricsSink) ObserveProcessingDuration(table string, duration time.Duration) {
	s.cdcConsumerProcessingSeconds.WithLabelValues(table).Observe(duration.Seconds())
}

// IncPublishErrors increments the counter for publishing errors.
func (s *prometheusMetricsSink) IncPublishErrors() {
	s.cdcConsumerPublishErrorsTotal.Inc()
}

// IncRedisHit increments the counter for Redis cache hits or misses.
func (s *prometheusMetricsSink) IncRedisHit(hit bool) {
	status := "miss"
	if hit {
		status = "hit"
	}
	s.cdcConsumerRedisHitTotal.WithLabelValues(status).Inc()
}

// SetConsumerLag sets the current consumer lag gauge.
func (s *prometheusMetricsSink) SetConsumerLag(lag float64) {
	s.cdcConsumerLagSeconds.Set(lag)
}

// IncUnhandledFieldsTotal increments the counter for unhandled CDC fields.
func (s *prometheusMetricsSink) IncUnhandledFieldsTotal(table, fieldName string) {
	s.cdcConsumerUnhandledFieldsTotal.WithLabelValues(table, fieldName).Inc()
}

// StartMetricsServer starts an HTTP server to expose Prometheus metrics.
// This is a helper function and might be called from bootstrap or main.
// It takes a ConfigProvider to get the port.
func StartMetricsServer(metricsPort string) *http.Server {
	// It's conventional for the metrics path to be /metrics
	hmux := http.NewServeMux()
	hmux.Handle("/metrics", promhttp.Handler())

	srv := &http.Server{
		Addr:    ":" + metricsPort, // e.g. ":8080"
		Handler: hmux,
	}

	// Start server in a goroutine so it doesn't block.
	go func() {
		// TODO: Add logging here once logger is available/injectable
		// log.Printf("Metrics server starting on port %s", metricsPort)
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			// log.Fatalf("Metrics server ListenAndServe error: %v", err)
			// For now, print to stderr or panic, as logger isn't injected here.
			// This part needs robust error handling in a real app (e.g. via an error channel)
			panic(fmt.Sprintf("Metrics server failed: %v", err))
		}
	}()
	return srv // Return server instance for graceful shutdown
}

// PrometheusSink implements the domain.MetricsSink interface using Prometheus.
// It also manages an HTTP server to expose the /metrics endpoint.
type PrometheusSink struct {
	configProvider domain.ConfigProvider
	logger         domain.Logger
	httpServer     *http.Server

	// Placeholder for actual Prometheus collectors
	// eventsTotal         *prometheus.CounterVec
	// processingSeconds   *prometheus.HistogramVec
	// publishErrorsTotal  prometheus.Counter
	// redisHitsTotal      *prometheus.CounterVec
	// consumerLag         prometheus.Gauge
}

// NewPrometheusSink creates a new Prometheus metrics sink.
// It initializes Prometheus collectors and sets up an HTTP server for the /metrics endpoint.
func NewPrometheusSink(
	cfg domain.ConfigProvider,
	log domain.Logger,
) (*PrometheusSink, error) {
	logger := log.With(zap.String("component", "prometheus_sink"))

	// Placeholder: Initialize actual Prometheus collectors here and register them.
	// Example:
	// ps.eventsTotal = prometheus.NewCounterVec(...)
	// prometheus.MustRegister(ps.eventsTotal)

	metricsPort := cfg.GetString("metrics_port") // Use constant from config package
	if metricsPort == "" {
		metricsPort = "8080" // Default if not set
		logger.Warn(context.Background(), "Metrics port not configured, defaulting", zap.String("default_port", metricsPort))
	}

	mux := http.NewServeMux()
	// Placeholder: Use promhttp.Handler() once collectors are registered.
	// mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		// Simple placeholder handler
		_, _ = w.Write([]byte("# Placeholder /metrics endpoint. Real Prometheus metrics go here.\n"))
		_, _ = w.Write([]byte("cdc_consumer_stub_metric 1\n"))
	})

	server := &http.Server{
		Addr:    ":" + metricsPort,
		Handler: mux,
		// ReadTimeout:  5 * time.Second, // Example timeouts
		// WriteTimeout: 10 * time.Second,
		// IdleTimeout:  120 * time.Second,
	}

	logger.Info(context.Background(), "Prometheus Sink created (stub implementation)", zap.String("metrics_port", metricsPort))

	return &PrometheusSink{
		configProvider: cfg,
		logger:         logger,
		httpServer:     server,
	}, nil
}

// StartHttpServer starts the HTTP server to expose the /metrics endpoint.
// This should be run in a goroutine.
func (ps *PrometheusSink) StartHttpServer() {
	ps.logger.Info(context.Background(), "Starting Prometheus metrics HTTP server", zap.String("address", ps.httpServer.Addr))
	if err := ps.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		ps.logger.Error(context.Background(), "Prometheus metrics HTTP server failed", zap.Error(err))
	}
}

// ShutdownHttpServer gracefully shuts down the /metrics HTTP server.
func (ps *PrometheusSink) ShutdownHttpServer(ctx context.Context) error {
	ps.logger.Info(ctx, "Shutting down Prometheus metrics HTTP server...")
	if err := ps.httpServer.Shutdown(ctx); err != nil {
		ps.logger.Error(ctx, "Prometheus metrics HTTP server shutdown failed", zap.Error(err))
		return fmt.Errorf("metrics server shutdown failed: %w", err)
	}
	ps.logger.Info(ctx, "Prometheus metrics HTTP server shutdown complete.")
	return nil
}

// --- Implementation of domain.MetricsSink interface (stubs) ---

func (ps *PrometheusSink) IncEventsTotal(table, result string) {
	// Placeholder: ps.eventsTotal.WithLabelValues(table, result).Inc()
	ps.logger.Debug(context.Background(), "Metric: IncEventsTotal (stub)", zap.String("table", table), zap.String("result", result))
}

func (ps *PrometheusSink) ObserveProcessingDuration(table string, duration time.Duration) {
	// Placeholder: ps.processingSeconds.WithLabelValues(table).Observe(duration.Seconds())
	ps.logger.Debug(context.Background(), "Metric: ObserveProcessingDuration (stub)", zap.String("table", table), zap.Duration("duration", duration))
}

func (ps *PrometheusSink) IncPublishErrors() {
	// Placeholder: ps.publishErrorsTotal.Inc()
	ps.logger.Debug(context.Background(), "Metric: IncPublishErrors (stub)")
}

func (ps *PrometheusSink) IncRedisHit(hit bool) {
	hitOrMiss := "miss"
	if hit {
		hitOrMiss = "hit"
	}
	// Placeholder: ps.redisHitsTotal.WithLabelValues(hitOrMiss).Inc()
	ps.logger.Debug(context.Background(), "Metric: IncRedisHit (stub)", zap.String("type", hitOrMiss))
}

func (ps *PrometheusSink) SetConsumerLag(lag float64) {
	// Placeholder: ps.consumerLag.Set(lag)
	ps.logger.Debug(context.Background(), "Metric: SetConsumerLag (stub)", zap.Float64("lag", lag))
}
