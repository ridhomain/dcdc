package metrics

import (
	"fmt"
	"net/http" // For converting bool to string for label
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/domain"
)

// prometheusMetricsSink implements the domain.MetricsSink interface using Prometheus.
type prometheusMetricsSink struct {
	cdcConsumerEventsTotal          *prometheus.CounterVec
	cdcConsumerPublishErrorsTotal   prometheus.Counter
	cdcConsumerEventsPublishedTotal *prometheus.CounterVec
	cdcConsumerRedisHitTotal        *prometheus.CounterVec
	cdcConsumerProcessingSeconds    *prometheus.HistogramVec
	cdcConsumerLagSeconds           prometheus.Gauge
	cdcConsumerUnhandledFieldsTotal *prometheus.CounterVec
	cdcConsumerDedupChecksTotal     *prometheus.CounterVec
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
		cdcConsumerEventsPublishedTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "cdc_consumer_events_published_total",
				Help: "Total number of events published, labeled by subject and status.",
			},
			[]string{"subject", "status"}, // status: success, failure
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
		cdcConsumerDedupChecksTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "daisi_cdc_consumer_deduplication_checks_total",
				Help: "Total number of deduplication checks performed, partitioned by table and result (hit/miss).",
			},
			[]string{"table", "result"},
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

// IncEventsPublished increments the counter for published events.
func (s *prometheusMetricsSink) IncEventsPublished(subject string, status string) {
	s.cdcConsumerEventsPublishedTotal.WithLabelValues(subject, status).Inc()
}

// IncDedupCheck increments the counter for deduplication checks.
func (s *prometheusMetricsSink) IncDedupCheck(table string, result string) {
	s.cdcConsumerDedupChecksTotal.WithLabelValues(table, result).Inc()
}

// StartMetricsServer starts an HTTP server to expose Prometheus metrics.
// This is a helper function and might be called from bootstrap or main.
// It takes a ConfigProvider to get the port.
func StartMetricsServer(metricsPort string) *http.Server {
	// It's conventional for the metrics path to be /metrics
	hmux := http.NewServeMux()
	hmux.Handle("/metrics", promhttp.Handler())
	hmux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		// Consider a more structured health response if needed in the future
		_, _ = w.Write([]byte(`{"status": "ok"}`))
	})

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
