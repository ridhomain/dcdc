package domain

import (
	"context"
	"time"

	"go.uber.org/zap" // Assuming Zap will be used for the Logger interface as per task details
)

// ConfigProvider defines the interface for accessing application configuration.
// This allows different configuration sources (e.g., Viper, environment variables)
// to be used interchangeably.
type ConfigProvider interface {
	GetString(key string) string
	GetDuration(key string) time.Duration
	GetInt(key string) int
	GetBool(key string) bool // Added for completeness, often needed
	Set(key string, value interface{})
}

// Logger defines the interface for structured logging throughout the application.
// This promotes consistent logging and allows easy swapping of logging libraries.
// Logging methods now expect a context.Context for context-aware logging (e.g., event_id).
type Logger interface {
	Info(ctx context.Context, msg string, fields ...zap.Field)
	Warn(ctx context.Context, msg string, fields ...zap.Field)
	Error(ctx context.Context, msg string, fields ...zap.Field)
	Debug(ctx context.Context, msg string, fields ...zap.Field)
	With(fields ...zap.Field) Logger // For adding static, non-contextual fields to a logger instance
}

// DedupStore defines the interface for checking if an event has already been processed.
// This is crucial for ensuring exactly-once semantics.
type DedupStore interface {
	// IsDuplicate checks if the given eventID has been seen within the specified TTL.
	// It should return true if it's a duplicate, false otherwise.
	// An error is returned if the check fails.
	IsDuplicate(ctx context.Context, eventID EventID, ttl time.Duration) (bool, error)
}

// Publisher defines the interface for sending processed events to the downstream
// message broker (e.g., NATS JetStream).
type Publisher interface {
	// Publish sends the given data to the specified subject.
	// An error is returned if publishing fails.
	Publish(ctx context.Context, subject string, data []byte) error
}

// MetricsSink defines the interface for emitting application metrics.
// This allows for different monitoring backends (e.g., Prometheus).
type MetricsSink interface {
	IncEventsTotal(table, result string) // result: "processed", "skipped", "duplicate"
	ObserveProcessingDuration(table string, duration time.Duration)
	IncPublishErrors()
	IncRedisHit(hit bool) // true for hit, false for miss
	SetConsumerLag(lag float64)
	IncUnhandledFieldsTotal(table, fieldName string)  // New metric for unhandled CDC fields
	IncEventsPublished(subject string, status string) // New metric for published events
	IncDedupCheck(table string, result string)        // New metric for deduplication checks (hit/miss)
}
