package redis

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/adapters/config"
	"gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/domain"
	"go.uber.org/zap"
)

const (
	dedupKeyPrefix = "dedup:" // As per PRD: dedup:{event_id}
)

// DedupStore implements the domain.DedupStore interface using Redis.
type DedupStore struct {
	configProvider domain.ConfigProvider
	logger         domain.Logger
	metricsSink    domain.MetricsSink
	redisClient    *redis.Client
}

// NewDedupStore creates a new Redis-based deduplication store.
func NewDedupStore(
	cfg domain.ConfigProvider,
	log domain.Logger,
	metrics domain.MetricsSink,
) (*DedupStore, error) {
	logger := log.With(zap.String("component", "redis_dedup_store"))

	redisAddr := cfg.GetString(config.KeyRedisAddr)
	if redisAddr == "" {
		logger.Error(context.Background(), "Redis address is not configured", zap.String("config_key", config.KeyRedisAddr))
		return nil, fmt.Errorf("redis address not configured (key: %s)", config.KeyRedisAddr)
	}

	rawAddr := strings.TrimSpace(redisAddr)

	if !strings.HasPrefix(rawAddr, "redis://") {
		rawAddr = "redis://" + rawAddr
	}

	opts, err := redis.ParseURL(rawAddr)
	if err != nil {
		logger.Error(context.Background(), "Failed to parse Redis URL", zap.Error(err), zap.String("redis_url", redisAddr))
		return nil, fmt.Errorf("failed to parse Redis URL: %w", err)
	}

	client := redis.NewClient(opts)

	// Ping Redis to ensure connectivity.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) // Short timeout for ping
	defer cancel()

	if status := client.Ping(ctx); status.Err() != nil {
		logger.Error(context.Background(), "Failed to connect to Redis", zap.Error(status.Err()), zap.String("address", redisAddr))
		client.Close() // Close the client if ping fails
		return nil, fmt.Errorf("failed to ping Redis at %s: %w", redisAddr, status.Err())
	}

	logger.Info(context.Background(), "Successfully connected to Redis", zap.String("address", redisAddr))

	return &DedupStore{
		configProvider: cfg,
		logger:         logger,
		metricsSink:    metrics,
		redisClient:    client,
	}, nil
}

// extractTableFromEventID extracts the table name from an EventID string (format: LSN:Table:PK)
func extractTableFromEventID(eventID domain.EventID) string {
	parts := strings.SplitN(string(eventID), ":", 3)
	if len(parts) > 1 {
		return parts[1] // The second part is the table name
	}
	return "unknown_table" // Fallback, though eventID should always be valid
}

// IsDuplicate checks if the given eventID has been seen within the specified TTL.
// It uses Redis SETNX command for atomicity.
// It implements the domain.DedupStore interface.
func (s *DedupStore) IsDuplicate(ctx context.Context, eventID domain.EventID, ttl time.Duration) (bool, error) {
	redisKey := dedupKeyPrefix + string(eventID)
	tableName := extractTableFromEventID(eventID)

	s.logger.Debug(ctx, "Checking for duplicate event in Redis",
		zap.String("redis_key", redisKey),
		zap.Duration("ttl_seconds", ttl), // Log TTL in seconds for clarity
		zap.String("event_id_param", string(eventID)),
	)

	// Redis SETNX key value EX seconds
	// Returns true if the key was set (i.e., it's a new event).
	// Returns false if the key was not set (i.e., key already exists, it's a duplicate).
	s.logger.Info(ctx, "Attempting Redis SETNX operation",
		zap.String("redis_key", redisKey),
		zap.String("value", "seen"), // The value doesn't really matter for SETNX, but good to log
		zap.Float64("ttl_seconds_for_setnx", ttl.Seconds()),
	)
	wasSet, err := s.redisClient.SetNX(ctx, redisKey, "seen", ttl).Result() // Using "seen" as value
	if err != nil {
		// Distinguish between Redis being down vs. other errors if necessary.
		// For now, any error from SetNX is a failure in the dedup check.
		s.logger.Error(ctx, "Redis SETNX failed", zap.Error(err), zap.String("key", redisKey))
		// This pattern allows errors.Is(returnedErr, domain.ErrExternalService) to work if domain.ErrExternalService is a sentinel.
		// It also preserves the original Redis error for inspection.
		redisSpecificError := fmt.Errorf("redis SETNX for key '%s' failed: %w", redisKey, err)
		return false, domain.NewErrExternalService("Redis_deduplicator", redisSpecificError)
	}

	s.logger.Info(ctx, "Redis SETNX result",
		zap.String("redis_key", redisKey),
		zap.Bool("was_set_by_this_call (key_was_new)", wasSet),
	)

	// If wasSet is true, the key was new, so it's NOT a duplicate.
	// If wasSet is false, the key existed, so it IS a duplicate.
	isDuplicate := !wasSet

	if isDuplicate {
		s.logger.Info(ctx, "Event determined to be a duplicate", zap.String("redis_key", redisKey), zap.String("table_name", tableName))
		s.metricsSink.IncDedupCheck(tableName, "hit")
	} else {
		s.logger.Info(ctx, "Event determined to be new", zap.String("redis_key", redisKey), zap.String("table_name", tableName))
		s.metricsSink.IncDedupCheck(tableName, "miss")
	}
	return isDuplicate, nil
}

// Shutdown gracefully closes any Redis resources.
func (s *DedupStore) Shutdown() error {
	s.logger.Info(context.Background(), "Redis DedupStore shutting down...")
	if s.redisClient != nil {
		if err := s.redisClient.Close(); err != nil {
			s.logger.Error(context.Background(), "Failed to close Redis client", zap.Error(err))
			return fmt.Errorf("failed to close Redis client: %w", err)
		}
	}
	s.logger.Info(context.Background(), "Redis DedupStore shutdown complete.")
	return nil
}
