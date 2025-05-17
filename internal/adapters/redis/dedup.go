package redis

import (
	"context"
	"fmt"
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
	redisClient    *redis.Client
}

// NewDedupStore creates a new Redis-based deduplication store.
func NewDedupStore(
	cfg domain.ConfigProvider,
	log domain.Logger,
) (*DedupStore, error) {
	logger := log.With(zap.String("component", "redis_dedup_store"))

	redisAddr := cfg.GetString(config.KeyRedisAddr)
	if redisAddr == "" {
		logger.Error(context.Background(), "Redis address is not configured", zap.String("config_key", config.KeyRedisAddr))
		return nil, fmt.Errorf("redis address not configured (key: %s)", config.KeyRedisAddr)
	}

	opts, err := redis.ParseURL(redisAddr)
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
		redisClient:    client,
	}, nil
}

// IsDuplicate checks if the given eventID has been seen within the specified TTL.
// It uses Redis SETNX command for atomicity.
// It implements the domain.DedupStore interface.
func (s *DedupStore) IsDuplicate(ctx context.Context, eventID domain.EventID, ttl time.Duration) (bool, error) {
	redisKey := dedupKeyPrefix + string(eventID)
	s.logger.Debug(ctx, "Checking for duplicate event in Redis",
		zap.String("redis_key", redisKey),
		zap.Duration("ttl", ttl),
	)

	// Redis SETNX key value EX seconds
	// Returns true if the key was set (i.e., it's a new event).
	// Returns false if the key was not set (i.e., key already exists, it's a duplicate).
	wasSet, err := s.redisClient.SetNX(ctx, redisKey, "", ttl).Result()
	if err != nil {
		// Distinguish between Redis being down vs. other errors if necessary.
		// For now, any error from SetNX is a failure in the dedup check.
		s.logger.Error(ctx, "Redis SETNX failed", zap.Error(err), zap.String("key", redisKey))
		// This pattern allows errors.Is(returnedErr, domain.ErrExternalService) to work if domain.ErrExternalService is a sentinel.
		// It also preserves the original Redis error for inspection.
		redisSpecificError := fmt.Errorf("redis SETNX for key '%s' failed: %w", redisKey, err)
		return false, domain.NewErrExternalService("Redis_deduplicator", redisSpecificError)
	}

	// If wasSet is true, the key was new, so it's NOT a duplicate.
	// If wasSet is false, the key existed, so it IS a duplicate.
	isDuplicate := !wasSet

	if isDuplicate {
		s.logger.Info(ctx, "Event determined to be a duplicate", zap.String("redis_key", redisKey))
	} else {
		s.logger.Info(ctx, "Event determined to be new", zap.String("redis_key", redisKey))
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
