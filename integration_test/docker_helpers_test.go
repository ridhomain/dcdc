package integration_test

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// startNATSContainer sets up and starts a NATS container with JetStream enabled.
// It returns the container instance, the NATS connection URL, and an error if any occurred.
func startNATSContainer(ctx context.Context) (tc.Container, string, error) {
	req := tc.ContainerRequest{
		Image:        "nats:2.10.14-alpine",
		ExposedPorts: []string{"4222/tcp", "6222/tcp", "8222/tcp"}, // NATS, NATS Route, NATS Monitoring
		Cmd:          []string{"-js"},                              // Enable JetStream
		WaitingFor: wait.ForLog("Server is ready").
			WithStartupTimeout(1 * time.Minute).
			WithPollInterval(200 * time.Millisecond),
	}
	natsContainer, err := tc.GenericContainer(ctx, tc.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, "", fmt.Errorf("failed to start NATS container: %w", err)
	}

	// Get mapped port for NATS client connections
	natsPort, err := natsContainer.MappedPort(ctx, "4222/tcp")
	if err != nil {
		// Attempt to terminate if we can't get the port
		_ = natsContainer.Terminate(ctx)
		return nil, "", fmt.Errorf("failed to get NATS mapped port 4222: %w", err)
	}

	natsHost, err := natsContainer.Host(ctx)
	if err != nil {
		_ = natsContainer.Terminate(ctx)
		return nil, "", fmt.Errorf("failed to get NATS container host: %w", err)
	}

	natsURL := fmt.Sprintf("nats://%s:%s", natsHost, natsPort.Port())
	return natsContainer, natsURL, nil
}

// startRedisContainer sets up and starts a Redis container.
// It returns the container instance, the Redis connection address, and an error if any occurred.
func startRedisContainer(ctx context.Context) (tc.Container, string, error) {
	req := tc.ContainerRequest{
		Image:        "redis:7.2-alpine",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor: wait.ForLog("Ready to accept connections").
			WithStartupTimeout(1 * time.Minute).
			WithPollInterval(200 * time.Millisecond),
	}
	redisContainer, err := tc.GenericContainer(ctx, tc.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, "", fmt.Errorf("failed to start Redis container: %w", err)
	}

	// Get mapped port for Redis
	redisPort, err := redisContainer.MappedPort(ctx, "6379/tcp")
	if err != nil {
		_ = redisContainer.Terminate(ctx)
		return nil, "", fmt.Errorf("failed to get Redis mapped port 6379: %w", err)
	}

	redisHost, err := redisContainer.Host(ctx)
	if err != nil {
		_ = redisContainer.Terminate(ctx)
		return nil, "", fmt.Errorf("failed to get Redis container host: %w", err)
	}

	redisAddr := fmt.Sprintf("%s:%s", redisHost, redisPort.Port())

	// Optional: Perform a quick PING to ensure it's truly ready, Testcontainers wait strategy should cover this.
	// However, adding an explicit check can sometimes catch issues earlier.
	opts, err := redis.ParseURL(fmt.Sprintf("redis://%s", redisAddr))
	if err != nil {
		_ = redisContainer.Terminate(ctx)
		return nil, "", fmt.Errorf("failed to parse redis addr for ping: %w", err)
	}

	rdb := redis.NewClient(opts)
	defer rdb.Close()

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	status := rdb.Ping(pingCtx)
	if status.Err() != nil {
		_ = redisContainer.Terminate(ctx)
		return nil, "", fmt.Errorf("failed to ping Redis container at %s: %w", redisAddr, status.Err())
	}

	return redisContainer, redisAddr, nil
}
