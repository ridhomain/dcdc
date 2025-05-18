package integration_test

import (
	"context"
	"testing"
	"time"

	natsIO "github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/suite"
	tc "github.com/testcontainers/testcontainers-go"
)

// IntegrationTestSuite is a testify suite for running integration tests
// that require NATS and Redis Docker containers.
type IntegrationTestSuite struct {
	suite.Suite
	ctx    context.Context    // Suite-level context
	cancel context.CancelFunc // To cancel the suite-level context

	natsContainer  tc.Container
	natsURL        string
	redisContainer tc.Container
	redisAddr      string
}

// SetupSuite runs once before all tests in the suite.
// It starts the NATS and Redis containers.
func (s *IntegrationTestSuite) SetupSuite() {
	s.T().Log("Setting up integration test suite...")
	// Give more time for suite setup (container downloads/starts)
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 5*time.Minute)

	var err error
	s.T().Log("Starting NATS container for suite...")
	s.natsContainer, s.natsURL, err = startNATSContainer(s.ctx)
	s.Require().NoError(err, "SetupSuite: Failed to start NATS container")
	s.T().Logf("NATS container started, URL: %s", s.natsURL)

	s.T().Log("Starting Redis container for suite...")
	s.redisContainer, s.redisAddr, err = startRedisContainer(s.ctx)
	s.Require().NoError(err, "SetupSuite: Failed to start Redis container")
	s.T().Logf("Redis container started, Addr: %s", s.redisAddr)

	s.T().Log("Integration test suite setup complete.")
}

// TearDownSuite runs once after all tests in the suite are finished.
// It terminates the NATS and Redis containers.
func (s *IntegrationTestSuite) TearDownSuite() {
	s.T().Log("Tearing down integration test suite...")
	var err error
	if s.natsContainer != nil {
		s.T().Log("Terminating NATS container...")
		err = s.natsContainer.Terminate(s.ctx)
		s.NoError(err, "TearDownSuite: Failed to terminate NATS container")
	}
	if s.redisContainer != nil {
		s.T().Log("Terminating Redis container...")
		err = s.redisContainer.Terminate(s.ctx)
		s.NoError(err, "TearDownSuite: Failed to terminate Redis container")
	}
	if s.cancel != nil {
		s.cancel() // Cancel the suite-level context
	}
	s.T().Log("Integration test suite teardown complete.")
}

// TestNATSConnection verifies that a connection can be established to the NATS container.
func (s *IntegrationTestSuite) TestNATSConnection() {
	s.T().Logf("TestNATSConnection: Attempting to connect to NATS at %s", s.natsURL)
	nc, err := natsIO.Connect(s.natsURL, natsIO.Timeout(10*time.Second), natsIO.RetryOnFailedConnect(true), natsIO.MaxReconnects(3), natsIO.ReconnectWait(1*time.Second))
	s.Require().NoError(err, "TestNATSConnection: Failed to connect to NATS")
	defer nc.Close()

	s.Equal(natsIO.CONNECTED, nc.Status(), "TestNATSConnection: NATS connection status should be CONNECTED")
	s.T().Log("TestNATSConnection: Successfully connected to NATS and verified status.")
}

// TestRedisConnection verifies that a connection can be established to the Redis container.
func (s *IntegrationTestSuite) TestRedisConnection() {
	s.T().Logf("TestRedisConnection: Attempting to connect to Redis at %s", s.redisAddr)
	opts, err := redis.ParseURL("redis://" + s.redisAddr)
	s.Require().NoError(err, "TestRedisConnection: Failed to parse Redis address: %s", s.redisAddr)

	rdb := redis.NewClient(opts)
	defer rdb.Close()

	pingCmd := rdb.Ping(s.ctx) // Use suite context
	s.Require().NoError(pingCmd.Err(), "TestRedisConnection: Redis PING command failed")
	s.Equal("PONG", pingCmd.Val(), "TestRedisConnection: Redis PING should return PONG")
	s.T().Log("TestRedisConnection: Successfully connected to Redis and PING was successful.")
}

// TestRunIntegrationSuite is the entry point for running the integration test suite.
func TestRunIntegrationSuite(t *testing.T) {
	// This is how you run a test suite with testify
	suite.Run(t, new(IntegrationTestSuite))
}
