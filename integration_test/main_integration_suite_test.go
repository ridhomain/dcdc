package integration_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	natsIO "github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/suite"
	tc "github.com/testcontainers/testcontainers-go"
	"go.uber.org/zap"

	"gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/adapters/config" // For config keys
	"gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/adapters/nats"   // Corrected import
	"gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/application"
	"gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/bootstrap"
	"gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/domain"
)

// IntegrationTestSuite is a testify suite for running integration tests
// that require NATS and Redis Docker containers, and an instance of the application.

type IntegrationTestSuite struct {
	suite.Suite
	ctx    context.Context    // Suite-level context for container setup/teardown
	cancel context.CancelFunc // To cancel the suite-level context

	natsContainer  tc.Container
	natsURL        string
	redisContainer tc.Container
	redisAddr      string

	// Application components
	appCtx         context.Context // Context for the application instance itself
	appCancel      context.CancelFunc
	appContainer   *bootstrap.App
	appCleanup     func()
	appConsumer    *application.Consumer
	appIngester    *nats.JetStreamIngester
	appPublisher   domain.Publisher
	appDedupStore  domain.DedupStore
	appLogger      domain.Logger
	appMetricsPort string

	// For ENV var restoration
	originalEnvValues map[string]string
}

// SetupSuite runs once before all tests in the suite.
func (s *IntegrationTestSuite) SetupSuite() {
	s.T().Log("Setting up integration test suite...")
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 5*time.Minute)
	s.originalEnvValues = make(map[string]string)

	var err error
	s.T().Log("Starting NATS container for suite...")
	s.natsContainer, s.natsURL, err = startNATSContainer(s.ctx)
	s.Require().NoError(err, "SetupSuite: Failed to start NATS container")
	s.T().Logf("NATS container started, URL: %s", s.natsURL)

	s.T().Log("Starting Redis container for suite...")
	s.redisContainer, s.redisAddr, err = startRedisContainer(s.ctx)
	s.Require().NoError(err, "SetupSuite: Failed to start Redis container")
	s.T().Logf("Redis container started, Addr: %s", s.redisAddr)

	// --- Initialize Application Under Test ---
	s.T().Log("Initializing application under test by setting ENV vars and calling InitializeApp()...")
	s.appCtx, s.appCancel = context.WithCancel(context.Background())

	const testEnvPrefix = "DAISI_CDC_"
	envKeysToSet := map[string]string{
		testEnvPrefix + strings.ToUpper(config.KeyNatsURL):            s.natsURL,
		testEnvPrefix + strings.ToUpper(config.KeyRedisAddr):          "redis://" + s.redisAddr,
		testEnvPrefix + strings.ToUpper(config.KeyLogLevel):           "debug",
		testEnvPrefix + strings.ToUpper(config.KeyMetricsPort):        "9099", // Test-specific port
		testEnvPrefix + strings.ToUpper(config.KeyJSWaStreamName):     "test_wa_stream",
		testEnvPrefix + strings.ToUpper(config.KeyJSCdcStreamName):    "test_cdc_events_stream",
		testEnvPrefix + strings.ToUpper(config.KeyJSCdcConsumerGroup): "test_cdc_consumers",
	}
	s.appMetricsPort = envKeysToSet[testEnvPrefix+strings.ToUpper(config.KeyMetricsPort)]

	for key, value := range envKeysToSet {
		originalValue, isSet := os.LookupEnv(key)
		if isSet {
			s.originalEnvValues[key] = originalValue
		} else {
			// Store a special marker if it wasn't set, so we can unset it later
			s.originalEnvValues[key] = "__NOT_SET__"
		}
		os.Setenv(key, value)
	}

	s.appContainer, s.appCleanup, err = bootstrap.InitializeApp()
	s.Require().NoError(err, "SetupSuite: Failed to initialize application DI container")

	s.appLogger = s.appContainer.Logger
	s.appIngester = s.appContainer.Ingester
	// Publisher, DedupStore, and Consumer should now be available on bootstrap.App after go generate
	s.appPublisher = s.appContainer.Publisher
	s.appDedupStore = s.appContainer.DedupStore
	s.appConsumer = s.appContainer.Consumer

	s.Require().NotNil(s.appLogger, "appLogger is nil after InitializeApp")
	s.Require().NotNil(s.appIngester, "appIngester is nil after InitializeApp")
	s.Require().NotNil(s.appPublisher, "appPublisher is nil after InitializeApp")
	s.Require().NotNil(s.appDedupStore, "appDedupStore is nil after InitializeApp")
	s.Require().NotNil(s.appConsumer, "appConsumer is nil after InitializeApp")

	s.appLogger.Info(s.appCtx, "Application components initialized for integration test suite")

	if s.appIngester != nil {
		s.T().Log("Starting application ingester...")
		go func() {
			defer func() {
				if r := recover(); r != nil {
					var panicErr error
					if errAsErr, ok := r.(error); ok {
						panicErr = errAsErr
					} else {
						panicErr = fmt.Errorf("%v", r)
					}
					s.appLogger.Error(s.appCtx, "Ingester panicked", zap.Error(panicErr)) // Corrected logger call
					s.T().Errorf("Ingester panicked: %v", r)
				}
			}()
			if err := s.appIngester.Start(); err != nil && err != context.Canceled && err != natsIO.ErrConnectionClosed {
				s.appLogger.Error(s.appCtx, "Ingester failed to start or stopped unexpectedly", zap.Error(err)) // Corrected
			}
		}()
	} else {
		s.T().Log("appIngester is nil, cannot start. Check bootstrap.App structure and DI wiring.")
	}

	s.T().Logf("Application metrics server should be running on port %s (started by DI)", s.appMetricsPort)
	time.Sleep(2 * time.Second)
	s.T().Log("Integration test suite setup complete with application initialized.")
}

// TearDownSuite runs once after all tests in the suite are finished.
func (s *IntegrationTestSuite) TearDownSuite() {
	s.T().Log("Tearing down integration test suite...")

	if s.appCancel != nil {
		s.appCancel()
	}
	time.Sleep(1 * time.Second)

	if s.appCleanup != nil {
		s.T().Log("Calling InitializeApp cleanup function...")
		s.appCleanup()
	}

	// Restore original ENV vars
	for key, originalValue := range s.originalEnvValues {
		if originalValue == "__NOT_SET__" {
			_ = os.Unsetenv(key)
		} else {
			os.Setenv(key, originalValue)
		}
	}

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
		s.cancel()
	}
	s.T().Log("Integration test suite teardown complete.")
}

// TestAppComponentsInitialization verifies that essential app components are not nil.
func (s *IntegrationTestSuite) TestAppComponentsInitialization() {
	s.T().Log("Verifying application component initialization...")
	s.Require().NotNil(s.appContainer, "DI Container (bootstrap.App) should not be nil")
	s.NotNil(s.appLogger, "Logger should not be nil")
	s.NotNil(s.appIngester, "Ingester should not be nil")
	// Uncommented now, assuming go generate was successful
	s.NotNil(s.appPublisher, "Publisher should not be nil")
	s.NotNil(s.appDedupStore, "DedupStore should not be nil")
	s.NotNil(s.appConsumer, "Consumer should not be nil")
	s.T().Log("Application components appear initialized.")
}

// TestAppNATSConnectionHealth checks if the application's NATS ingester/publisher seems healthy.
func (s *IntegrationTestSuite) TestAppNATSConnectionHealth() {
	s.T().Log("Verifying application's NATS connection health (basic check)...")
	s.Require().NotNil(s.appContainer, "appContainer is nil, cannot proceed")
	s.Require().NotNil(s.appContainer.Cfg, "appContainer.Cfg is nil, cannot proceed")

	nc, err := natsIO.Connect(s.natsURL)
	s.Require().NoError(err, "Helper NATS connection failed")
	defer nc.Close()
	jsCtx, err := nc.JetStream()
	s.Require().NoError(err, "Helper NATS JetStream context failed")

	streamName := s.appContainer.Cfg.GetString(config.KeyJSCdcStreamName)
	s.Require().NotEmpty(streamName, "CDC Stream name from app config should not be empty")
	_, err = jsCtx.StreamInfo(streamName)
	s.NoError(err, "Application's CDC stream '%s' should exist", streamName)
	s.T().Logf("Application's CDC stream '%s' found.", streamName)

	waStreamName := s.appContainer.Cfg.GetString(config.KeyJSWaStreamName)
	s.Require().NotEmpty(waStreamName, "WA Stream name from app config should not be empty")
	_, err = jsCtx.StreamInfo(waStreamName)
	s.NoError(err, "Application's WA stream '%s' should exist", waStreamName)
	s.T().Logf("Application's WA stream '%s' found.", waStreamName)
}

// TestAppRedisConnectionHealth checks if the application's DedupStore is working.
func (s *IntegrationTestSuite) TestAppRedisConnectionHealth() {
	s.T().Log("Verifying application's Redis connection health...")
	// No longer a TODO, should work if s.appDedupStore is populated.
	s.Require().NotNil(s.appDedupStore, "appDedupStore is nil. Ensure bootstrap.App exposes it and it's extracted in SetupSuite.")
	s.Require().NotNil(s.appContainer, "appContainer is nil")
	s.Require().NotNil(s.appContainer.Cfg, "appContainer.Cfg is nil")

	dedupTTL := s.appContainer.Cfg.GetDuration(config.KeyDedupTTL)
	isDup, err := s.appDedupStore.IsDuplicate(s.appCtx, domain.EventID("healthcheck:testkey:11.3"), dedupTTL)
	s.NoError(err, "DedupStore.IsDuplicate should not error for health check")
	s.False(isDup, "DedupStore.IsDuplicate should return false for a new key")

	isDupAgain, errAgain := s.appDedupStore.IsDuplicate(s.appCtx, domain.EventID("healthcheck:testkey:11.3"), dedupTTL)
	s.NoError(errAgain, "DedupStore.IsDuplicate (2nd call) should not error")
	s.True(isDupAgain, "DedupStore.IsDuplicate (2nd call) should return true for the same key")
	s.T().Log("Application's DedupStore appears to be connected to Redis.")
}

// TestAppMetricsEndpointAccessible checks if the app's /metrics endpoint is up.
func (s *IntegrationTestSuite) TestAppMetricsEndpointAccessible() {
	s.T().Logf("Verifying application's /metrics endpoint on port %s...", s.appMetricsPort)
	s.Require().NotEmpty(s.appMetricsPort, "appMetricsPort is not set")

	httpClient := http.Client{Timeout: 5 * time.Second}
	metricsURL := fmt.Sprintf("http://localhost:%s/metrics", s.appMetricsPort)
	resp, err := httpClient.Get(metricsURL)
	s.Require().NoError(err, "HTTP GET to /metrics endpoint failed")
	defer resp.Body.Close()

	s.Equal(http.StatusOK, resp.StatusCode, "/metrics endpoint should return 200 OK")
	bodyBytes, err := io.ReadAll(resp.Body)
	s.Require().NoError(err, "Failed to read /metrics response body")
	s.Contains(string(bodyBytes), "go_gc_duration_seconds", "/metrics response should contain standard Go metrics")
	s.T().Log("/metrics endpoint is accessible and serves basic metrics.")
}

// TestNATSContainerRawConnection verifies that a connection can be established to the NATS container.
func (s *IntegrationTestSuite) TestNATSContainerRawConnection() {
	s.T().Logf("TestNATSContainerRawConnection: Attempting to connect to NATS at %s", s.natsURL)
	nc, err := natsIO.Connect(s.natsURL, natsIO.Timeout(10*time.Second), natsIO.RetryOnFailedConnect(true), natsIO.MaxReconnects(3), natsIO.ReconnectWait(1*time.Second))
	s.Require().NoError(err, "TestNATSContainerRawConnection: Failed to connect to NATS")
	defer nc.Close()

	s.Equal(natsIO.CONNECTED, nc.Status(), "TestNATSContainerRawConnection: NATS connection status should be CONNECTED")
	s.T().Log("TestNATSContainerRawConnection: Successfully connected to NATS and verified status.")
}

// TestRedisContainerRawConnection verifies that a connection can be established to the Redis container.
func (s *IntegrationTestSuite) TestRedisContainerRawConnection() {
	s.T().Logf("TestRedisContainerRawConnection: Attempting to connect to Redis at %s", s.redisAddr)
	opts, err := redis.ParseURL("redis://" + s.redisAddr)
	s.Require().NoError(err, "TestRedisContainerRawConnection: Failed to parse Redis address: %s", s.redisAddr)

	rdb := redis.NewClient(opts)
	defer rdb.Close()

	pingCmd := rdb.Ping(s.ctx)
	s.Require().NoError(pingCmd.Err(), "TestRedisContainerRawConnection: Redis PING command failed")
	s.Equal("PONG", pingCmd.Val(), "TestRedisContainerRawConnection: Redis PING should return PONG")
	s.T().Log("TestRedisContainerRawConnection: Successfully connected to Redis and PING was successful.")
}

// TestRunIntegrationSuite is the entry point for running the integration test suite.
func TestRunIntegrationSuite(t *testing.T) {
	suite.Run(t, new(IntegrationTestSuite))
}
