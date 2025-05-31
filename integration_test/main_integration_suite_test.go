package integration_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
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

	// For WA stream subscription
	waNatsConn         *natsIO.Conn
	waSubscription     *natsIO.Subscription
	receivedWaMessages chan *natsIO.Msg // Buffered channel to store messages from wa_stream
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
		testEnvPrefix + strings.ToUpper(config.KeyNatsURL):               s.natsURL,
		testEnvPrefix + strings.ToUpper(config.KeyRedisAddr):             "redis://" + s.redisAddr,
		testEnvPrefix + strings.ToUpper(config.KeyLogLevel):              "debug",
		testEnvPrefix + strings.ToUpper(config.KeyMetricsPort):           "9099", // Test-specific port
		testEnvPrefix + strings.ToUpper(config.KeyJSWebsocketStreamName): "test_wa_stream",
		testEnvPrefix + strings.ToUpper(config.KeyJSCdcStreamName):       "test_cdc_events_stream",
		testEnvPrefix + strings.ToUpper(config.KeyJSCdcConsumerGroup):    "test_cdc_consumers",
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

	// Setup NATS subscription for wa_stream to capture published application events
	s.T().Logf("TestHelper: Setting up NATS subscription to WA stream subjects on NATS server %s", s.natsURL)
	s.waNatsConn, err = natsIO.Connect(s.natsURL,
		natsIO.Timeout(10*time.Second),
		natsIO.RetryOnFailedConnect(true),
		natsIO.MaxReconnects(3),
		natsIO.ReconnectWait(1*time.Second),
	)
	s.Require().NoError(err, "SetupSuite: Failed to connect waNatsConn to NATS at %s", s.natsURL)

	s.receivedWaMessages = make(chan *natsIO.Msg, 100) // Buffer of 100 messages

	waStreamSubjects := "websocket.>" // Default, can be overridden by config if necessary for tests later
	if s.appContainer != nil && s.appContainer.Cfg != nil {
		cfgWaSubjects := s.appContainer.Cfg.GetString(config.KeyJSWebsocketStreamSubjects)
		if cfgWaSubjects != "" {
			waStreamSubjects = cfgWaSubjects
		}
	}
	s.T().Logf("TestHelper: Subscribing waNatsConn to subjects: %s", waStreamSubjects)
	s.waSubscription, err = s.waNatsConn.ChanSubscribe(waStreamSubjects, s.receivedWaMessages)
	s.Require().NoError(err, "SetupSuite: Failed to subscribe waNatsConn to subjects '%s'", waStreamSubjects)
	s.Require().True(s.waSubscription.IsValid(), "SetupSuite: WA stream subscription should be valid")
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

	// Drain and close WA stream subscription and connection
	if s.waSubscription != nil && s.waSubscription.IsValid() {
		s.T().Log("TestHelper: Draining WA stream subscription...")
		if err := s.waSubscription.Drain(); err != nil {
			s.T().Logf("TestHelper: Error draining WA stream subscription: %v", err)
		}
		// Unsubscribe is often implicitly handled by Drain + connection close, but explicit can be good
		// if err := s.waSubscription.Unsubscribe(); err != nil {
		// 	s.T().Logf("TestHelper: Error unsubscribing WA stream: %v", err)
		// }
	}
	if s.waNatsConn != nil {
		s.T().Log("TestHelper: Draining and closing WA NATS connection...")
		if !s.waNatsConn.IsClosed() {
			if err := s.waNatsConn.Drain(); err != nil {
				s.T().Logf("TestHelper: Error draining WA NATS connection: %v", err)
			}
			s.waNatsConn.Close()
		}
	}
	if s.receivedWaMessages != nil {
		close(s.receivedWaMessages)
	}

	if s.cancel != nil {
		s.cancel()
	}
	s.T().Log("Integration test suite teardown complete.")
}

// waitForPublishedWaMessage waits for a message on the WA stream that matches the criteria.
func (s *IntegrationTestSuite) waitForPublishedWaMessage(timeout time.Duration, criteria func(msgData []byte) bool) (*natsIO.Msg, error) {
	s.T().Helper()
	timeoutCtx, cancel := context.WithTimeout(s.appCtx, timeout) // Use appCtx as base, can be suite ctx too
	defer cancel()

	s.T().Logf("TestHelper: Waiting for message on WA stream for up to %s...", timeout)

	for {
		select {
		case <-timeoutCtx.Done():
			return nil, fmt.Errorf("timed out waiting for message on WA stream after %s: %w", timeout, timeoutCtx.Err())
		case msg, ok := <-s.receivedWaMessages:
			if !ok {
				return nil, fmt.Errorf("receivedWaMessages channel was closed while waiting")
			}
			if msg == nil { // Should not happen with a well-behaved ChanSubscribe
				continue
			}
			s.T().Logf("TestHelper: Received candidate message on WA stream. Subject: %s, Size: %d bytes", msg.Subject, len(msg.Data))
			if criteria(msg.Data) {
				s.T().Logf("TestHelper: Criteria met for message on subject '%s'", msg.Subject)
				return msg, nil
			} else {
				s.T().Logf("TestHelper: Criteria NOT met for message on subject '%s'. Continuing to wait...", msg.Subject)
			}
		}
	}
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

// TestAppNATSConnectionHealth checks if the application's NATS connection health (basic check)
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

	waStreamName := s.appContainer.Cfg.GetString(config.KeyJSWebsocketStreamName)
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

// --- Prometheus Metrics Helper ---

// getMetricValue parses Prometheus text format and extracts the value of a specific metric with given labels.
// This is a simplified parser; for complex scenarios, a proper Prometheus client library might be better.
func getMetricValue(t *testing.T, metricsBody string, metricName string, labels map[string]string) (float64, error) {
	t.Helper()
	scanner := bufio.NewScanner(strings.NewReader(metricsBody))
	lookingFor := metricName
	if len(labels) > 0 {
		lookingFor += "{"
		labelParts := []string{}
		for k, v := range labels {
			labelParts = append(labelParts, fmt.Sprintf(`%s="%s"`, k, v))
		}
		sort.Strings(labelParts) // Labels need to be in alphabetical order as Prometheus exports them
		lookingFor += strings.Join(labelParts, ",")
		lookingFor += "}"
	}

	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") { // Skip comments and type lines
			continue
		}
		if strings.HasPrefix(line, lookingFor+" ") { // Ensure it's the exact metric, followed by a space then value
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				valueStr := parts[len(parts)-1] // Value is the last part
				value, err := strconv.ParseFloat(valueStr, 64)
				if err != nil {
					return 0, fmt.Errorf("failed to parse metric value '%s' for '%s': %w", valueStr, line, err)
				}
				return value, nil
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("error scanning metrics body: %w", err)
	}
	return 0, fmt.Errorf("metric '%s' with labels %v not found", metricName, labels)
}

// fetchMetrics retrieves the current metrics from the application's /metrics endpoint.
func (s *IntegrationTestSuite) fetchMetrics(t *testing.T) string {
	t.Helper()
	s.Require().NotEmpty(s.appMetricsPort, "fetchMetrics: appMetricsPort is not set")

	httpClient := http.Client{Timeout: 5 * time.Second}
	metricsURL := fmt.Sprintf("http://localhost:%s/metrics", s.appMetricsPort)
	resp, err := httpClient.Get(metricsURL)
	s.Require().NoError(err, "fetchMetrics: HTTP GET to /metrics endpoint failed")
	defer resp.Body.Close()

	s.Require().Equal(http.StatusOK, resp.StatusCode, "fetchMetrics: /metrics endpoint should return 200 OK")
	bodyBytes, err := io.ReadAll(resp.Body)
	s.Require().NoError(err, "fetchMetrics: Failed to read /metrics response body")
	return string(bodyBytes)
}

// --- Test Methods ---

// TestHappyPath_SingleMessage_MessagesTable tests the end-to-end happy path for a single CDC event.
func (s *IntegrationTestSuite) TestHappyPath_SingleMessage_MessagesTable() {
	s.T().Log("Running TestHappyPath_SingleMessage_MessagesTable...")

	// 1. Prepare a sample CDCEventData for the 'messages' table
	testCompanyID := "test-company-happy-456"
	testMessagePK := fmt.Sprintf("msg_happy_%s", uuid.NewString()[0:8]) // Unique message ID
	now := time.Now().UTC()

	sampleMessageData := domain.MessageData{
		ID:               now.UnixNano() / 1000, // Example of a unique int64 ID
		MessageID:        testMessagePK,
		ChatID:           "chat_happy_789",
		AgentID:          "agent_happy_abc",
		CompanyID:        testCompanyID,
		From:             "user@example.com",
		To:               "agent@example.com",
		Jid:              "user@example.com/resource",
		Flow:             "inbound",
		MessageObj:       map[string]interface{}{"type": "text", "text": "Hello from happy path E2E test!"},
		Key:              map[string]interface{}{"id": uuid.NewString(), "remoteJid": "user@example.com", "fromMe": false},
		Status:           "delivered",
		IsDeleted:        false,
		MessageTimestamp: now.UnixMilli(),
		MessageDate:      domain.CustomDate{Time: now},
		// CreatedAt:        now,
		// UpdatedAt:        now,
	}

	recordData := make(map[string]interface{})
	messageDataJSON, err := json.Marshal(sampleMessageData)
	s.Require().NoError(err, "Failed to marshal sampleMessageData")
	err = json.Unmarshal(messageDataJSON, &recordData)
	s.Require().NoError(err, "Failed to unmarshal messageDataJSON to recordData map")

	commitLSN := now.UnixNano()

	sampleCDCEvent := domain.CDCEventData{
		Record:  recordData,
		Changes: nil,
		Action:  "insert",
		Metadata: struct {
			TableSchema            string                 `json:"table_schema"`
			TableName              string                 `json:"table_name"`
			CommitTimestamp        string                 `json:"commit_timestamp"`
			CommitLSN              int64                  `json:"commit_lsn"`
			IdempotencyKey         string                 `json:"idempotency_key"`
			TransactionAnnotations map[string]interface{} `json:"transaction_annotations,omitempty"`
			Sink                   struct {
				ID   string `json:"id"`
				Name string `json:"name"`
			} `json:"sink"`
		}{
			TableSchema:     "public", // Default schema used by Sequin/test
			TableName:       "messages",
			CommitTimestamp: now.Format(time.RFC3339Nano),
			CommitLSN:       commitLSN,
			IdempotencyKey:  uuid.New().String(),
			Sink: struct {
				ID   string `json:"id"`
				Name string `json:"name"`
			}{
				ID:   "test-sink-id",
				Name: "test-sequin-sink",
			},
		},
		PK:        map[string]interface{}{"id": sampleMessageData.ID},
		TypedData: &sampleMessageData,
	}

	// Construct the subject for publishing the raw CDC event, mimicking Sequin's pattern
	// sequin.changes.<database_name>.<schema_name>.<table_name>.<action>
	// The consumer subscribes to a pattern like this via KeyJSCdcStreamSubjects
	// (e.g., default "sequin.changes.*.*.*.*")
	cdcPublishSubject := fmt.Sprintf("sequin.changes.test_db.%s.%s.%s",
		sampleCDCEvent.Metadata.TableSchema,
		sampleCDCEvent.Metadata.TableName,
		sampleCDCEvent.Action,
	)
	s.T().Logf("Publishing raw CDC event to subject: %s", cdcPublishSubject)

	// 2. Publish the event using the helper from event_helpers_test.go
	// This helper connects its own NATS client using s.natsURL.
	err = publishCDCEvent(s.T(), s, cdcPublishSubject, sampleCDCEvent) // Pass by value
	s.Require().NoError(err, "Failed to publish CDC event")
	s.T().Logf("Published sample CDC event for MessageID: %s, LSN: %d", sampleMessageData.MessageID, sampleCDCEvent.Metadata.CommitLSN)

	// 3. Wait for the enriched message on wa_stream
	expectedOriginalMessageID := sampleMessageData.MessageID
	s.T().Logf("Waiting for enriched message on WA stream corresponding to original MessageID: %s", expectedOriginalMessageID)

	receivedMsg, err := s.waitForPublishedWaMessage(15*time.Second, func(msgData []byte) bool {
		var enrichedPayload domain.EnrichedEventPayload
		if jsonErr := json.Unmarshal(msgData, &enrichedPayload); jsonErr != nil {
			s.T().Logf("waitForPublishedWaMessage criteria: Error unmarshalling msgData: %v", jsonErr)
			return false
		}
		return enrichedPayload.MessageID == expectedOriginalMessageID &&
			enrichedPayload.CompanyID == sampleMessageData.CompanyID &&
			enrichedPayload.AgentID == sampleMessageData.AgentID
	})
	s.Require().NoError(err, "Did not receive expected message on wa_stream within timeout")
	s.Require().NotNil(receivedMsg, "Received nil message from wa_stream")
	s.T().Logf("Received message on WA stream. Subject: %s", receivedMsg.Subject)

	// 4. Unmarshal and validate the enriched message
	var enrichedPayload domain.EnrichedEventPayload
	err = json.Unmarshal(receivedMsg.Data, &enrichedPayload)
	s.Require().NoError(err, "Failed to unmarshal enriched event payload from WA stream message")

	s.T().Logf("Successfully unmarshalled EnrichedEventPayload. EventID: %s, CompanyID: %s, AgentID: %s, MessageID: %s, ChatID: %s",
		enrichedPayload.EventID, enrichedPayload.CompanyID, enrichedPayload.AgentID, enrichedPayload.MessageID, enrichedPayload.ChatID)

	s.NotEmpty(enrichedPayload.EventID, "EnrichedEventPayload.EventID should not be empty")
	s.Equal(sampleMessageData.CompanyID, enrichedPayload.CompanyID, "Enriched CompanyID mismatch")
	s.Equal(sampleMessageData.AgentID, enrichedPayload.AgentID, "Enriched AgentID mismatch")
	s.Equal(sampleMessageData.MessageID, enrichedPayload.MessageID, "Enriched MessageID mismatch")
	s.Equal(sampleMessageData.ChatID, enrichedPayload.ChatID, "Enriched ChatID mismatch")

	s.Require().NotNil(enrichedPayload.RowData, "EnrichedEventPayload.RowData is nil")
	s.T().Logf("Enriched RowData: %+v", enrichedPayload.RowData)

	s.assertRowDataField(enrichedPayload.RowData, "id", float64(sampleMessageData.ID))
	s.assertRowDataField(enrichedPayload.RowData, "message_id", sampleMessageData.MessageID)
	s.assertRowDataField(enrichedPayload.RowData, "chat_id", sampleMessageData.ChatID)
	s.assertRowDataField(enrichedPayload.RowData, "agent_id", sampleMessageData.AgentID)
	s.assertRowDataField(enrichedPayload.RowData, "company_id", sampleMessageData.CompanyID)
	s.assertRowDataField(enrichedPayload.RowData, "from", sampleMessageData.From)
	s.assertRowDataField(enrichedPayload.RowData, "to", sampleMessageData.To)
	s.assertRowDataField(enrichedPayload.RowData, "flow", sampleMessageData.Flow)
	s.assertRowDataField(enrichedPayload.RowData, "status", sampleMessageData.Status)
	s.assertRowDataField(enrichedPayload.RowData, "is_deleted", sampleMessageData.IsDeleted)
	s.assertRowDataField(enrichedPayload.RowData, "message_timestamp", float64(sampleMessageData.MessageTimestamp))

	expectedMessageDateStr := sampleMessageData.MessageDate.Time.Format("2006-01-02")
	s.assertRowDataField(enrichedPayload.RowData, "message_date", expectedMessageDateStr)

	expectedMessageObjJSON, err := json.Marshal(sampleMessageData.MessageObj)
	s.Require().NoError(err, "Failed to marshal expected MessageObj")
	actualMessageObjRaw, ok := enrichedPayload.RowData["message_obj"]
	s.Require().True(ok, "RowData.message_obj not found")
	actualMessageObjJSON, err := json.Marshal(actualMessageObjRaw)
	s.Require().NoError(err, "Failed to marshal actual MessageObj from RowData")
	s.JSONEq(string(expectedMessageObjJSON), string(actualMessageObjJSON), "RowData.message_obj mismatch")

	expectedKeyJSON, err := json.Marshal(sampleMessageData.Key)
	s.Require().NoError(err, "Failed to marshal expected Key")
	actualKeyRaw, ok := enrichedPayload.RowData["key"]
	s.Require().True(ok, "RowData.key not found")
	actualKeyJSON, err := json.Marshal(actualKeyRaw)
	s.Require().NoError(err, "Failed to marshal actual Key from RowData")
	s.JSONEq(string(expectedKeyJSON), string(actualKeyJSON), "RowData.key mismatch")

	// Verify the EventID construction (LSN:Table:PK)
	expectedEventID := fmt.Sprintf("%d:%s:%s", sampleCDCEvent.Metadata.CommitLSN, "messages", sampleMessageData.MessageID)
	s.Equal(expectedEventID, enrichedPayload.EventID, "Enriched EventID does not match expected format")

	// 4. Verify Prometheus metrics
	s.T().Logf("Verifying Prometheus metrics after happy path event...")

	// Previous state after TestDuplicateMessageHandling (for table='messages'):
	// - cdc_consumer_events_total{result="processed"}: 1
	// - daisi_cdc_consumer_deduplication_checks_total{result="miss"}: 1
	// - daisi_cdc_consumer_deduplication_checks_total{result="hit"}: 1
	// This test (TestHappyPath_SingleMessage_MessagesTable) adds for table='messages':
	// - +1 to cdc_consumer_events_total{result="processed"} (cumulative total: 2)
	// - +1 to daisi_cdc_consumer_deduplication_checks_total{result="miss"} (cumulative total: 2)
	// - +0 to daisi_cdc_consumer_deduplication_checks_total{result="hit"} (cumulative total: 1)

	// Fetch all metrics once for this verification block
	metricsBody := s.fetchMetrics(s.T())

	// 1. Assert cdc_consumer_events_total{table="messages", result="processed"}
	processedCount, err := getMetricValue(s.T(), metricsBody, "cdc_consumer_events_total", map[string]string{"table": "messages", "result": "processed"})
	s.Require().NoError(err, "Failed to get metric cdc_consumer_events_total for messages/processed")
	s.Assert().Equal(2.0, processedCount, "Expected 2 events processed for messages table (cumulative), got %.f", processedCount)
	s.T().Logf("Metric cdc_consumer_events_total{table=\"messages\", result=\"processed\"}: %f", processedCount)

	// 2. Assert cdc_consumer_events_published_total for the specific subject
	// enrichedPayload should be the variable holding the unmarshalled domain.EnrichedEventPayload
	// Construct the expected subject based on its fields, similar to how it's done in transform_service.go
	if enrichedPayload.CompanyID == "" || enrichedPayload.AgentID == "" || enrichedPayload.ChatID == "" {
		s.T().Fatalf("enrichedPayload CompanyID, AgentID, or ChatID is empty, cannot assert cdc_consumer_events_published_total metric. CompanyID: '%s', AgentID: '%s', ChatID: '%s'", enrichedPayload.CompanyID, enrichedPayload.AgentID, enrichedPayload.ChatID)
	}
	expectedPublishedSubject := fmt.Sprintf("websocket.%s.%s.messages.%s", enrichedPayload.CompanyID, enrichedPayload.AgentID, enrichedPayload.ChatID)

	publishedCount, err := getMetricValue(s.T(), metricsBody, "cdc_consumer_events_published_total", map[string]string{"subject": expectedPublishedSubject, "status": "success"})
	s.Require().NoError(err, "Failed to get metric cdc_consumer_events_published_total for subject %s", expectedPublishedSubject)
	s.Assert().Equal(1.0, publishedCount, "Expected 1 event published successfully to WA stream for subject %s, got %.f", expectedPublishedSubject, publishedCount)
	s.T().Logf("Metric cdc_consumer_events_published_total{subject=\"%s\", status=\"success\"}: %f", expectedPublishedSubject, publishedCount)

	// 3. Assert daisi_cdc_consumer_deduplication_checks_total{table="messages", result="miss"}
	dedupMissCount, err := getMetricValue(s.T(), metricsBody, "daisi_cdc_consumer_deduplication_checks_total", map[string]string{"table": "messages", "result": "miss"})
	s.Require().NoError(err, "Failed to get metric daisi_cdc_consumer_deduplication_checks_total for messages/miss")
	s.Assert().Equal(2.0, dedupMissCount, "Expected 2 deduplication checks with result 'miss' for messages table (cumulative), got %.f", dedupMissCount)
	s.T().Logf("Metric daisi_cdc_consumer_deduplication_checks_total{table=\"messages\", result=\"miss\"}: %f", dedupMissCount)

	// 4. Assert daisi_cdc_consumer_deduplication_checks_total{table="messages", result="hit"}
	// This should be 1.0 (cumulative from TestDuplicateMessageHandling) as this test case is a new event (a miss).
	dedupHitCount, err := getMetricValue(s.T(), metricsBody, "daisi_cdc_consumer_deduplication_checks_total", map[string]string{"table": "messages", "result": "hit"})
	if err != nil {
		// If the metric is expected to be 1.0, an error fetching it (e.g., not found) is a test failure.
		s.Require().NoError(err, "Failed to get metric daisi_cdc_consumer_deduplication_checks_total for messages/hit, expected value 1.0")
	}
	s.Assert().Equal(1.0, dedupHitCount, "Metric daisi_cdc_consumer_deduplication_checks_total{table=\"messages\", result=\"hit\"} should be 1.0 (cumulative), got %.f", dedupHitCount)
	s.T().Logf("Metric daisi_cdc_consumer_deduplication_checks_total{table=\"messages\", result=\"hit\"}: %f", dedupHitCount)

	s.T().Logf("TestHappyPath_SingleMessage_MessagesTable completed successfully.")
}

// TestDuplicateMessageHandling tests that a duplicate CDC event is correctly deduplicated,
// not published a second time, and metrics reflect this.
func (s *IntegrationTestSuite) TestDuplicateMessageHandling() {
	s.T().Log("Running TestDuplicateMessageHandling...")

	// 1. Prepare a sample CDCEventData for the 'messages' table
	testCompanyID := "test-company-duplicate-789"
	testMessagePK := fmt.Sprintf("msg_dup_%s", uuid.NewString()[0:8])
	now := time.Now().UTC()

	sampleMessageData := domain.MessageData{
		ID:               now.UnixNano()/1000 + 100, // Ensure unique ID from happy path
		MessageID:        testMessagePK,
		ChatID:           "chat_dup_123",
		AgentID:          "agent_dup_def",
		CompanyID:        testCompanyID,
		From:             "sender_dup@example.com",
		To:               "receiver_dup@example.com",
		MessageObj:       map[string]interface{}{"text": "This is a duplicate test message."},
		Status:           "sent",
		IsDeleted:        false,
		MessageTimestamp: now.UnixMilli(),
		MessageDate:      domain.CustomDate{Time: now},
		// CreatedAt:        now,
		// UpdatedAt:        now,
	}

	recordData := make(map[string]interface{})
	messageDataJSON, err := json.Marshal(sampleMessageData)
	s.Require().NoError(err, "Failed to marshal sampleMessageData for duplicate test")
	err = json.Unmarshal(messageDataJSON, &recordData)
	s.Require().NoError(err, "Failed to unmarshal messageDataJSON to recordData map for duplicate test")

	commitLSN := now.UnixNano() + 1000 // Ensure unique LSN

	sampleCDCEvent := domain.CDCEventData{
		Record: recordData,
		Action: "insert",
		Metadata: struct {
			TableSchema            string                 `json:"table_schema"`
			TableName              string                 `json:"table_name"`
			CommitTimestamp        string                 `json:"commit_timestamp"`
			CommitLSN              int64                  `json:"commit_lsn"`
			IdempotencyKey         string                 `json:"idempotency_key"`
			TransactionAnnotations map[string]interface{} `json:"transaction_annotations,omitempty"`
			Sink                   struct {
				ID   string `json:"id"`
				Name string `json:"name"`
			} `json:"sink"`
		}{
			TableSchema:     "public",
			TableName:       "messages",
			CommitTimestamp: now.Format(time.RFC3339Nano),
			CommitLSN:       commitLSN,
			IdempotencyKey:  uuid.New().String(),
		},
		PK:        map[string]interface{}{"id": sampleMessageData.ID},
		TypedData: &sampleMessageData,
	}

	cdcPublishSubject := fmt.Sprintf("sequin.changes.test_db.%s.%s.%s",
		sampleCDCEvent.Metadata.TableSchema,
		sampleCDCEvent.Metadata.TableName,
		sampleCDCEvent.Action,
	)

	// 2. Publish the event FIRST time
	s.T().Logf("Publishing first instance of CDC event. LSN: %d, MessageID: %s", sampleCDCEvent.Metadata.CommitLSN, sampleMessageData.MessageID)
	err = publishCDCEvent(s.T(), s, cdcPublishSubject, sampleCDCEvent)
	s.Require().NoError(err, "Failed to publish first CDC event")

	// 3. Wait for and validate the first enriched message on wa_stream
	expectedOriginalMessageID := sampleMessageData.MessageID
	receivedMsg1, err := s.waitForPublishedWaMessage(10*time.Second, func(msgData []byte) bool {
		var p domain.EnrichedEventPayload
		_ = json.Unmarshal(msgData, &p)
		return p.MessageID == expectedOriginalMessageID && p.CompanyID == testCompanyID
	})
	s.Require().NoError(err, "Did not receive the first message on wa_stream")
	s.Require().NotNil(receivedMsg1, "First received message should not be nil")
	s.T().Logf("First message received on WA stream. Subject: %s", receivedMsg1.Subject)

	var enrichedPayload1 domain.EnrichedEventPayload
	err = json.Unmarshal(receivedMsg1.Data, &enrichedPayload1)
	s.Require().NoError(err, "Failed to unmarshal first enriched payload")

	// 4. Publish the event SECOND time (duplicate)
	s.T().Logf("Publishing second (duplicate) instance of CDC event. LSN: %d, MessageID: %s", sampleCDCEvent.Metadata.CommitLSN, sampleMessageData.MessageID)
	// IMPORTANT: Use a *slightly different* idempotency key for the raw CDC publishing if Sequin itself dedups based on that.
	// However, our internal dedupper uses EventID (LSN:Table:PK), so the same CDCEventData should trigger internal dedup.
	// For this test, let's assume the same raw event is published.
	err = publishCDCEvent(s.T(), s, cdcPublishSubject, sampleCDCEvent) // Publish exact same event again
	s.Require().NoError(err, "Failed to publish second (duplicate) CDC event")

	// 5. Assert that a SECOND enriched message for the same original event does NOT appear
	s.T().Logf("Verifying no second message for MessageID %s is published to WA stream...", expectedOriginalMessageID)
	_, err = s.waitForPublishedWaMessage(5*time.Second, func(msgData []byte) bool { // Shorter timeout
		var p domain.EnrichedEventPayload
		_ = json.Unmarshal(msgData, &p)
		// This criteria should ideally not be met by a *new* message if dedup works
		return p.MessageID == expectedOriginalMessageID && p.EventID != enrichedPayload1.EventID // Looking for a *different* enriched event for the *same* original message
	})
	s.Error(err, "Expected a timeout or error waiting for a *second, distinct* enriched message, as it should be deduplicated.")
	if err != nil {
		s.Contains(err.Error(), "timed out waiting for message", "Error should indicate timeout for the second message")
		s.T().Log("Correctly did not receive a second distinct enriched message for the duplicate event.")
	} else {
		s.Fail("A second, distinct enriched message was unexpectedly received for a supposedly duplicate event.")
	}

	// 6. Verify Prometheus metrics
	s.T().Log("Verifying Prometheus metrics for duplicate handling...")
	time.Sleep(1 * time.Second) // Give Prometheus a moment
	metricsBody := s.fetchMetrics(s.T())

	consumerProcessedMetricName := "cdc_consumer_events_total"
	publisherPublishedMetricName := "cdc_consumer_events_published_total"
	deduplicationMetricName := "daisi_cdc_consumer_deduplication_checks_total"

	// Metric: cdc_consumer_events_total (processed)
	processedCount, err := getMetricValue(s.T(), metricsBody, consumerProcessedMetricName, map[string]string{"table": "messages", "result": "processed"})
	s.Require().NoError(err, "Failed to get metric: %s {result:processed}", consumerProcessedMetricName)
	s.Equal(float64(1), processedCount, "Expected 1 event processed for messages table, got %.f", processedCount)

	// Metric: cdc_consumer_events_total (duplicate)
	duplicateSkippedCount, err := getMetricValue(s.T(), metricsBody, consumerProcessedMetricName, map[string]string{"table": "messages", "result": "duplicate"})
	s.Require().NoError(err, "Failed to get metric: %s {result:duplicate}", consumerProcessedMetricName)
	s.Equal(float64(1), duplicateSkippedCount, "Expected 1 event skipped as duplicate for messages table, got %.f", duplicateSkippedCount)

	// Metric: cdc_consumer_events_published_total (Ensure only one publish for the original successful event)
	expectedPublishedSubject := fmt.Sprintf("websocket.%s.%s.messages.%s",
		enrichedPayload1.CompanyID,
		enrichedPayload1.AgentID,
		enrichedPayload1.ChatID,
	)
	publishedCount, err := getMetricValue(s.T(), metricsBody, publisherPublishedMetricName, map[string]string{"subject": expectedPublishedSubject, "status": "success"})
	s.Require().NoError(err, "Failed to get metric: %s for subject %s", publisherPublishedMetricName, expectedPublishedSubject)
	s.Equal(float64(1), publishedCount, "Expected only 1 event published to subject %s, got %.f", expectedPublishedSubject, publishedCount)

	// Metric: daisi_cdc_consumer_deduplication_checks_total (miss)
	dedupMissCount, err := getMetricValue(s.T(), metricsBody, deduplicationMetricName, map[string]string{"table": "messages", "result": "miss"})
	s.Require().NoError(err, "Failed to get metric: %s {result:miss}", deduplicationMetricName)
	s.Equal(float64(1), dedupMissCount, "Expected 1 deduplication check with result 'miss', got %.f", dedupMissCount)

	// Metric: daisi_cdc_consumer_deduplication_checks_total (hit)
	dedupHitCount, err := getMetricValue(s.T(), metricsBody, deduplicationMetricName, map[string]string{"table": "messages", "result": "hit"})
	s.Require().NoError(err, "Failed to get metric: %s {result:hit}", deduplicationMetricName)
	s.Equal(float64(1), dedupHitCount, "Expected 1 deduplication check with result 'hit', got %.f", dedupHitCount)

	s.T().Log("TestDuplicateMessageHandling completed successfully.")
}

// TestSkippedTableHandling tests that a CDC event for a table not in the allowed list
// (e.g., "user_profiles") is correctly filtered out, not published, and metrics reflect this.
func (s *IntegrationTestSuite) TestSkippedTableHandling() {
	s.T().Log("Running TestSkippedTableHandling...")

	// 1. Prepare a sample CDCEventData for an unallowed table
	skippedTableName := "user_profiles" // This table should NOT be in domain.AllowedTables
	testPK := fmt.Sprintf("profile_%s", uuid.NewString()[0:8])
	now := time.Now().UTC()

	// Ensure the skipped table is indeed not in the allowed list for the test to be valid.
	// We check if the key exists in the map. 'ok' will be true if it exists, false otherwise.
	// We assert that 'ok' is false for this unallowed table.
	_, ok := domain.AllowedTables[skippedTableName]
	s.False(ok, "The table '%s' should NOT be in domain.AllowedTables for this test to be valid. 'ok' was: %v", skippedTableName, ok)

	sampleSkippedRecord := map[string]interface{}{
		"id":         testPK,
		"user_id":    "user_skipped_123",
		"company_id": "company_skipped_abc", // Include company_id as it's often used for routing/logging
		"full_name":  "Skipped User",
		"email":      "skipped@example.com",
		"created_at": now.Format(time.RFC3339Nano),
	}

	commitLSN := now.UnixNano() + 2000 // Ensure unique LSN

	sampleCDCEvent := domain.CDCEventData{
		Record: sampleSkippedRecord,
		Action: "insert",
		Metadata: struct {
			TableSchema            string                 `json:"table_schema"`
			TableName              string                 `json:"table_name"`
			CommitTimestamp        string                 `json:"commit_timestamp"`
			CommitLSN              int64                  `json:"commit_lsn"`
			IdempotencyKey         string                 `json:"idempotency_key"`
			TransactionAnnotations map[string]interface{} `json:"transaction_annotations,omitempty"`
			Sink                   struct {
				ID   string `json:"id"`
				Name string `json:"name"`
			} `json:"sink"`
		}{
			TableSchema:     "public",
			TableName:       skippedTableName, // Critical: use the unallowed table name
			CommitTimestamp: now.Format(time.RFC3339Nano),
			CommitLSN:       commitLSN,
			IdempotencyKey:  uuid.New().String(),
		},
		PK: map[string]interface{}{"id": testPK},
		// TypedData will be nil as this table isn't handled by specific structs
	}

	cdcPublishSubject := fmt.Sprintf("sequin.changes.test_db.%s.%s.%s",
		sampleCDCEvent.Metadata.TableSchema,
		sampleCDCEvent.Metadata.TableName,
		sampleCDCEvent.Action,
	)

	// 2. Publish the event
	s.T().Logf("Publishing CDC event for skipped table '%s'. LSN: %d, PK: %s", skippedTableName, sampleCDCEvent.Metadata.CommitLSN, testPK)
	err := publishCDCEvent(s.T(), s, cdcPublishSubject, sampleCDCEvent)
	s.Require().NoError(err, "Failed to publish CDC event for skipped table")

	// 3. Assert that NO message is published to wa_stream for this event
	s.T().Logf("Verifying no message for skipped table '%s', PK '%s' is published to WA stream...", skippedTableName, testPK)
	_, err = s.waitForPublishedWaMessage(5*time.Second, func(msgData []byte) bool { // Shorter timeout
		// Try to unmarshal, but any message here is unexpected.
		// A robust check might try to find a unique identifier from the skipped event if it somehow leaked.
		// For now, any message received in this short window after publishing a skipped event is a failure.
		s.T().Logf("Unexpectedly received a message on wa_stream while waiting for skipped event. Data: %s", string(msgData))
		return true // If any message comes through, the criteria is met for failure.
	})
	s.Error(err, "Expected a timeout waiting for a message on wa_stream, as the event for table '%s' should have been skipped.", skippedTableName)
	if err != nil {
		s.Contains(err.Error(), "timed out waiting for message", "Error should indicate timeout for the skipped message")
		s.T().Logf("Correctly did not receive any message on wa_stream for the skipped table event.")
	} else {
		s.Fail("An unexpected message was received on wa_stream for a supposedly skipped table event.")
	}

	// 4. Verify Prometheus metrics
	s.T().Log("Verifying Prometheus metrics for skipped table handling...")
	time.Sleep(1 * time.Second) // Give Prometheus a moment to scrape
	metricsBody := s.fetchMetrics(s.T())

	consumerEventsTotalMetric := "cdc_consumer_events_total"

	// Metric: cdc_consumer_events_total{table="user_profiles", result="skipped"} should be 1
	skippedCount, err := getMetricValue(s.T(), metricsBody, consumerEventsTotalMetric, map[string]string{"table": skippedTableName, "result": "skipped"})
	s.Require().NoError(err, "Failed to get metric: %s{table=%s, result=skipped}", consumerEventsTotalMetric, skippedTableName)
	s.Assert().Equal(1.0, skippedCount, "Expected 1 event skipped for table '%s', got %.f", skippedTableName, skippedCount)
	s.T().Logf("Metric %s{table=\"%s\", result=\"skipped\"}: %f", consumerEventsTotalMetric, skippedTableName, skippedCount)

	// Metric: cdc_consumer_events_total{table="user_profiles", result="processed"} should NOT exist or be 0
	// Depending on metric initialization, it might not exist if never incremented.
	processedCount, err := getMetricValue(s.T(), metricsBody, consumerEventsTotalMetric, map[string]string{"table": skippedTableName, "result": "processed"})
	if err == nil { // Metric exists
		s.Assert().Equal(0.0, processedCount, "Expected 0 events processed for table '%s', got %.f", skippedTableName, processedCount)
		s.T().Logf("Metric %s{table=\"%s\", result=\"processed\"}: %f (expected 0)", consumerEventsTotalMetric, skippedTableName, processedCount)
	} else { // Metric does not exist, which is also OK.
		s.T().Logf("Metric %s{table=\"%s\", result=\"processed\"} not found, which is acceptable as it should be 0.", consumerEventsTotalMetric, skippedTableName)
	}

	// Metric: daisi_cdc_consumer_deduplication_checks_total{table="user_profiles"} should not be incremented
	// as filtering happens before deduplication.
	dedupMissCount, err := getMetricValue(s.T(), metricsBody, "daisi_cdc_consumer_deduplication_checks_total", map[string]string{"table": skippedTableName, "result": "miss"})
	if err == nil {
		s.Assert().Equal(0.0, dedupMissCount, "Expected 0 dedup miss for table '%s', got %.f", skippedTableName, dedupMissCount)
	} else {
		s.T().Logf("Metric daisi_cdc_consumer_deduplication_checks_total{table=\"%s\", result=\"miss\"} not found, which is correct.", skippedTableName)
	}

	dedupHitCount, err := getMetricValue(s.T(), metricsBody, "daisi_cdc_consumer_deduplication_checks_total", map[string]string{"table": skippedTableName, "result": "hit"})
	if err == nil {
		s.Assert().Equal(0.0, dedupHitCount, "Expected 0 dedup hit for table '%s', got %.f", skippedTableName, dedupHitCount)
	} else {
		s.T().Logf("Metric daisi_cdc_consumer_deduplication_checks_total{table=\"%s\", result=\"hit\"} not found, which is correct.", skippedTableName)
	}

	// Metric: cdc_consumer_events_published_total should not be incremented for any subject related to this event.
	// This is harder to assert definitively without knowing a potential (but wrong) subject.
	// The earlier check for no message on wa_stream is the primary validation for this.

	s.T().Log("TestSkippedTableHandling completed successfully.")
}

// TestPublishFailuresAndRedelivery commented, Fail to simulate publish failures, JetStream redeliveries, and handling after MaxDeliver is reached.
// TestPublishFailuresAndRedelivery tests NATS publish failures, JetStream redeliveries,
// and handling after MaxDeliver is reached.
// func (s *IntegrationTestSuite) TestPublishFailuresAndRedelivery() {
// 	s.T().Log("Running TestPublishFailuresAndRedelivery...")

// 	const testTableName = "messages"
// 	maxDeliver := s.appContainer.Cfg.GetInt(config.KeyJSMaxDeliver)
// 	if maxDeliver == 0 {
// 		maxDeliver = 3 // Default from ingest.go if not set
// 	}
// 	ackWait := s.appContainer.Cfg.GetDuration(config.KeyJSAckWait)
// 	if ackWait == 0 {
// 		ackWait = 30 * time.Second // Default from NATS client
// 	}
// 	s.T().Logf("Test will expect up to %d delivery attempts for the failing message. AckWait: %s", maxDeliver, ackWait)

// 	// 1. Publish a first event successfully to ensure baseline and get stream state
// 	s.T().Log("Publishing a baseline successful event...")
// 	baseCompanyID := "test-company-redelivery-base"
// 	baseMessagePK := fmt.Sprintf("msg_base_redeliver_%s", uuid.NewString()[0:8])
// 	baseNow := time.Now().UTC()
// 	baseSampleMessageData := domain.MessageData{
// 		ID:        baseNow.UnixNano()/1000 + 3000,
// 		MessageID: baseMessagePK,
// 		ChatID:    "chat_base_redeliver_123",
// 		AgentID:   "agent_base_redeliver_abc",
// 		CompanyID: baseCompanyID,
// 		Status:    "delivered",
// 	}
// 	baseRecordData := s.mustMarshalToMap(baseSampleMessageData)
// 	baseCommitLSN := baseNow.UnixNano() + 3000
// 	baseCDCEvent := domain.CDCEventData{
// 		Record: baseRecordData, Action: "insert",
// 		Metadata:  createTestMetadata(testTableName, baseCommitLSN, baseNow),
// 		PK:        map[string]interface{}{"id": baseSampleMessageData.ID},
// 		TypedData: &baseSampleMessageData,
// 	}
// 	baseCdcPublishSubject := fmt.Sprintf("sequin.changes.test_db.public.%s.insert", testTableName)
// 	err := publishCDCEvent(s.T(), s, baseCdcPublishSubject, baseCDCEvent)
// 	s.Require().NoError(err, "Failed to publish baseline CDC event")
// 	_, err = s.waitForPublishedWaMessage(10*time.Second, func(msgData []byte) bool {
// 		var p domain.EnrichedEventPayload
// 		_ = json.Unmarshal(msgData, &p)
// 		return p.MessageID == baseMessagePK
// 	})
// 	s.Require().NoError(err, "Baseline message not received on wa_stream")
// 	s.T().Log("Baseline successful event processed.")

// 	// Capture initial metrics state AFTER baseline event for relevant counters
// 	initialMetricsBody := s.fetchMetrics(s.T())
// 	failCompanyID := "test-company-redelivery-fail"
// 	failAgentID := "agent_redeliver_fail_def"
// 	failChatID := "chat_redeliver_fail_456"
// 	expectedFailingPublishSubject := fmt.Sprintf("websocket.%s.%s.%s.%s", failCompanyID, failAgentID, testTableName, failChatID)

// 	initialPublishFailureCountNATS, _ := getMetricValue(s.T(), initialMetricsBody, "cdc_consumer_events_published_total", map[string]string{"status": "failure", "subject": expectedFailingPublishSubject})
// 	initialProcessErrorCountNATS, _ := getMetricValue(s.T(), initialMetricsBody, "cdc_consumer_events_total", map[string]string{"table": testTableName, "result": "publish_error"})

// 	// 2. Configure wa_stream to be "full" to reject later publish attempts
// 	s.T().Log("Configuring wa_stream to be full to induce publish failures...")
// 	waStreamName := s.appContainer.Cfg.GetString(config.KeyJSWebsocketStreamName)
// 	jsCtx, err := s.waNatsConn.JetStream()
// 	s.Require().NoError(err, "Failed to get JetStream context on waNatsConn for stream config")

// 	streamInfoAfterBaseline, err := jsCtx.StreamInfo(waStreamName)
// 	s.Require().NoError(err, "Failed to get current info for stream: %s", waStreamName)
// 	originalWaStreamConfig := streamInfoAfterBaseline.Config

// 	configToMakeStreamFull := streamInfoAfterBaseline.Config
// 	configToMakeStreamFull.MaxMsgs = int64(streamInfoAfterBaseline.State.Msgs)
// 	configToMakeStreamFull.Retention = natsIO.LimitsPolicy
// 	configToMakeStreamFull.Discard = natsIO.DiscardNew

// 	_, err = jsCtx.UpdateStream(&configToMakeStreamFull)
// 	s.Require().NoError(err, "Failed to update stream '%s' to be full", waStreamName)
// 	s.T().Logf("Updated stream '%s' to MaxMsgs=%d, Retention=%s, Discard=%s. Stream should now reject new messages.",
// 		waStreamName, configToMakeStreamFull.MaxMsgs, configToMakeStreamFull.Retention, configToMakeStreamFull.Discard)
// 	s.T().Log("Pausing for 5 seconds for wa_stream configuration to propagate...")
// 	time.Sleep(5 * time.Second)

// 	// Drain any residual messages from WA channel before the main test part
// 	s.T().Log("Draining any pending WA messages before publishing failing event...")
// 	for len(s.receivedWaMessages) > 0 {
// 		<-s.receivedWaMessages
// 	}
// 	s.T().Log("WA message channel drained.")

// 	// Defer restoration of original wa_stream config
// 	defer func() {
// 		s.T().Logf("Defer: Restoring original config for stream '%s'", waStreamName)
// 		currentInfo, infoErr := jsCtx.StreamInfo(waStreamName)
// 		if infoErr == natsIO.ErrStreamNotFound { // If stream was deleted somehow (shouldn't be by this test logic)
// 			s.T().Logf("Defer: Stream %s not found, attempting to re-add with original config.", waStreamName)
// 			if _, addErr := jsCtx.AddStream(&originalWaStreamConfig); addErr != nil {
// 				s.T().Logf("ERROR in Defer: Failed to re-add stream %s: %v", waStreamName, addErr)
// 			}
// 		} else if infoErr != nil {
// 			s.T().Logf("ERROR in Defer: Could not get stream info for %s to restore: %v", waStreamName, infoErr)
// 		} else {
// 			configToRestore := currentInfo.Config
// 			configToRestore.MaxMsgs = originalWaStreamConfig.MaxMsgs
// 			configToRestore.Retention = originalWaStreamConfig.Retention
// 			configToRestore.Discard = originalWaStreamConfig.Discard
// 			configToRestore.Subjects = originalWaStreamConfig.Subjects // Ensure subjects are also restored
// 			if _, updateErr := jsCtx.UpdateStream(&configToRestore); updateErr != nil {
// 				s.T().Logf("ERROR in Defer: Failed to restore stream %s to original config: %v", waStreamName, updateErr)
// 			} else {
// 				s.T().Logf("Defer: Successfully restored stream %s to original configuration.", waStreamName)
// 			}
// 		}
// 	}()

// 	// 3. Publish the event that will eventually fail to be processed correctly
// 	s.T().Log("Publishing the 'failing' CDC event to the CDC stream (sequin.changes)...")
// 	failMessagePK := fmt.Sprintf("msg_fail_redeliver_%s", uuid.NewString()[0:8])
// 	failNow := time.Now().UTC()
// 	failSampleMessageData := domain.MessageData{
// 		ID:        failNow.UnixNano()/1000 + 4000,
// 		MessageID: failMessagePK, ChatID: failChatID, AgentID: failAgentID, CompanyID: failCompanyID, Status: "pending_nats_outage",
// 	}
// 	failRecordData := s.mustMarshalToMap(failSampleMessageData)
// 	failCommitLSN := failNow.UnixNano() + 4000
// 	failCDCEvent := domain.CDCEventData{
// 		Record: failRecordData, Action: "insert",
// 		Metadata:  createTestMetadata(testTableName, failCommitLSN, failNow),
// 		PK:        map[string]interface{}{"id": failSampleMessageData.ID},
// 		TypedData: &failSampleMessageData,
// 	}
// 	failCdcPublishSubject := fmt.Sprintf("sequin.changes.test_db.public.%s.insert", testTableName)

// 	err = publishCDCEvent(s.T(), s, failCdcPublishSubject, failCDCEvent) // Published to CDC Stream
// 	s.Require().NoError(err, "Failed to publish the 'failing' CDC event to the CDC stream")
// 	s.T().Logf("Successfully published 'failing' event (LSN: %d, MessageID: %s) to CDC stream.", failCommitLSN, failMessagePK)

// 	// Give a very brief moment for the app ingester to potentially pick up the message
// 	// This is a bit racy but aims to have the message in-flight within the app when NATS goes down.
// 	time.Sleep(1 * time.Second)

// 	// 4. Stop NATS container to simulate outage during app's publish attempt to wa_stream
// 	s.T().Log("Stopping NATS container to simulate outage...")
// 	stopCtx, stopCancel := context.WithTimeout(s.ctx, 10*time.Second)
// 	defer stopCancel()
// 	err = s.natsContainer.Stop(stopCtx, nil) // Allow some time for graceful stop
// 	s.Require().NoError(err, "Failed to stop NATS container")
// 	s.T().Log("NATS container stopped.")

// 	// Defer restarting NATS container to ensure it comes back up
// 	defer func() {
// 		s.T().Log("Defer: Ensuring NATS container is started...")
// 		// Check if it's already running (e.g. if test panicked before explicit restart)
// 		state, stateErr := s.natsContainer.State(s.ctx)
// 		if stateErr == nil && state.Running {
// 			s.T().Log("Defer: NATS container already running.")
// 			return
// 		}
// 		startCtx, startCancel := context.WithTimeout(s.ctx, 30*time.Second)
// 		defer startCancel()
// 		if err := s.natsContainer.Start(startCtx); err != nil {
// 			s.T().Logf("ERROR in Defer: Failed to restart NATS container: %v", err)
// 			// If NATS doesn't restart, subsequent tests or cleanup might fail.
// 			// Depending on test runner, this might not halt everything.
// 		} else {
// 			s.T().Log("Defer: NATS container started successfully.")
// 			// Allow a moment for NATS to be fully ready after restart
// 			time.Sleep(3 * time.Second)
// 		}
// 	}()

// 	// 5. Wait for application to attempt publishing (and fail due to NATS down)
// 	// CDC message for failCDCEvent will be NACKed or AckWait will expire.
// 	s.T().Logf("Waiting for %s for app to attempt processing and NACK during NATS outage...", (ackWait + 5*time.Second).String())
// 	time.Sleep(ackWait + 5*time.Second) // Wait for AckWait + buffer

// 	// 6. Restart NATS container
// 	s.T().Log("Restarting NATS container...")
// 	startCtx, startCancel := context.WithTimeout(s.ctx, 30*time.Second)
// 	defer startCancel()
// 	err = s.natsContainer.Start(startCtx)
// 	s.Require().NoError(err, "Failed to restart NATS container")
// 	s.T().Log("NATS container restarted. Allowing 5s for app to reconnect...")
// 	time.Sleep(5 * time.Second) // Give app components time to reconnect

// 	// 7. Observe Redeliveries. App ingester gets failCDCEvent again.
// 	// App tries to publish to wa_stream, which is STILL FULL.
// 	// This should lead to maxDeliver publish failures to wa_stream.
// 	s.T().Logf("Verifying 'failing' message (MessageID: %s) does NOT appear on wa_stream after NATS restart & redeliveries (wa_stream is full)...", failMessagePK)
// 	// Timeout should account for maxDeliver * (AckWait for CDC + processing time + app's internal publish retry logic if any)
// 	// This can be long. For CI, AckWait on CDC consumer should be low.
// 	redeliveryTimeout := time.Duration(maxDeliver+1) * (ackWait + 10*time.Second) // +1 for buffer, 10s for processing/publish attempt
// 	s.T().Logf("Setting wa_stream receive timeout to %s for redelivery observation.", redeliveryTimeout)

// 	_, err = s.waitForPublishedWaMessage(redeliveryTimeout, func(msgData []byte) bool {
// 		var p domain.EnrichedEventPayload
// 		_ = json.Unmarshal(msgData, &p)
// 		return p.MessageID == failMessagePK // If it appears, it's a test failure
// 	})
// 	s.Error(err, "Expected a timeout waiting for the 'failing' message (MessageID: %s) on wa_stream, as all publish attempts should fail (NATS outage then full stream).", failMessagePK)
// 	if err != nil {
// 		s.Contains(err.Error(), "timed out waiting for message", "Error should indicate timeout for the failing message on wa_stream")
// 		s.T().Logf("Correctly did not receive 'failing' message (MessageID: %s) on wa_stream.", failMessagePK)
// 	} else {
// 		s.FailNowf("Message (MessageID: %s) unexpectedly appeared on wa_stream.", failMessagePK)
// 	}

// 	// 8. Verify Prometheus Metrics
// 	s.T().Log("Verifying Prometheus metrics after simulated NATS outage and redeliveries to full stream...")
// 	// Wait a bit longer to ensure all retries and metric updates have occurred.
// 	time.Sleep(5 * time.Second)
// 	finalMetricsBody := s.fetchMetrics(s.T())

// 	// Metric: cdc_consumer_events_published_total{status="failure"} for expectedFailingPublishSubject
// 	// This counts failures from the application's publisher trying to publish to wa_stream.
// 	// Expected to increment by `maxDeliver` due to redeliveries hitting the full wa_stream.
// 	// Failures during NATS outage might also be logged by publisher if it has retry logic, but
// 	// the primary count here is for when NATS is UP but wa_stream is full.
// 	finalPublishFailureCount, err := getMetricValue(s.T(), finalMetricsBody, "cdc_consumer_events_published_total", map[string]string{"status": "failure", "subject": expectedFailingPublishSubject})
// 	s.Require().NoError(err, "Failed to get cdc_consumer_events_published_total{status=failure} for subject %s", expectedFailingPublishSubject)
// 	deltaPublishFailures := finalPublishFailureCount - initialPublishFailureCountNATS
// 	s.Assert().Equal(float64(maxDeliver), deltaPublishFailures,
// 		"Expected %d publish failures to wa_stream (subject: %s) due to full stream on redeliveries, got %.f (delta)", maxDeliver, expectedFailingPublishSubject, deltaPublishFailures)
// 	s.T().Logf("Metric cdc_consumer_events_published_total{status=failure, subject=%s} incremented by %.f", expectedFailingPublishSubject, deltaPublishFailures)

// 	// Metric: cdc_consumer_events_total{table="messages", result="publish_error"}
// 	// This counts events that failed processing at the consumer level due to publish errors.
// 	// Should also increment by `maxDeliver` as each redelivered CDC event leads to a publish error.
// 	finalProcessErrorCount, err := getMetricValue(s.T(), finalMetricsBody, "cdc_consumer_events_total", map[string]string{"table": testTableName, "result": "publish_error"})
// 	s.Require().NoError(err, "Failed to get cdc_consumer_events_total{result=publish_error} for table %s", testTableName)
// 	deltaProcessErrors := finalProcessErrorCount - initialProcessErrorCountNATS
// 	s.Assert().Equal(float64(maxDeliver), deltaProcessErrors,
// 		"Expected %d 'publish_error' results for table %s on redeliveries, got %.f (delta)", maxDeliver, testTableName, deltaProcessErrors)
// 	s.T().Logf("Metric cdc_consumer_events_total{table=%s, result=publish_error} incremented by %.f", testTableName, deltaProcessErrors)

// 	s.T().Log("TestPublishFailuresAndRedelivery with NATS stop/start strategy completed.")
// }

// mustMarshalToMap is a helper for tests to marshal a struct to JSON and then unmarshal to map[string]interface{}
func (s *IntegrationTestSuite) mustMarshalToMap(data interface{}) map[string]interface{} {
	s.T().Helper()
	jsonData, err := json.Marshal(data)
	s.Require().NoError(err, "mustMarshalToMap: Failed to marshal data to JSON")
	var mapData map[string]interface{}
	err = json.Unmarshal(jsonData, &mapData)
	s.Require().NoError(err, "mustMarshalToMap: Failed to unmarshal JSON to map")
	return mapData
}

// assertRowDataField is a helper to assert values in the RowData map, handling potential type differences.
func (s *IntegrationTestSuite) assertRowDataField(rowData map[string]interface{}, key string, expectedValue interface{}) {
	s.T().Helper()
	actualValue, ok := rowData[key]
	if !ok {
		s.Failf("RowData assertion failed", "Key '%s' not found in RowData", key)
		return
	}

	// Handle float64 that might have been int64/int32 etc.
	if expectedFloat, ok := expectedValue.(float64); ok {
		if actualFloat, ok := actualValue.(float64); ok {
			s.Equal(expectedFloat, actualFloat, "Mismatch for key '%s'. Expected: %v (%T), Actual: %v (%T)", key, expectedValue, expectedValue, actualValue, actualValue)
			return
		}
		// If actualValue is int, convert to float64 for comparison
		if actualInt, ok := actualValue.(int); ok {
			s.Equal(expectedFloat, float64(actualInt), "Mismatch for key '%s'. Expected: %v (%T), Actual: %v (%T) (actual was int)", key, expectedValue, expectedValue, actualValue, actualValue)
			return
		}
		if actualInt64, ok := actualValue.(int64); ok {
			s.Equal(expectedFloat, float64(actualInt64), "Mismatch for key '%s'. Expected: %v (%T), Actual: %v (%T) (actual was int64)", key, expectedValue, expectedValue, actualValue, actualValue)
			return
		}
	}

	// Handle string comparison for dates or other specific string fields directly
	if expectedStr, ok := expectedValue.(string); ok {
		if actualStr, ok := actualValue.(string); ok {
			s.Equal(expectedStr, actualStr, "Mismatch for key '%s'. Expected: %s, Actual: %s", key, expectedStr, actualStr)
			return
		}
	}

	// Default deep equality check for other types (like bool, or if types already match)
	s.Equal(expectedValue, actualValue, "Mismatch for key '%s'. Expected: %v (%T), Actual: %v (%T)", key, expectedValue, expectedValue, actualValue, actualValue)
}

// Helper to create metadata for tests, reducing duplication
func createTestMetadata(tableName string, commitLSN int64, ts time.Time) struct {
	TableSchema            string                 `json:"table_schema"`
	TableName              string                 `json:"table_name"`
	CommitTimestamp        string                 `json:"commit_timestamp"`
	CommitLSN              int64                  `json:"commit_lsn"`
	IdempotencyKey         string                 `json:"idempotency_key"`
	TransactionAnnotations map[string]interface{} `json:"transaction_annotations,omitempty"`
	Sink                   struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	} `json:"sink"`
} {
	return struct {
		TableSchema            string                 `json:"table_schema"`
		TableName              string                 `json:"table_name"`
		CommitTimestamp        string                 `json:"commit_timestamp"`
		CommitLSN              int64                  `json:"commit_lsn"`
		IdempotencyKey         string                 `json:"idempotency_key"`
		TransactionAnnotations map[string]interface{} `json:"transaction_annotations,omitempty"`
		Sink                   struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		} `json:"sink"`
	}{
		TableSchema:     "public",
		TableName:       tableName,
		CommitTimestamp: ts.Format(time.RFC3339Nano),
		CommitLSN:       commitLSN,
		IdempotencyKey:  uuid.New().String(),
		Sink: struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		}{ID: "test-sink-id", Name: "test-sequin-sink"},
	}
}

// TestRunIntegrationSuite is the entry point for running the suite.
func TestRunIntegrationSuite(t *testing.T) {
	suite.Run(t, new(IntegrationTestSuite))
}
