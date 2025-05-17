package application

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	jsoniter "github.com/json-iterator/go"
	"gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/adapters/config" // For config keys
	"gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/adapters/logger" // For logger constants if any, and ContextWithEventID
	"gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/domain"

	// For zap.Field type, used in mockLogger
	"go.uber.org/zap"
)

var testJson = jsoniter.ConfigFastest

// --- Mocks are now in consumer_mocks_test.go ---

// --- Test Cases for processEvent ---

func TestConsumer_processEvent_HappyPath_Messages(t *testing.T) {
	// Mock setup
	mockCfg := new(mockConfigProvider)
	mockLog := new(mockLogger)
	mockDedup := new(mockDedupStore)
	mockPub := new(mockPublisher)
	mockMetrics := new(mockMetricsSink)
	mockTransformer := new(mockEventTransformer)
	mockMsg := new(mockCDCEventMessage)

	mockLog.On("With", zap.String("component", "consumer")).Return(mockLog)

	consumer := NewConsumer(mockCfg, mockLog, mockDedup, mockPub, mockMetrics, nil, mockTransformer)

	// Test data for raw message
	companyID := "testCompany"
	dbName := companyID
	schemaName := "public"
	action := "insert"
	agentID := "agent123"
	chatID := "chat789"
	messageID := "msgABC"
	lsn := "12345"
	tableName := "messages"
	originalSubject := fmt.Sprintf("sequin.changes.%s.%s.%s.%s", dbName, schemaName, tableName, action)

	// Raw data that consumer will parse initially
	rawCDCDataRecord := map[string]interface{}{
		"id":         messageID,
		"agent_id":   agentID,
		"chat_id":    chatID,
		"company_id": companyID,
		"text":       "Hello",
	}
	cdcEventForInput := domain.CDCEventData{
		Record: rawCDCDataRecord,
		Metadata: struct {
			TableSchema            string                 `json:"table_schema"`
			TableName              string                 `json:"table_name"`
			CommitTimestamp        string                 `json:"commit_timestamp"`
			CommitLSN              interface{}            `json:"commit_lsn"`
			IdempotencyKey         string                 `json:"idempotency_key"`
			TransactionAnnotations map[string]interface{} `json:"transaction_annotations,omitempty"`
			Sink                   struct {
				ID   string `json:"id"`
				Name string `json:"name"`
			} `json:"sink"`
		}{
			TableName:       tableName,
			CommitLSN:       lsn,
			TableSchema:     "public",
			CommitTimestamp: "2024-05-24T10:00:00Z",
			IdempotencyKey:  "idemp-key-msg-consumer",
			Sink: struct {
				ID   string "json:\"id\""
				Name string "json:\"name\""
			}{"sink-id-consumer", "consumer-test-sink"},
		},
	}
	rawDataBytes, _ := testJson.Marshal(cdcEventForInput)
	mockMsg.data = rawDataBytes
	mockMsg.subject = originalSubject

	// Data expected to be returned by the transformer
	expectedTransformedEventIDStr := fmt.Sprintf("%s:%s:%s", lsn, tableName, messageID)
	expectedTargetSubject := fmt.Sprintf("wa.%s.%s.messages.%s", companyID, agentID, chatID)
	expectedEnrichedPayload := &domain.EnrichedEventPayload{
		EventID: expectedTransformedEventIDStr,
		AgentID: agentID,
		ChatID:  chatID,
		RowData: rawCDCDataRecord,
	}
	expectedPayloadBytes, _ := testJson.Marshal(expectedEnrichedPayload)

	// Mock expectations
	mockMsg.On("GetData").Return(rawDataBytes)
	mockTransformer.On("TransformAndEnrich", mock.Anything, mock.AnythingOfType("*domain.CDCEventData"), originalSubject, tableName).
		Return(expectedEnrichedPayload, expectedTargetSubject, expectedPayloadBytes, nil)

	mockCfg.On("GetDuration", config.KeyDedupTTL).Return(5 * time.Minute)
	mockDedup.On("IsDuplicate", mock.Anything, domain.EventID(expectedTransformedEventIDStr), 5*time.Minute).Return(false, nil)
	mockMetrics.On("IncRedisHit", false).Return()

	mockLog.On("Error", mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("[]zapcore.Field")).Maybe()
	mockLog.On("Warn", mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("[]zapcore.Field")).Maybe()
	mockLog.On("Info", mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("[]zapcore.Field")).Maybe()

	mockPub.On("Publish", mock.Anything, expectedTargetSubject, expectedPayloadBytes).Return(nil)

	mockMetrics.On("ObserveProcessingDuration", tableName, mock.AnythingOfType("time.Duration")).Return()
	mockMetrics.On("IncEventsTotal", tableName, "processed").Return()
	mockMsg.On("Ack").Return(nil)

	// Execute
	// Prepare context. processEvent itself will add more specific eventID and table name.
	// No need to put table name or prelim eventID in ctx for processEvent directly,
	// as processEvent extracts tableName and TransformAndEnrich provides the final eventID for logging.
	// However, the initial log in HandleCDCEvent uses ctx, so it might have prelim_event_id.
	// processEvent itself will put tableName into ctx if not present.
	initialCtx := context.WithValue(context.Background(), logger.LogKeyTable, tableName) // Simulating HandleCDCEvent a bit
	initialCtx = logger.ContextWithEventID(initialCtx, "prelimID")

	consumer.processEvent(initialCtx, mockMsg, originalSubject)

	// Assertions
	mockTransformer.AssertExpectations(t)
	mockCfg.AssertExpectations(t)
	mockDedup.AssertExpectations(t)
	mockPub.AssertExpectations(t)
	mockMetrics.AssertExpectations(t)
	mockMsg.AssertExpectations(t)
	mockLog.AssertExpectations(t)
	// mockMsg.AssertNotCalled(t, "Ack") // Removed: Ack should be called in happy path
}

func TestConsumer_processEvent_DuplicateEvent(t *testing.T) {
	mockCfg := new(mockConfigProvider)
	mockLog := new(mockLogger)
	mockDedup := new(mockDedupStore)
	mockPub := new(mockPublisher)
	mockMetrics := new(mockMetricsSink)
	mockTransformer := new(mockEventTransformer)
	mockMsg := new(mockCDCEventMessage)

	mockLog.On("With", zap.String("component", "consumer")).Return(mockLog)
	consumer := NewConsumer(mockCfg, mockLog, mockDedup, mockPub, mockMetrics, nil, mockTransformer)

	tableName := "messages"
	dbName := "testCompany"
	schemaName := "public"
	action := "insert"
	originalSubject := fmt.Sprintf("sequin.changes.%s.%s.%s.%s", dbName, schemaName, tableName, action)
	expectedTransformedEventIDStr := "lsn123:messages:pk123"
	rawCDCDataRecord := map[string]interface{}{"id": "pk123", "agent_id": "agent1", "chat_id": "chat1", "company_id": "testCompany"}
	cdcEventForInput := domain.CDCEventData{Record: rawCDCDataRecord, Metadata: struct {
		TableSchema            string                 `json:"table_schema"`
		TableName              string                 `json:"table_name"`
		CommitTimestamp        string                 `json:"commit_timestamp"`
		CommitLSN              interface{}            `json:"commit_lsn"`
		IdempotencyKey         string                 `json:"idempotency_key"`
		TransactionAnnotations map[string]interface{} `json:"transaction_annotations,omitempty"`
		Sink                   struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		} `json:"sink"`
	}{
		TableName:       tableName,
		CommitLSN:       "lsn123",
		TableSchema:     "public",
		CommitTimestamp: "2024-05-24T10:01:00Z",
		IdempotencyKey:  "idemp-key-dup",
		Sink: struct {
			ID   string "json:\"id\""
			Name string "json:\"name\""
		}{"sink-id-consumer", "consumer-test-sink"},
	}}
	rawDataBytes, _ := testJson.Marshal(cdcEventForInput)
	mockMsg.data = rawDataBytes
	mockMsg.subject = originalSubject

	// Add expectation for GetData
	mockMsg.On("GetData").Return(rawDataBytes)

	enrichedPayloadFromTransformer := &domain.EnrichedEventPayload{EventID: expectedTransformedEventIDStr, RowData: rawCDCDataRecord}
	mockTransformer.On("TransformAndEnrich", mock.Anything, mock.AnythingOfType("*domain.CDCEventData"), originalSubject, tableName).
		Return(enrichedPayloadFromTransformer, "anySubject", []byte("anyPayload"), nil)

	mockCfg.On("GetDuration", config.KeyDedupTTL).Return(5 * time.Minute)
	mockDedup.On("IsDuplicate", mock.Anything, domain.EventID(expectedTransformedEventIDStr), 5*time.Minute).Return(true, nil)
	mockMetrics.On("IncEventsTotal", tableName, "duplicate").Return()
	mockMsg.On("Ack").Return(nil)

	mockLog.On("Info", mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("[]zapcore.Field")).Maybe()

	ctx := context.Background()
	ctx = context.WithValue(ctx, logger.LogKeyTable, tableName)
	consumer.processEvent(ctx, mockMsg, originalSubject)

	mockTransformer.AssertExpectations(t)
	mockDedup.AssertExpectations(t)
	mockMetrics.AssertExpectations(t)
	mockMsg.AssertExpectations(t)
	mockPub.AssertNotCalled(t, "Publish", mock.Anything, mock.Anything, mock.Anything)
	mockLog.AssertExpectations(t)
}

func TestConsumer_processEvent_PublishError(t *testing.T) {
	mockCfg := new(mockConfigProvider)
	mockLog := new(mockLogger)
	mockDedup := new(mockDedupStore)
	mockPub := new(mockPublisher)
	mockMetrics := new(mockMetricsSink)
	mockTransformer := new(mockEventTransformer)
	mockMsg := new(mockCDCEventMessage)

	mockLog.On("With", zap.String("component", "consumer")).Return(mockLog)
	consumer := NewConsumer(mockCfg, mockLog, mockDedup, mockPub, mockMetrics, nil, mockTransformer)

	companyID := "testCompany"
	dbName := companyID
	schemaName := "public"
	action := "insert"
	agentID := "agent123"
	chatID := "chat789"
	messageID := "msgABC"
	lsn := "12345"
	tableName := "messages"
	originalSubject := fmt.Sprintf("sequin.changes.%s.%s.%s.%s", dbName, schemaName, tableName, action)

	rawCDCDataRecord := map[string]interface{}{
		"id":         messageID,
		"agent_id":   agentID,
		"chat_id":    chatID,
		"company_id": companyID,
		"text":       "Hello",
	}
	cdcEventForInput := domain.CDCEventData{Record: rawCDCDataRecord, Metadata: struct {
		TableSchema            string                 `json:"table_schema"`
		TableName              string                 `json:"table_name"`
		CommitTimestamp        string                 `json:"commit_timestamp"`
		CommitLSN              interface{}            `json:"commit_lsn"`
		IdempotencyKey         string                 `json:"idempotency_key"`
		TransactionAnnotations map[string]interface{} `json:"transaction_annotations,omitempty"`
		Sink                   struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		} `json:"sink"`
	}{
		TableName:       tableName,
		CommitLSN:       lsn,
		TableSchema:     "public",
		CommitTimestamp: "2024-05-24T10:02:00Z",
		IdempotencyKey:  "idemp-key-pub-err",
		Sink: struct {
			ID   string "json:\"id\""
			Name string "json:\"name\""
		}{"sink-id-consumer", "consumer-test-sink"},
	}}
	rawDataBytes, _ := testJson.Marshal(cdcEventForInput)
	mockMsg.data = rawDataBytes
	mockMsg.subject = originalSubject

	// Add expectation for GetData
	mockMsg.On("GetData").Return(rawDataBytes)

	expectedTransformedEventIDStr := fmt.Sprintf("%s:%s:%s", lsn, tableName, messageID)
	expectedTargetSubject := fmt.Sprintf("wa.%s.%s.messages.%s", companyID, agentID, chatID)
	enrichedPayloadFromTransformer := &domain.EnrichedEventPayload{EventID: expectedTransformedEventIDStr, AgentID: agentID, ChatID: chatID, RowData: rawCDCDataRecord}
	expectedPayloadBytes, _ := testJson.Marshal(enrichedPayloadFromTransformer)

	mockTransformer.On("TransformAndEnrich", mock.Anything, mock.AnythingOfType("*domain.CDCEventData"), originalSubject, tableName).
		Return(enrichedPayloadFromTransformer, expectedTargetSubject, expectedPayloadBytes, nil)

	mockCfg.On("GetDuration", config.KeyDedupTTL).Return(5 * time.Minute)
	mockCfg.On("GetDuration", config.KeyPanicGuardFailureThresholdDuration).Return(15 * time.Minute)
	mockDedup.On("IsDuplicate", mock.Anything, domain.EventID(expectedTransformedEventIDStr), 5*time.Minute).Return(false, nil)
	mockMetrics.On("IncRedisHit", false).Return()

	// Expect the Warn call from incrementConsecutiveFailures
	mockLog.On(
		"Warn",
		mock.Anything, // Context
		"First consecutive processing failure detected",
		mock.AnythingOfType("[]zapcore.Field"), // Variadic fields become a slice here
	).Return().Maybe() // Use Maybe if the exact fields don't need strict assertion or if call is conditional

	mockLog.On("Error", mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("[]zapcore.Field")).Maybe()
	mockLog.On("Info", mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("[]zapcore.Field")).Maybe()

	mockPub.On("Publish", mock.Anything, expectedTargetSubject, expectedPayloadBytes).
		Return(domain.NewErrExternalService("NATS_publisher", fmt.Errorf("NATS no responders")))

	mockMetrics.On("IncPublishErrors").Return()
	mockMetrics.On("IncEventsTotal", tableName, "publish_error").Return()
	mockMsg.On("Nack", time.Duration(0)).Return(nil)

	ctx := context.Background()
	ctx = context.WithValue(ctx, logger.LogKeyTable, tableName)
	consumer.processEvent(ctx, mockMsg, originalSubject)

	mockCfg.AssertExpectations(t)
	mockTransformer.AssertExpectations(t)
	mockDedup.AssertExpectations(t)
	mockPub.AssertExpectations(t)
	mockMetrics.AssertExpectations(t)
	mockMsg.AssertExpectations(t)
	mockLog.AssertExpectations(t)
	mockMsg.AssertNotCalled(t, "Ack")
}

func TestConsumer_processEvent_TransformDataError(t *testing.T) {
	mockCfg := new(mockConfigProvider)
	mockLog := new(mockLogger)
	mockDedup := new(mockDedupStore)
	mockPub := new(mockPublisher)
	mockMetrics := new(mockMetricsSink)
	mockTransformer := new(mockEventTransformer)
	mockMsg := new(mockCDCEventMessage)

	mockLog.On("With", zap.String("component", "consumer")).Return(mockLog)
	consumer := NewConsumer(mockCfg, mockLog, mockDedup, mockPub, mockMetrics, nil, mockTransformer)

	tableName := "messages"
	dbName := "testCompany"
	schemaName := "public"
	action := "insert"
	originalSubject := fmt.Sprintf("sequin.changes.%s.%s.%s.%s", dbName, schemaName, tableName, action)
	rawCDCDataRecord := map[string]interface{}{"id": "pk123", "agent_id": "agent1"} // Missing company_id and chat_id for messages
	cdcEventForInput := domain.CDCEventData{Record: rawCDCDataRecord, Metadata: struct {
		TableSchema            string                 `json:"table_schema"`
		TableName              string                 `json:"table_name"`
		CommitTimestamp        string                 `json:"commit_timestamp"`
		CommitLSN              interface{}            `json:"commit_lsn"`
		IdempotencyKey         string                 `json:"idempotency_key"`
		TransactionAnnotations map[string]interface{} `json:"transaction_annotations,omitempty"`
		Sink                   struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		} `json:"sink"`
	}{
		TableName:       tableName,
		CommitLSN:       "lsn123",
		TableSchema:     "public",
		CommitTimestamp: "2024-05-24T10:03:00Z",
		IdempotencyKey:  "idemp-key-transform-err",
		Sink: struct {
			ID   string "json:\"id\""
			Name string "json:\"name\""
		}{"sink-id-consumer", "consumer-test-sink"},
	}}
	rawDataBytes, _ := testJson.Marshal(cdcEventForInput)
	mockMsg.data = rawDataBytes
	mockMsg.subject = originalSubject

	// Add expectation for GetData
	mockMsg.On("GetData").Return(rawDataBytes)

	// Mock expectations: Transformer returns a data error
	// For example, ErrMissingCompanyID or ErrChatIDMissingForMessages could be returned by the actual transformer.
	// We'll use a generic ErrDataProcessing for this test, or a specific sentinel one.
	// The transformer itself would log the specifics and inc specific metrics.
	// The consumer will see a transformErr and decide to Ack based on its type.
	transformError := domain.ErrMissingCompanyID // Example of a specific data error
	mockTransformer.On("TransformAndEnrich", mock.Anything, mock.AnythingOfType("*domain.CDCEventData"), originalSubject, tableName).
		Return(nil, "", nil, transformError)

	mockLog.On("Error", mock.Anything, "Event transformation failed", mock.AnythingOfType("[]zapcore.Field")).Return().Once()
	mockMetrics.On("IncEventsTotal", tableName, "transform_data_error").Return().Once()
	mockMsg.On("Ack").Return(nil).Once()

	ctx := context.Background()
	ctx = context.WithValue(ctx, logger.LogKeyTable, tableName)

	consumer.processEvent(ctx, mockMsg, originalSubject)

	mockTransformer.AssertExpectations(t)
	mockMetrics.AssertExpectations(t)
	mockMsg.AssertExpectations(t)
	mockLog.AssertExpectations(t)
	// Dedup and Publish should not be called
	mockDedup.AssertNotCalled(t, "IsDuplicate", mock.Anything, mock.Anything, mock.Anything)
	mockPub.AssertNotCalled(t, "Publish", mock.Anything, mock.Anything, mock.Anything)
}

// TODO: Add more test cases for other scenarios:
// - Happy path for "chats" and "agents"
// - Unmarshal error (handled by TransformAndEnrich mock returning error)
// - PK extraction error (handled by TransformAndEnrich mock returning error)
// - agent_id missing (handled by TransformAndEnrich mock returning error)
// - chat_id missing for "messages" table (should lead to error from TransformAndEnrich)
// - TransformAndEnrich itself returns an error
// - NATS subject parsing errors (for tableName in consumer.go's processEvent before calling transformer)
// - Test interaction with metrics for various paths (skipped, duplicate, error types)

// TestExtractPKValue is removed as extractPKValue is now part of transformService and will be tested there.
