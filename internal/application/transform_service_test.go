package application_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	"gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/application"
	"gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/domain"
)

var (
	dbName           = "test-company"
	schemaName       = "public"
	lsnVal     int64 = 1632321
	companyID        = "test-company"
	agentID          = "agent-123"
	chatID           = "chat-456"
	messageID        = "msg-789"

	baseRecordMessages = map[string]interface{}{
		"agent_id":     agentID,
		"chat_id":      chatID,
		"message_id":   messageID,
		"text":         "Hello world",
		"company_id":   companyID,
		"message_date": "2025-05-17",
	}

	validCDCEventMessages = domain.CDCEventData{
		Action: "I",
		Record: baseRecordMessages,
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
			TableName:       "messages",
			CommitLSN:       lsnVal,
			TableSchema:     schemaName,
			CommitTimestamp: "2024-05-24T10:00:00Z",
			IdempotencyKey:  "idemp-key-msg",
			Sink: struct {
				ID   string `json:"id"`
				Name string `json:"name"`
			}{"sink-id-123", "test-sink"},
		},
	}
)

func TestTransformService_TransformAndEnrich(t *testing.T) {
	json := jsoniter.ConfigFastest

	baseRecordChats := map[string]interface{}{
		"agent_id":   agentID,
		"chat_id":    chatID,
		"chat_name":  "Support Chat",
		"company_id": companyID,
	}
	baseRecordAgents := map[string]interface{}{
		"agent_id":       agentID,
		"company_id":     companyID,
		"agent_name_old": "John Doe",
	}

	validCDCEventChats := domain.CDCEventData{
		Action: "I",
		Record: baseRecordChats,
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
			TableName:       "chats",
			CommitLSN:       lsnVal,
			TableSchema:     schemaName,
			CommitTimestamp: "2024-05-17T18:35:38.696512Z",
			IdempotencyKey:  "idemp-key-chat",
			Sink: struct {
				ID   string "json:\"id\""
				Name string "json:\"name\""
			}{"sink-id-123", "test-sink"},
		},
	}

	validCDCEventAgents := domain.CDCEventData{
		Action: "I",
		Record: baseRecordAgents,
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
			TableName:       "agents",
			CommitLSN:       lsnVal,
			TableSchema:     schemaName,
			CommitTimestamp: "2024-05-17T18:35:38.696512Z",
			IdempotencyKey:  "idemp-key-agent",
			Sink: struct {
				ID   string "json:\"id\""
				Name string "json:\"name\""
			}{"sink-id-123", "test-sink"},
		},
	}

	tests := []struct {
		name                         string
		subject                      string
		cdcEvent                     domain.CDCEventData
		setupMocks                   func(mockLogger *mockLogger, mockMetrics *mockMetricsSink)
		expectedEnrichedPayloadCheck func(t *testing.T, payload *domain.EnrichedEventPayload, cdcEventDataOriginalRecord map[string]interface{}, expectedCompanyID string, expectedMessageID string)
		expectedTargetSubject        string
		expectedEventID              string
		expectedError                error
	}{
		{
			name:     "Happy Path - messages table",
			subject:  fmt.Sprintf("sequin.changes.%s.%s.messages.insert", dbName, schemaName),
			cdcEvent: validCDCEventMessages,
			setupMocks: func(mockLogger *mockLogger, mockMetrics *mockMetricsSink) {
				// Expect warning about unhandled fields
				mockLogger.On("Warn",
					mock.Anything, // context
					"Unhandled fields detected in CDC record",
					mock.Anything, // fields
				).Return()

				// Add expectations for metric increments for unhandled fields
				mockMetrics.On("IncUnhandledFieldsTotal", "messages", "text").Return().Once()
			},
			expectedEnrichedPayloadCheck: func(t *testing.T, payload *domain.EnrichedEventPayload, cdcEventDataOriginalRecord map[string]interface{}, expectedCompanyID string, expectedMessageID string) {
				assert.NotNil(t, payload)
				assert.NotEmpty(t, payload.EventID)
				assert.Equal(t, agentID, payload.AgentID)
				assert.Equal(t, chatID, payload.ChatID)
				assert.Equal(t, cdcEventDataOriginalRecord, payload.RowData)
				assert.Equal(t, expectedCompanyID, payload.RowData["company_id"])
				assert.Equal(t, expectedMessageID, payload.RowData["message_id"])
			},
			expectedTargetSubject: fmt.Sprintf("wa.%s.%s.messages.%s", companyID, agentID, chatID),
			expectedEventID:       fmt.Sprintf("%d:messages:%s", lsnVal, messageID),
			expectedError:         nil,
		},
		{
			name:     "Happy Path - chats table",
			subject:  fmt.Sprintf("sequin.changes.%s.%s.chats.insert", dbName, schemaName),
			cdcEvent: validCDCEventChats,
			setupMocks: func(mockLogger *mockLogger, mockMetrics *mockMetricsSink) {
				// Expect warning about unhandled fields
				mockLogger.On("Warn",
					mock.Anything, // context
					"Unhandled fields detected in CDC record",
					mock.Anything, // fields
				).Return()

				// Add expectations for metric increments for unhandled fields
				mockMetrics.On("IncUnhandledFieldsTotal", "chats", "chat_name").Return().Once()
			},
			expectedEnrichedPayloadCheck: func(t *testing.T, payload *domain.EnrichedEventPayload, cdcEventDataOriginalRecord map[string]interface{}, expectedCompanyID string, expectedMessageID string) {
				assert.NotNil(t, payload)
				assert.Equal(t, agentID, payload.AgentID)
				assert.Equal(t, chatID, payload.ChatID)
				assert.Equal(t, cdcEventDataOriginalRecord, payload.RowData)
				assert.Equal(t, expectedCompanyID, payload.RowData["company_id"])
			},
			expectedTargetSubject: fmt.Sprintf("wa.%s.%s.chats", companyID, agentID),
			expectedEventID:       fmt.Sprintf("%d:chats:%s", lsnVal, chatID),
			expectedError:         nil,
		},
		{
			name:     "Happy Path - agents table",
			subject:  fmt.Sprintf("sequin.changes.%s.%s.agents.insert", dbName, schemaName),
			cdcEvent: validCDCEventAgents,
			setupMocks: func(mockLogger *mockLogger, mockMetrics *mockMetricsSink) {
				// Expect warning about unhandled fields
				mockLogger.On("Warn",
					mock.Anything, // context
					"Unhandled fields detected in CDC record",
					mock.Anything, // fields
				).Return()

				// Add expectations for metric increments for unhandled fields
				mockMetrics.On("IncUnhandledFieldsTotal", "agents", "agent_name_old").Return().Once()
			},
			expectedEnrichedPayloadCheck: func(t *testing.T, payload *domain.EnrichedEventPayload, cdcEventDataOriginalRecord map[string]interface{}, expectedCompanyID string, expectedMessageID string) {
				assert.NotNil(t, payload)
				assert.Equal(t, agentID, payload.AgentID)
				assert.Equal(t, "", payload.ChatID)
				assert.Equal(t, cdcEventDataOriginalRecord, payload.RowData)
				assert.Equal(t, expectedCompanyID, payload.RowData["company_id"])
			},
			expectedTargetSubject: fmt.Sprintf("wa.%s.%s.agents", companyID, agentID),
			expectedEventID:       fmt.Sprintf("%d:agents:%s", lsnVal, agentID),
			expectedError:         nil,
		},
		{
			name:    "Happy Path - with unhandled fields",
			subject: fmt.Sprintf("sequin.changes.%s.%s.messages.insert", dbName, schemaName),
			cdcEvent: func() domain.CDCEventData {
				event := validCDCEventMessages
				dataWithNewField := make(map[string]interface{})
				for k, v := range baseRecordMessages {
					dataWithNewField[k] = v
				}
				dataWithNewField["new_unhandled_field"] = "some_value"
				dataWithNewField["another_unhandled"] = float64(123)
				event.Record = dataWithNewField
				return event
			}(),
			setupMocks: func(mockLogger *mockLogger, mockMetrics *mockMetricsSink) {
				mockMetrics.On("IncUnhandledFieldsTotal", "messages", "text").Return().Once()
				mockMetrics.On("IncUnhandledFieldsTotal", "messages", "new_unhandled_field").Return().Once()
				mockMetrics.On("IncUnhandledFieldsTotal", "messages", "another_unhandled").Return().Once()
				// This case should legitimately warn about unhandled fields.
				mockLogger.On("Warn",
					mock.Anything, // context
					"Unhandled fields detected in CDC record",
					mock.Anything, // fields - using Anything instead of complex matcher
				).Return()
			},
			expectedEnrichedPayloadCheck: func(t *testing.T, payload *domain.EnrichedEventPayload, cdcEventDataOriginalRecord map[string]interface{}, expectedCompanyID string, expectedMessageID string) {
				assert.NotNil(t, payload)
				assert.Equal(t, agentID, payload.AgentID)
				assert.Equal(t, chatID, payload.ChatID)
				assert.Equal(t, "some_value", payload.RowData["new_unhandled_field"])
				assert.Equal(t, expectedCompanyID, payload.RowData["company_id"])
				assert.Equal(t, expectedMessageID, payload.RowData["message_id"])
			},
			expectedTargetSubject: fmt.Sprintf("wa.%s.%s.messages.%s", companyID, agentID, chatID),
			expectedEventID:       fmt.Sprintf("%d:messages:%s", lsnVal, messageID),
			expectedError:         nil,
		},
		{
			name:    "Error - Missing PK in messages data (message_id)",
			subject: fmt.Sprintf("sequin.changes.%s.%s.messages.insert", dbName, schemaName),
			cdcEvent: func() domain.CDCEventData {
				event := validCDCEventMessages
				badData := make(map[string]interface{})
				for k, v := range baseRecordMessages {
					if k != "message_id" {
						badData[k] = v
					}
				}
				event.Record = badData
				return event
			}(),
			setupMocks: func(mockLogger *mockLogger, mockMetrics *mockMetricsSink) {
				mockLogger.On("Warn",
					mock.Anything, // context
					"Unhandled fields detected in CDC record",
					mock.Anything, // fields - using Anything instead of complex matcher
				).Return()
				mockMetrics.On("IncUnhandledFieldsTotal", "messages", "text").Return().Once()

				// Expect Error log during PK extraction failure
				mockLogger.On("Error",
					mock.Anything, // context
					"Failed to extract PK for EventID generation",
					mock.Anything, // fields
				).Return()

				// Add expectation for metrics increment
				mockMetrics.On("IncEventsTotal", "messages", "pk_extraction_error").Return()
			},
			expectedError: domain.NewErrDataProcessing("extract_pk", "messages", domain.ErrPKEmpty),
		},
		{
			name:    "Error - Record is nil",
			subject: fmt.Sprintf("sequin.changes.%s.%s.messages.insert", dbName, schemaName),
			cdcEvent: func() domain.CDCEventData {
				event := validCDCEventMessages
				event.Record = nil
				return event
			}(),
			setupMocks: func(mockLogger *mockLogger, mockMetrics *mockMetricsSink) {
				mockLogger.On("Error", mock.Anything, "Failed to populate typed data or identify unhandled fields", mock.Anything).Return()
				mockMetrics.On("IncEventsTotal", "messages", "typed_data_population_error").Return()
			},
			expectedError: domain.NewErrDataProcessing("populate_typed_data", "messages", errors.New("record is nil")),
		},
		{
			name:    "Error - Failed to marshal final payload",
			subject: fmt.Sprintf("sequin.changes.%s.%s.messages.insert", dbName, schemaName),
			cdcEvent: func() domain.CDCEventData {
				event := validCDCEventMessages
				dataCopy := make(map[string]interface{})
				for k, v := range baseRecordMessages {
					dataCopy[k] = v
				}
				dataCopy["break_json"] = make(chan int)
				event.Record = dataCopy
				return event
			}(),
			setupMocks: func(mockLogger *mockLogger, mockMetrics *mockMetricsSink) {
				mockLogger.On("Error", mock.Anything, "Failed to populate typed data or identify unhandled fields", mock.Anything).Return()
				mockMetrics.On("IncEventsTotal", "messages", "typed_data_population_error").Return()
			},
			expectedError: domain.NewErrDataProcessing("marshal_raw_record_for_typed", "messages", errors.New("json: unsupported type: chan int")),
		},
		{
			name:    "Error - Missing agent_id from messages data",
			subject: fmt.Sprintf("sequin.changes.%s.%s.messages.insert", dbName, schemaName),
			cdcEvent: func() domain.CDCEventData {
				event := validCDCEventMessages
				badData := make(map[string]interface{})
				for k, v := range baseRecordMessages {
					if k != "agent_id" {
						badData[k] = v
					}
				}
				event.Record = badData
				return event
			}(),
			setupMocks: func(mockLogger *mockLogger, mockMetrics *mockMetricsSink) {
				mockLogger.On("Warn", mock.Anything, "Unhandled fields detected in CDC record", mock.Anything).Return()
				mockMetrics.On("IncUnhandledFieldsTotal", "messages", "text").Return()
				mockLogger.On("Error", mock.Anything, "extracted agent_id is empty", mock.Anything).Return()
			},
			expectedError: domain.ErrAgentIDEmpty,
		},
		{
			name:    "Error - Missing company_id from agents data (for subject construction)",
			subject: fmt.Sprintf("sequin.changes.%s.%s.agents.insert", dbName, schemaName),
			cdcEvent: func() domain.CDCEventData {
				event := validCDCEventAgents
				badData := make(map[string]interface{})
				for k, v := range baseRecordAgents {
					if k != "company_id" {
						badData[k] = v
					}
				}
				event.Record = badData
				return event
			}(),
			setupMocks: func(mockLogger *mockLogger, mockMetrics *mockMetricsSink) {
				mockLogger.On("Warn", mock.Anything, "Unhandled fields detected in CDC record", mock.Anything).Return()
				mockMetrics.On("IncUnhandledFieldsTotal", "agents", "agent_name_old").Return()
				mockLogger.On("Error", mock.Anything, "authoritative companyID from payload is empty", mock.Anything).Return()
			},
			expectedError: domain.ErrMissingCompanyID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockLoggerInstance := new(mockLogger)
			mockMetricsInstance := new(mockMetricsSink)

			// Expect the call to log.With in NewTransformService for this specific mock instance
			mockLoggerInstance.On("With", zap.String("component", "transform_service")).Return(mockLoggerInstance)

			if tt.setupMocks != nil {
				tt.setupMocks(mockLoggerInstance, mockMetricsInstance)
			}

			svc := application.NewTransformService(mockLoggerInstance, mockMetricsInstance)
			ctx := context.Background()

			enrichedPayload, targetSubjectOut, finalPayloadBytes, err := svc.TransformAndEnrich(ctx, &tt.cdcEvent, tt.subject, tt.cdcEvent.Metadata.TableName)

			if tt.expectedError != nil {
				assert.Error(t, err)
				if !errors.Is(err, tt.expectedError) {
					var dataProcessingErr *domain.ErrDataProcessing
					if expectedDataProcessing, ok := tt.expectedError.(*domain.ErrDataProcessing); ok && errors.As(err, &dataProcessingErr) {
						assert.Equal(t, expectedDataProcessing.Stage, dataProcessingErr.Stage, "Mismatched ErrDataProcessing stage")
						assert.Equal(t, expectedDataProcessing.Table, dataProcessingErr.Table, "Mismatched ErrDataProcessing table")
					} else {
						assert.Fail(t, fmt.Sprintf("Expected error type %T or to wrap it, but got %T. Error: %v. Expected: %v", tt.expectedError, err, err, tt.expectedError))
					}
				}
				assert.Nil(t, enrichedPayload)
				assert.Empty(t, finalPayloadBytes)
				assert.Empty(t, targetSubjectOut)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, enrichedPayload)
				assert.NotEmpty(t, finalPayloadBytes)
				assert.Equal(t, tt.expectedTargetSubject, targetSubjectOut)

				if tt.expectedEnrichedPayloadCheck != nil {
					expectedCompIDForCheck := companyID
					expectedMsgIDForCheck := ""
					if tt.cdcEvent.Metadata.TableName == "messages" {
						expectedMsgIDForCheck = messageID
					}
					tt.expectedEnrichedPayloadCheck(t, enrichedPayload, tt.cdcEvent.Record, expectedCompIDForCheck, expectedMsgIDForCheck)
				}

				assert.Equal(t, tt.expectedEventID, enrichedPayload.EventID)
				assert.Equal(t, agentID, enrichedPayload.AgentID)
				if tt.cdcEvent.Metadata.TableName == "messages" || tt.cdcEvent.Metadata.TableName == "chats" {
					assert.Equal(t, chatID, enrichedPayload.ChatID)
				} else {
					assert.Empty(t, enrichedPayload.ChatID)
				}

				var unmarshalledPayload domain.EnrichedEventPayload
				err = json.Unmarshal(finalPayloadBytes, &unmarshalledPayload)
				assert.NoError(t, err, "Final payload should be valid JSON")

				if enrichedPayload.RowData != nil && unmarshalledPayload.RowData != nil {
					assert.Equal(t, enrichedPayload.RowData, unmarshalledPayload.RowData)
				}
				tempEnrichedPayload := *enrichedPayload
				tempUnmarshalledPayload := unmarshalledPayload
				tempEnrichedPayload.RowData = nil
				tempUnmarshalledPayload.RowData = nil
				assert.Equal(t, tempEnrichedPayload, tempUnmarshalledPayload)

				mockLoggerInstance.AssertExpectations(t)
				mockMetricsInstance.AssertExpectations(t)
			}
		})
	}
}
