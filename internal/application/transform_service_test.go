package application

import (
	"context"
	"errors"
	"fmt"
	"testing"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/domain"
)

func TestTransformService_TransformAndEnrich(t *testing.T) {
	json := jsoniter.ConfigFastest
	companyID := "test-company"
	agentID := "agent-123"
	chatID := "chat-456"
	messageID := "msg-789"
	lsnVal := "0/16B6E58"
	dbName := companyID
	schemaName := "public"

	baseRecordMessages := map[string]interface{}{
		"agent_id":   agentID,
		"chat_id":    chatID,
		"message_id": messageID,
		"text":       "Hello world",
		"company_id": companyID,
	}
	baseRecordChats := map[string]interface{}{
		"agent_id":   agentID,
		"chat_id":    chatID,
		"chat_name":  "Support Chat",
		"company_id": companyID,
	}
	baseRecordAgents := map[string]interface{}{
		"agent_id":   agentID,
		"company_id": companyID,
		"agent_name": "John Doe",
	}

	validCDCEventMessages := domain.CDCEventData{
		Action: "I",
		Record: baseRecordMessages,
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
			TableName:       "messages",
			CommitLSN:       lsnVal,
			TableSchema:     "public",
			CommitTimestamp: "2024-05-24T10:00:00Z",
			IdempotencyKey:  "idemp-key-msg",
			Sink: struct {
				ID   string "json:\"id\""
				Name string "json:\"name\""
			}{"sink-id-123", "test-sink"},
		},
	}

	validCDCEventChats := domain.CDCEventData{
		Action: "I",
		Record: baseRecordChats,
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
			TableName:       "chats",
			CommitLSN:       lsnVal,
			TableSchema:     "public",
			CommitTimestamp: "2024-05-24T10:01:00Z",
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
			CommitLSN              interface{}            `json:"commit_lsn"`
			IdempotencyKey         string                 `json:"idempotency_key"`
			TransactionAnnotations map[string]interface{} `json:"transaction_annotations,omitempty"`
			Sink                   struct {
				ID   string `json:"id"`
				Name string `json:"name"`
			} `json:"sink"`
		}{
			TableName:       "agents",
			CommitLSN:       lsnVal,
			TableSchema:     "public",
			CommitTimestamp: "2024-05-24T10:02:00Z",
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
			name:       "Happy Path - messages table",
			subject:    fmt.Sprintf("sequin.changes.%s.%s.messages.insert", dbName, schemaName),
			cdcEvent:   validCDCEventMessages,
			setupMocks: func(mockLogger *mockLogger, mockMetrics *mockMetricsSink) {},
			expectedEnrichedPayloadCheck: func(t *testing.T, payload *domain.EnrichedEventPayload, cdcEventDataOriginalRecord map[string]interface{}, expectedCompanyID string, expectedMessageID string) {
				assert.NotNil(t, payload)
				assert.Equal(t, agentID, payload.AgentID)
				assert.Equal(t, chatID, payload.ChatID)
				assert.Equal(t, cdcEventDataOriginalRecord, payload.RowData)
				assert.Equal(t, expectedCompanyID, payload.RowData["company_id"])
				assert.Equal(t, expectedMessageID, payload.RowData["message_id"])
			},
			expectedTargetSubject: fmt.Sprintf("wa.%s.%s.messages.%s", companyID, agentID, chatID),
			expectedEventID:       fmt.Sprintf("%s:messages:%s", lsnVal, messageID),
			expectedError:         nil,
		},
		{
			name:       "Happy Path - chats table",
			subject:    fmt.Sprintf("sequin.changes.%s.%s.chats.insert", dbName, schemaName),
			cdcEvent:   validCDCEventChats,
			setupMocks: func(mockLogger *mockLogger, mockMetrics *mockMetricsSink) {},
			expectedEnrichedPayloadCheck: func(t *testing.T, payload *domain.EnrichedEventPayload, cdcEventDataOriginalRecord map[string]interface{}, expectedCompanyID string, expectedMessageID string) {
				assert.NotNil(t, payload)
				assert.Equal(t, agentID, payload.AgentID)
				assert.Equal(t, chatID, payload.ChatID)
				assert.Equal(t, cdcEventDataOriginalRecord, payload.RowData)
				assert.Equal(t, expectedCompanyID, payload.RowData["company_id"])
			},
			expectedTargetSubject: fmt.Sprintf("wa.%s.%s.chats", companyID, agentID),
			expectedEventID:       fmt.Sprintf("%s:chats:%s", lsnVal, chatID),
			expectedError:         nil,
		},
		{
			name:       "Happy Path - agents table",
			subject:    fmt.Sprintf("sequin.changes.%s.%s.agents.insert", dbName, schemaName),
			cdcEvent:   validCDCEventAgents,
			setupMocks: func(mockLogger *mockLogger, mockMetrics *mockMetricsSink) {},
			expectedEnrichedPayloadCheck: func(t *testing.T, payload *domain.EnrichedEventPayload, cdcEventDataOriginalRecord map[string]interface{}, expectedCompanyID string, expectedMessageID string) {
				assert.NotNil(t, payload)
				assert.Equal(t, agentID, payload.AgentID)
				assert.Empty(t, payload.ChatID)
				assert.Equal(t, cdcEventDataOriginalRecord, payload.RowData)
				assert.Equal(t, expectedCompanyID, payload.RowData["company_id"])
			},
			expectedTargetSubject: fmt.Sprintf("wa.%s.%s.agents", companyID, agentID),
			expectedEventID:       fmt.Sprintf("%s:agents:%s", lsnVal, agentID),
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
				dataWithNewField["another_unhandled"] = 123
				event.Record = dataWithNewField
				return event
			}(),
			setupMocks: func(mockLogger *mockLogger, mockMetrics *mockMetricsSink) {
				mockMetrics.On("IncUnhandledFieldsTotal", "messages", "new_unhandled_field").Return().Once()
				mockMetrics.On("IncUnhandledFieldsTotal", "messages", "another_unhandled").Return().Once()
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
			expectedEventID:       fmt.Sprintf("%s:messages:%s", lsnVal, messageID),
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
				mockLogger.On("Error", mock.Anything, "Failed to extract PK for table messages", "table", "messages", "record", mock.Anything, "error", mock.Anything).Return()
			},
			expectedError: domain.ErrPKEmpty,
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
				mockLogger.On("Error", mock.Anything, "CDC event data record is nil or not a map", "table", "messages", "record", nil).Return()
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
				mockLogger.On("Error", mock.Anything, "Failed to marshal enriched payload", "eventID", mock.Anything, "error", mock.Anything).Return()
			},
			expectedError: domain.NewErrDataProcessing("marshal_enriched_payload", "messages", errors.New("json: unsupported type: chan int")),
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
				mockLogger.On("Error", mock.Anything, "AgentID is missing in record for table messages", "table", "messages", "record", mock.Anything).Return()
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
				mockLogger.On("Error", mock.Anything, "CompanyID is missing in record for table agents", "table", "agents", "record", mock.Anything).Return()
			},
			expectedError: domain.ErrMissingCompanyID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockLoggerInstance := new(mockLogger)
			mockMetricsInstance := new(mockMetricsSink)

			if tt.setupMocks != nil {
				tt.setupMocks(mockLoggerInstance, mockMetricsInstance)
			}

			svc := NewTransformService(mockLoggerInstance, mockMetricsInstance)
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
