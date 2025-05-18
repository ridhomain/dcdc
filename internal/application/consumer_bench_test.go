package application_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt" // For fmt.Sprintf in setup
	"testing"
	"time"

	"gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/adapters/config" // For config.KeyDedupTTL
	"gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/application"
	"gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/domain"
	"go.uber.org/zap"
	// jsoniter "github.com/json-iterator/go" // jsoniter is already available via application.json
)

// benchmarkDedupStore is a simple mock for domain.DedupStore for benchmarking.
type benchmarkDedupStore struct {
	SimulateError   bool
	ReturnDuplicate bool
	IsDuplicateFunc func(ctx context.Context, eventID domain.EventID, ttl time.Duration) (bool, error)
}

func (m *benchmarkDedupStore) IsDuplicate(ctx context.Context, eventID domain.EventID, ttl time.Duration) (bool, error) {
	if m.IsDuplicateFunc != nil {
		return m.IsDuplicateFunc(ctx, eventID, ttl)
	}
	if m.SimulateError {
		return false, domain.NewErrExternalService("mock dedup store error", errors.New("simulated error"))
	}
	return m.ReturnDuplicate, nil
}

// benchmarkPublisher is a simple mock for domain.Publisher for benchmarking.
type benchmarkPublisher struct {
	SimulateError bool
	PublishFunc   func(ctx context.Context, subject string, data []byte) error
}

func (m *benchmarkPublisher) Publish(ctx context.Context, subject string, data []byte) error {
	if m.PublishFunc != nil {
		return m.PublishFunc(ctx, subject, data)
	}
	if m.SimulateError {
		return domain.NewErrExternalService("mock publisher error", errors.New("simulated error"))
	}
	return nil
}

// benchmarkLogger is a simple mock for domain.Logger for benchmarking.
type benchmarkLogger struct{}

func (m *benchmarkLogger) Info(ctx context.Context, msg string, fields ...zap.Field)  {}
func (m *benchmarkLogger) Warn(ctx context.Context, msg string, fields ...zap.Field)  {}
func (m *benchmarkLogger) Error(ctx context.Context, msg string, fields ...zap.Field) {}
func (m *benchmarkLogger) Debug(ctx context.Context, msg string, fields ...zap.Field) {}
func (m *benchmarkLogger) With(fields ...zap.Field) domain.Logger                     { return m }

// benchmarkMetricsSink is a simple mock for domain.MetricsSink for benchmarking.
type benchmarkMetricsSink struct{}

func (m *benchmarkMetricsSink) IncEventsTotal(table, result string)                            {}
func (m *benchmarkMetricsSink) ObserveProcessingDuration(table string, duration time.Duration) {}
func (m *benchmarkMetricsSink) IncPublishErrors()                                              {}
func (m *benchmarkMetricsSink) IncRedisHit(hit bool)                                           {}
func (m *benchmarkMetricsSink) SetConsumerLag(lag float64)                                     {}
func (m *benchmarkMetricsSink) IncUnhandledFieldsTotal(table, fieldName string)                {}

// benchmarkEventTransformer is a simple mock for application.EventTransformer for benchmarking.
type benchmarkEventTransformer struct {
	SimulateError          bool
	EnrichedPayload        *domain.EnrichedEventPayload
	TargetSubject          string
	PayloadBytes           []byte
	TransformAndEnrichFunc func(ctx context.Context, cdcEventData *domain.CDCEventData, originalSubject string, tableNameOverride string) (*domain.EnrichedEventPayload, string, []byte, error)
}

func (m *benchmarkEventTransformer) TransformAndEnrich(ctx context.Context, cdcEventData *domain.CDCEventData, originalSubject string, tableNameOverride string) (*domain.EnrichedEventPayload, string, []byte, error) {
	if m.TransformAndEnrichFunc != nil {
		return m.TransformAndEnrichFunc(ctx, cdcEventData, originalSubject, tableNameOverride)
	}
	if m.SimulateError {
		return nil, "", nil, errors.New("mock transform error")
	}
	return m.EnrichedPayload, m.TargetSubject, m.PayloadBytes, nil
}

// benchmarkConfigProvider is a simple mock for domain.ConfigProvider for benchmarking.
type benchmarkConfigProvider struct {
	GetStringFunc   func(key string) string
	GetDurationFunc func(key string) time.Duration
	GetIntFunc      func(key string) int
	GetBoolFunc     func(key string) bool
}

func (m *benchmarkConfigProvider) GetString(key string) string {
	if m.GetStringFunc != nil {
		return m.GetStringFunc(key)
	}
	return ""
}
func (m *benchmarkConfigProvider) GetDuration(key string) time.Duration {
	if m.GetDurationFunc != nil {
		return m.GetDurationFunc(key)
	}
	return 0
}
func (m *benchmarkConfigProvider) GetInt(key string) int {
	if m.GetIntFunc != nil {
		return m.GetIntFunc(key)
	}
	return 0
}
func (m *benchmarkConfigProvider) GetBool(key string) bool {
	if m.GetBoolFunc != nil {
		return m.GetBoolFunc(key)
	}
	return false
}

// benchmarkCDCEventMessage is a simple mock for application.CDCEventMessage for benchmarking.
type benchmarkCDCEventMessage struct {
	DataBytes    []byte
	SubjectValue string
	AckFunc      func() error
	NackFunc     func(delay time.Duration) error
}

func (m *benchmarkCDCEventMessage) GetData() []byte    { return m.DataBytes }
func (m *benchmarkCDCEventMessage) GetSubject() string { return m.SubjectValue }
func (m *benchmarkCDCEventMessage) Ack() error {
	if m.AckFunc != nil {
		return m.AckFunc()
	}
	return nil
}
func (m *benchmarkCDCEventMessage) Nack(delay time.Duration) error {
	if m.NackFunc != nil {
		return m.NackFunc(delay)
	}
	return nil
}

// Ensure mocks implement the interfaces
var _ domain.DedupStore = (*benchmarkDedupStore)(nil)
var _ domain.Publisher = (*benchmarkPublisher)(nil)
var _ domain.Logger = (*benchmarkLogger)(nil)
var _ domain.MetricsSink = (*benchmarkMetricsSink)(nil)
var _ application.EventTransformer = (*benchmarkEventTransformer)(nil) // Assumes EventTransformer is defined in 'application' package
var _ domain.ConfigProvider = (*benchmarkConfigProvider)(nil)
var _ application.CDCEventMessage = (*benchmarkCDCEventMessage)(nil) // Assumes CDCEventMessage is defined in 'application' package

// Helper function to panic on marshal errors in benchmark setup - not for production code
func mustMarshal(v interface{}) []byte {
	bytes, err := json.Marshal(v) // Using the package-level json from consumer.go (jsoniter)
	if err != nil {
		panic(fmt.Sprintf("mustMarshal failed: %v", err))
	}
	return bytes
}

func BenchmarkConsumerProcessEvent(b *testing.B) {
	// Common setup for all sub-benchmarks
	cfgProvider := &benchmarkConfigProvider{
		GetDurationFunc: func(key string) time.Duration {
			if key == config.KeyDedupTTL { // Use actual config key
				return 5 * time.Minute
			}
			if key == config.KeyPanicGuardFailureThresholdDuration { // Add this condition
				return 1 * time.Hour // Effectively disable panic guard for benchmarks
			}
			return 0
		},
	}
	logger := &benchmarkLogger{}
	metrics := &benchmarkMetricsSink{}

	// --- Base Sample Data (used for Medium Payload and other scenarios) ---
	sampleRecordMedium := map[string]interface{}{
		"id":                float64(1),
		"message_id":        "benchMsg123-medium",
		"chat_id":           "benchChat456-medium",
		"agent_id":          "benchAgent789-medium",
		"company_id":        "benchCompany1-medium",
		"from":              "sender-medium@example.com",
		"to":                "receiver-medium@example.com",
		"jid":               "jid_example-medium@s.whatsapp.net",
		"flow":              "inbound",
		"message_obj":       map[string]interface{}{"text": "hello benchmark world - medium payload"},
		"key":               map[string]interface{}{"remoteJid": "remote_jid-medium@s.whatsapp.net", "id": "key123-medium", "fromMe": false},
		"status":            "delivered",
		"is_deleted":        false,
		"message_timestamp": time.Now().UnixMilli(),
		"message_date":      time.Now().Format("2006-01-02"),
		"created_at":        time.Now().Format(time.RFC3339Nano),
		"updated_at":        time.Now().Format(time.RFC3339Nano),
		"last_metadata":     nil,
	}
	commitLSN := int64(1234567890)
	authoritativeTableName := "messages"

	// --- Payload Size Variations ---
	sampleRecordSmall := map[string]interface{}{
		"id":          float64(2),
		"message_id":  "benchMsgSmall",
		"chat_id":     "benchChatSmall",
		"agent_id":    "benchAgentSmall",
		"company_id":  "benchCompanySmall",
		"message_obj": map[string]interface{}{"text": "hi"},
		"created_at":  time.Now().Format(time.RFC3339Nano),
	}

	sampleRecordLarge := map[string]interface{}{
		"id":         float64(3),
		"message_id": "benchMsgLarge-0123456789-0123456789-0123456789",
		"chat_id":    "benchChatLarge-0123456789-0123456789-0123456789",
		"agent_id":   "benchAgentLarge-0123456789-0123456789-0123456789",
		"company_id": "benchCompanyLarge-0123456789-0123456789-0123456789",
		"from":       "sender-large-0123456789@example.com",
		"to":         "receiver-large-0123456789@example.com",
		"jid":        "jid_example-large-0123456789-0123456789@s.whatsapp.net",
		"flow":       "inbound_with_extra_details_for_large_payload_scenario",
		"message_obj": map[string]interface{}{
			"text":            "This is a significantly larger text message for the large payload benchmark. It includes more details and a longer string to simulate real-world complex messages. We are testing jsoniter performance here.",
			"media_url":       "https://example.com/very/long/url/to/media/file.jpg",
			"caption":         "A very descriptive caption that also adds to the payload size.",
			"metadata_custom": map[string]interface{}{"key1": "value1_long_string", "key2": 12345, "key3": true, "key4": "another_long_string_value_for_testing_purposes"},
		},
		"key":               map[string]interface{}{"remoteJid": "remote_jid-large-0123456789-0123456789@s.whatsapp.net", "id": "key123-large-0123456789", "fromMe": false, "additional_key_info": "some_extra_data_in_key"},
		"status":            "read_by_recipient_and_acknowledged_on_server",
		"is_deleted":        false,
		"message_timestamp": time.Now().Add(-5 * time.Minute).UnixMilli(),
		"message_date":      time.Now().Add(-5 * time.Minute).Format("2006-01-02"),
		"created_at":        time.Now().Format(time.RFC3339Nano),
		"updated_at":        time.Now().Format(time.RFC3339Nano),
		"retries_attempted": float64(3),
		"error_code_last":   "NETWORK_TIMEOUT_001X",
		"user_properties":   map[string]string{"segment": "premium_user_segment_A", "locale": "en_US_extra_long_locale_string"},
		"forwarding_score":  float64(8),
		"last_metadata":     map[string]interface{}{"prev_status": "delivered_to_device", "delivery_timestamp": time.Now().Add(-10 * time.Minute).Format(time.RFC3339Nano)},
	}

	// Helper to create CDC data bytes and transformer data for a given record
	prepareBenchmarkData := func(record map[string]interface{}, lsn int64, tableName string) ([]byte, *domain.EnrichedEventPayload, string, []byte) {
		fixedBytes := []byte(fmt.Sprintf(`{
			"record": %s,
			"action": "insert",
			"metadata": {
				"table_schema": "public",
				"table_name": "%s",
				"commit_timestamp": "%s",
				"commit_lsn": %d,
				"idempotency_key": "idem-key-bench-%s-%d"
			}
		}`, mustMarshal(record), tableName, time.Now().Format(time.RFC3339Nano), lsn, record["message_id"], lsn))

		eventID := fmt.Sprintf("%d:%s:%s", lsn, tableName, record["message_id"])
		enrichedPayload := &domain.EnrichedEventPayload{
			EventID:   eventID,
			CompanyID: record["company_id"].(string),
			AgentID:   record["agent_id"].(string),
			ChatID:    record["chat_id"].(string), // Will panic if chat_id is not in record, ensure small/large have it or handle
			MessageID: record["message_id"].(string),
			RowData:   record,
		}
		// For small payload, chat_id might not be present directly for subject construction if not a message, adjust if necessary.
		// Assuming 'messages' table for all for simplicity here, thus expecting chat_id.
		targetSubject := fmt.Sprintf("wa.%s.%s.messages.%s",
			enrichedPayload.CompanyID,
			enrichedPayload.AgentID,
			enrichedPayload.ChatID,
		)
		payloadBytes := mustMarshal(enrichedPayload)
		return fixedBytes, enrichedPayload, targetSubject, payloadBytes
	}

	fixedSampleCDCEventDataBytesMedium, baseMockedEnrichedPayloadMedium, baseMockedTargetSubjectMedium, baseMockedPayloadBytesMedium := prepareBenchmarkData(sampleRecordMedium, commitLSN, authoritativeTableName)
	fixedSampleCDCEventDataBytesSmall, baseMockedEnrichedPayloadSmall, baseMockedTargetSubjectSmall, baseMockedPayloadBytesSmall := prepareBenchmarkData(sampleRecordSmall, commitLSN+1, authoritativeTableName)
	fixedSampleCDCEventDataBytesLarge, baseMockedEnrichedPayloadLarge, baseMockedTargetSubjectLarge, baseMockedPayloadBytesLarge := prepareBenchmarkData(sampleRecordLarge, commitLSN+2, authoritativeTableName)

	sampleNatsMsgMedium := &benchmarkCDCEventMessage{
		DataBytes:    fixedSampleCDCEventDataBytesMedium,
		SubjectValue: fmt.Sprintf("sequin.changes.db.public.%s.insert", authoritativeTableName),
		AckFunc:      func() error { return nil },
		NackFunc:     func(delay time.Duration) error { return nil },
	}
	sampleNatsMsgSmall := &benchmarkCDCEventMessage{
		DataBytes:    fixedSampleCDCEventDataBytesSmall,
		SubjectValue: fmt.Sprintf("sequin.changes.db.public.%s.insert", authoritativeTableName),
		AckFunc:      func() error { return nil },
		NackFunc:     func(delay time.Duration) error { return nil },
	}
	sampleNatsMsgLarge := &benchmarkCDCEventMessage{
		DataBytes:    fixedSampleCDCEventDataBytesLarge,
		SubjectValue: fmt.Sprintf("sequin.changes.db.public.%s.insert", authoritativeTableName),
		AckFunc:      func() error { return nil },
		NackFunc:     func(delay time.Duration) error { return nil },
	}

	ctx := context.Background()

	// --- Standard Scenarios (using Medium payload by default) ---
	// Scenario 1: Happy Path
	b.Run("HappyPath_MediumPayload", func(b *testing.B) {
		dedupStore := &benchmarkDedupStore{ReturnDuplicate: false, SimulateError: false}
		publisher := &benchmarkPublisher{SimulateError: false}
		transformer := &benchmarkEventTransformer{
			EnrichedPayload: baseMockedEnrichedPayloadMedium,
			TargetSubject:   baseMockedTargetSubjectMedium,
			PayloadBytes:    baseMockedPayloadBytesMedium,
			SimulateError:   false,
		}
		consumer := application.NewConsumer(cfgProvider, logger, dedupStore, publisher, metrics, nil /* worker Pool */, transformer)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			consumer.ProcessEvent(ctx, sampleNatsMsgMedium, sampleNatsMsgMedium.GetSubject())
		}
		b.StopTimer()
	})

	// Scenario 2: Duplicate Event
	b.Run("DuplicateEvent_MediumPayload", func(b *testing.B) {
		dedupStore := &benchmarkDedupStore{ReturnDuplicate: true, SimulateError: false} // Simulate duplicate
		publisher := &benchmarkPublisher{SimulateError: false}
		transformer := &benchmarkEventTransformer{ // Should not be called if duplicate
			EnrichedPayload: baseMockedEnrichedPayloadMedium,
			TargetSubject:   baseMockedTargetSubjectMedium,
			PayloadBytes:    baseMockedPayloadBytesMedium,
			SimulateError:   false,
		}
		consumer := application.NewConsumer(cfgProvider, logger, dedupStore, publisher, metrics, nil, transformer)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			consumer.ProcessEvent(ctx, sampleNatsMsgMedium, sampleNatsMsgMedium.GetSubject())
		}
		b.StopTimer()
	})

	// Scenario 3: DedupStore Error
	b.Run("DedupStoreError_MediumPayload", func(b *testing.B) {
		dedupStore := &benchmarkDedupStore{ReturnDuplicate: false, SimulateError: true} // Simulate dedup store error
		publisher := &benchmarkPublisher{SimulateError: false}
		transformer := &benchmarkEventTransformer{ // Should not be called if dedup store errors
			EnrichedPayload: baseMockedEnrichedPayloadMedium,
			TargetSubject:   baseMockedTargetSubjectMedium,
			PayloadBytes:    baseMockedPayloadBytesMedium,
			SimulateError:   false,
		}
		consumer := application.NewConsumer(cfgProvider, logger, dedupStore, publisher, metrics, nil, transformer)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			consumer.ProcessEvent(ctx, sampleNatsMsgMedium, sampleNatsMsgMedium.GetSubject())
		}
		b.StopTimer()
	})

	// Scenario 4: Publish Error
	b.Run("PublishError_MediumPayload", func(b *testing.B) {
		dedupStore := &benchmarkDedupStore{ReturnDuplicate: false, SimulateError: false}
		publisher := &benchmarkPublisher{SimulateError: true} // Simulate publish error
		transformer := &benchmarkEventTransformer{
			EnrichedPayload: baseMockedEnrichedPayloadMedium,
			TargetSubject:   baseMockedTargetSubjectMedium,
			PayloadBytes:    baseMockedPayloadBytesMedium,
			SimulateError:   false,
		}
		consumer := application.NewConsumer(cfgProvider, logger, dedupStore, publisher, metrics, nil, transformer)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			consumer.ProcessEvent(ctx, sampleNatsMsgMedium, sampleNatsMsgMedium.GetSubject())
		}
		b.StopTimer()
	})

	// Scenario 5: Transform Error
	b.Run("TransformError_MediumPayload", func(b *testing.B) {
		dedupStore := &benchmarkDedupStore{ReturnDuplicate: false, SimulateError: false}
		publisher := &benchmarkPublisher{SimulateError: false} // Should not be called if transform errors
		transformer := &benchmarkEventTransformer{
			SimulateError: true, // Simulate transform error
		}
		consumer := application.NewConsumer(cfgProvider, logger, dedupStore, publisher, metrics, nil, transformer)

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			consumer.ProcessEvent(ctx, sampleNatsMsgMedium, sampleNatsMsgMedium.GetSubject())
		}
		b.StopTimer()
	})

	// --- Payload Size Scenarios (Happy Path for other mocks) ---
	b.Run("PayloadSize_Small", func(b *testing.B) {
		dedupStore := &benchmarkDedupStore{ReturnDuplicate: false, SimulateError: false}
		publisher := &benchmarkPublisher{SimulateError: false}
		transformer := &benchmarkEventTransformer{
			EnrichedPayload: baseMockedEnrichedPayloadSmall,
			TargetSubject:   baseMockedTargetSubjectSmall,
			PayloadBytes:    baseMockedPayloadBytesSmall,
			SimulateError:   false,
		}
		consumer := application.NewConsumer(cfgProvider, logger, dedupStore, publisher, metrics, nil, transformer)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			consumer.ProcessEvent(ctx, sampleNatsMsgSmall, sampleNatsMsgSmall.GetSubject())
		}
		b.StopTimer()
	})

	b.Run("PayloadSize_Large", func(b *testing.B) {
		dedupStore := &benchmarkDedupStore{ReturnDuplicate: false, SimulateError: false}
		publisher := &benchmarkPublisher{SimulateError: false}
		transformer := &benchmarkEventTransformer{
			EnrichedPayload: baseMockedEnrichedPayloadLarge,
			TargetSubject:   baseMockedTargetSubjectLarge,
			PayloadBytes:    baseMockedPayloadBytesLarge,
			SimulateError:   false,
		}
		consumer := application.NewConsumer(cfgProvider, logger, dedupStore, publisher, metrics, nil, transformer)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			consumer.ProcessEvent(ctx, sampleNatsMsgLarge, sampleNatsMsgLarge.GetSubject())
		}
		b.StopTimer()
	})
}
