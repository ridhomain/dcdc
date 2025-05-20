package application_test

import (
	"context"
	"time"

	"github.com/stretchr/testify/mock"
	"gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/domain"
	"go.uber.org/zap" // For zap.Field
)

// --- Mocks ---

// mockCDCEventMessage
type mockCDCEventMessage struct {
	mock.Mock
	data    []byte
	subject string
}

func (m *mockCDCEventMessage) GetData() []byte {
	args := m.Called()
	if args.Get(0) != nil {
		return args.Get(0).([]byte)
	}
	return m.data
}
func (m *mockCDCEventMessage) GetSubject() string {
	args := m.Called()
	if args.Get(0) != nil {
		return args.Get(0).(string)
	}
	return m.subject
}
func (m *mockCDCEventMessage) Ack() error {
	args := m.Called()
	return args.Error(0)
}
func (m *mockCDCEventMessage) Nack(delay time.Duration) error {
	args := m.Called(delay)
	return args.Error(0)
}

// mockConfigProvider
type mockConfigProvider struct {
	mock.Mock
}

func (m *mockConfigProvider) GetString(key string) string {
	args := m.Called(key)
	return args.String(0)
}
func (m *mockConfigProvider) GetDuration(key string) time.Duration {
	args := m.Called(key)
	if args.Get(0) == nil {
		return 0
	}
	return args.Get(0).(time.Duration)
}
func (m *mockConfigProvider) GetInt(key string) int {
	args := m.Called(key)
	return args.Int(0)
}
func (m *mockConfigProvider) GetBool(key string) bool {
	args := m.Called(key)
	return args.Bool(0)
}
func (m *mockConfigProvider) Set(key string, value interface{}) {
	m.Called(key, value)
}

// mockLogger
type mockLogger struct {
	mock.Mock
}

func (m *mockLogger) Info(ctx context.Context, msg string, fields ...zap.Field) {
	m.Called(ctx, msg, fields)
}
func (m *mockLogger) Warn(ctx context.Context, msg string, fields ...zap.Field) {
	m.Called(ctx, msg, fields)
}
func (m *mockLogger) Error(ctx context.Context, msg string, fields ...zap.Field) {
	m.Called(ctx, msg, fields)
}
func (m *mockLogger) Debug(ctx context.Context, msg string, fields ...zap.Field) {
	m.Called(ctx, msg, fields)
}
func (m *mockLogger) With(fields ...zap.Field) domain.Logger {
	interfaceFields := make([]interface{}, len(fields))
	for i, f := range fields {
		interfaceFields[i] = f
	}
	args := m.Called(interfaceFields...)
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(domain.Logger)
}

// mockDedupStore
type mockDedupStore struct {
	mock.Mock
}

func (m *mockDedupStore) IsDuplicate(ctx context.Context, eventID domain.EventID, ttl time.Duration) (bool, error) {
	args := m.Called(ctx, eventID, ttl)
	return args.Bool(0), args.Error(1)
}

// mockPublisher
type mockPublisher struct {
	mock.Mock
}

func (m *mockPublisher) Publish(ctx context.Context, subject string, data []byte) error {
	args := m.Called(ctx, subject, data)
	return args.Error(0)
}

// mockMetricsSink
type mockMetricsSink struct {
	mock.Mock
}

func (m *mockMetricsSink) IncEventsTotal(table, result string) {
	m.Called(table, result)
}
func (m *mockMetricsSink) ObserveProcessingDuration(table string, duration time.Duration) {
	m.Called(table, duration)
}
func (m *mockMetricsSink) IncPublishErrors() {
	m.Called()
}
func (m *mockMetricsSink) IncRedisHit(hit bool) {
	m.Called(hit)
}
func (m *mockMetricsSink) SetConsumerLag(lag float64) {
	m.Called(lag)
}
func (m *mockMetricsSink) IncUnhandledFieldsTotal(table, fieldName string) {
	m.Called(table, fieldName)
}
func (m *mockMetricsSink) IncEventsPublished(subject string, status string) {
	m.Called(subject, status)
}
func (m *mockMetricsSink) IncDedupCheck(table string, result string) {
	m.Called(table, result)
}

// mockEventTransformer
type mockEventTransformer struct {
	mock.Mock
}

func (m *mockEventTransformer) TransformAndEnrich(ctx context.Context, cdcEventData *domain.CDCEventData, originalSubject string, tableName string) (*domain.EnrichedEventPayload, string, []byte, error) {
	args := m.Called(ctx, cdcEventData, originalSubject, tableName)
	// Handle cases where *domain.EnrichedEventPayload might be nil
	var payload *domain.EnrichedEventPayload
	if args.Get(0) != nil {
		payload = args.Get(0).(*domain.EnrichedEventPayload)
	}
	// Handle cases where []byte might be nil
	var payloadBytes []byte
	if args.Get(2) != nil {
		payloadBytes = args.Get(2).([]byte)
	}
	return payload, args.String(1), payloadBytes, args.Error(3)
}
