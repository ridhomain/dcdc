package logger

import (
	"context"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/adapters/config" // To access KeyLogLevel
	"gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/domain"
)

// Define a context key for event_id.
type contextKey string

const (
	eventIDKey contextKey = "eventID"
	// LogKeyTable is the context key for table_name used in structured logging.
	LogKeyTable contextKey = "tableName"
	// RequestIDKey is the context key for request_id used in structured logging.
	// This key is exported so other packages (like nats ingester) can use it to set the request ID in the context.
	RequestIDKey contextKey = "requestID"
)

// ZapAdapter implements the domain.Logger interface using Zap.
type ZapAdapter struct {
	baseLogger *zap.Logger
}

// NewZapAdapter creates and initializes a new domain.Logger using Zap.
// It configures the logger based on the provided ConfigProvider (for log level)
// and adds a static serviceName field to all logs.
func NewZapAdapter(cfgProvider domain.ConfigProvider, serviceName string) (domain.Logger, error) {
	logLevelStr := cfgProvider.GetString(config.KeyLogLevel) // Use the exported KeyLogLevel

	var zapLevel zapcore.Level
	if err := zapLevel.UnmarshalText([]byte(logLevelStr)); err != nil {
		// Default to InfoLevel if parsing fails, and perhaps log this with a fallback logger if available.
		// For now, we'll just default silently as the main logger isn't fully up yet.
		zapLevel = zap.InfoLevel
	}

	// Custom time encoder for UTC timestamps in RFC3339 format.
	customTimeEncoder := func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
		enc.AppendString(t.UTC().Format(time.RFC3339Nano)) // Using Nano for more precision
	}

	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "ts",
		LevelKey:       "level",
		NameKey:        "logger", // Not typically used when a single service logger is created
		CallerKey:      "caller",
		FunctionKey:    zapcore.OmitKey, // Omitting function name for brevity
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     customTimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	zapConfig := zap.Config{
		Level:             zap.NewAtomicLevelAt(zapLevel),
		Development:       false, // Set to true for more verbose, human-readable output during dev
		DisableCaller:     false,
		DisableStacktrace: false, // Enable stacktraces for errors by default
		Sampling:          nil,   // No sampling
		Encoding:          "json",
		EncoderConfig:     encoderConfig,
		OutputPaths:       []string{"stdout"},
		ErrorOutputPaths:  []string{"stderr"},
		InitialFields: map[string]interface{}{
			"service": serviceName, // Add static service field
		},
	}

	logger, err := zapConfig.Build(
		zap.AddCaller(),
		zap.AddCallerSkip(1), // Adjust caller skip to account for our wrapper
	)
	if err != nil {
		return nil, err
	}

	return &ZapAdapter{baseLogger: logger}, nil
}

// ContextWithEventID injects an eventID into the provided context.
func ContextWithEventID(ctx context.Context, eventID domain.EventID) context.Context {
	return context.WithValue(ctx, eventIDKey, eventID)
}

// getLoggerWithContextFields prepares a logger with fields derived from context.
func (z *ZapAdapter) getLoggerWithContextFields(ctx context.Context) *zap.Logger {
	if ctx == nil {
		return z.baseLogger
	}

	fields := make([]zap.Field, 0, 3) // Pre-allocate for event_id, table_name, and request_id

	if eventID, ok := ctx.Value(eventIDKey).(domain.EventID); ok && eventID != "" {
		fields = append(fields, zap.String("event_id", string(eventID)))
	}
	if tableName, ok := ctx.Value(LogKeyTable).(string); ok && tableName != "" {
		fields = append(fields, zap.String(string(LogKeyTable), tableName))
	}
	if requestID, ok := ctx.Value(RequestIDKey).(string); ok && requestID != "" {
		fields = append(fields, zap.String(string(RequestIDKey), requestID))
	}
	// Potentially add other context-derived fields here in the future

	if len(fields) > 0 {
		return z.baseLogger.With(fields...)
	}
	return z.baseLogger
}

// Info logs an informational message.
func (z *ZapAdapter) Info(ctx context.Context, msg string, fields ...zap.Field) {
	z.getLoggerWithContextFields(ctx).Info(msg, fields...)
}

// Warn logs a warning message.
func (z *ZapAdapter) Warn(ctx context.Context, msg string, fields ...zap.Field) {
	z.getLoggerWithContextFields(ctx).Warn(msg, fields...)
}

// Error logs an error message.
func (z *ZapAdapter) Error(ctx context.Context, msg string, fields ...zap.Field) {
	z.getLoggerWithContextFields(ctx).Error(msg, fields...)
}

// Debug logs a debug message.
func (z *ZapAdapter) Debug(ctx context.Context, msg string, fields ...zap.Field) {
	z.getLoggerWithContextFields(ctx).Debug(msg, fields...)
}

// With returns a new Logger instance with the provided static fields.
// Contextual fields like event_id are handled per log call.
func (z *ZapAdapter) With(fields ...zap.Field) domain.Logger {
	return &ZapAdapter{
		baseLogger: z.baseLogger.With(fields...),
	}
}

// Sync flushes any buffered log entries.
// This can be called by the application during shutdown.
func (z *ZapAdapter) Sync() error {
	return z.baseLogger.Sync()
}
