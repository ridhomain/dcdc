package application

import (
	"context"
	"errors" // Import errors package for errors.Is and errors.As
	"fmt"    // For placeholder error formatting

	// For sorting PK map keys for consistent EventID
	"strings" // Added for strings.Split
	"sync"    // Added for sync.Mutex
	"time"    // For placeholder logic

	jsoniter "github.com/json-iterator/go"                                       // Added for JSON processing
	"gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/adapters/config" // Import config package
	"gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/adapters/logger" // For logger.ContextWithEventID
	"gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/domain"

	"go.uber.org/zap" // For placeholder logging fields
)

// Expected NATS subject parts for Sequin CDC messages
const (
	expectedSubjectPrefix    = "sequin.changes"
	expectedSubjectPartCount = 5 // sequin.changes.db.schema.table.action -> 5 parts if prefix is 1 part
)

var json = jsoniter.ConfigFastest

// CDCEventMessage represents the raw message structure received from the NATS JetStream subscription.
// This is a placeholder and might be replaced by a more specific type from a NATS library,
// e.g., nats.Msg, once the NATS adapter is more fully implemented.
type CDCEventMessage interface {
	GetData() []byte
	GetSubject() string
	Ack() error
	Nack(delay time.Duration) error // Nack with a delay
	// GetMetadata() (uint64, time.Time) // Example: sequence, timestamp from JetStream
}

// Consumer orchestrates the processing of CDC events.
// It uses various domain ports to interact with external systems (config, logging, dedup, publishing, metrics).
type Consumer struct {
	configProvider   domain.ConfigProvider
	logger           domain.Logger
	dedupStore       domain.DedupStore
	publisher        domain.Publisher
	metricsSink      domain.MetricsSink
	workerPool       *WorkerPool
	eventTransformer EventTransformer

	// Fields for panic guard (Task 8)
	panicGuardMutex                   sync.Mutex
	consecutiveProcessingFailureCount uint64
	firstProcessingFailureTimestamp   *time.Time
}

// NewConsumer creates a new instance of the CDC event Consumer.
// It requires all its dependencies (domain ports and worker Pool) to be provided.
func NewConsumer(
	cfg domain.ConfigProvider,
	log domain.Logger,
	dedup domain.DedupStore,
	pub domain.Publisher,
	metrics domain.MetricsSink,
	pool *WorkerPool,
	transformer EventTransformer,
) *Consumer {
	return &Consumer{
		configProvider:   cfg,
		logger:           log.With(zap.String("component", "consumer")),
		dedupStore:       dedup,
		publisher:        pub,
		metricsSink:      metrics,
		workerPool:       pool,
		eventTransformer: transformer,
	}
}

// parseNatsSubject parses the NATS subject (e.g., sequin.changes.db.schema.table.action)
// and returns a ParsedSubjectInfo struct.
func parseNatsSubject(subject string) domain.ParsedSubjectInfo {
	parts := strings.Split(subject, ".")
	info := domain.ParsedSubjectInfo{RawSubject: subject, IsValid: false}

	// sequin.changes.database_name.schema_name.table_name.action
	// Example parts: [sequin, changes, my_db, public, orders, insert]
	// Prefix = parts[0]+"."+parts[1]
	// Correct part count should be 6 if we split by "."
	if len(parts) != 6 {
		return info // Invalid structure
	}

	info.Prefix = parts[0] + "." + parts[1]
	if info.Prefix != expectedSubjectPrefix {
		return info // Prefix doesn't match
	}

	info.DatabaseName = parts[2]
	info.SchemaName = parts[3]
	info.TableName = parts[4]
	info.Action = parts[5]

	if info.DatabaseName == "" || info.SchemaName == "" || info.TableName == "" || info.Action == "" {
		return info // One of the critical parts is empty
	}

	info.IsValid = true
	return info
}

// ProcessEvent is the actual processing logic for a single event, intended to be run in a worker goroutine.
func (c *Consumer) ProcessEvent(ctx context.Context, msg CDCEventMessage, originalSubject string) {
	parsedSubject := parseNatsSubject(originalSubject)

	if !parsedSubject.IsValid {
		c.logger.Error(ctx, "Worker: Invalid NATS subject format", zap.String("subject", originalSubject))
		c.metricsSink.IncEventsTotal("unknown_subject_format_worker", "error")
		_ = msg.Ack()
		return
	}
	// Update context with the parsed table name for logging throughout this event's processing
	ctx = context.WithValue(ctx, logger.LogKeyTable, parsedSubject.TableName)

	var cdcEventData domain.CDCEventData
	if err := json.Unmarshal(msg.GetData(), &cdcEventData); err != nil {
		c.logger.Error(ctx, "Failed to unmarshal CDC event data", zap.Error(err), zap.ByteString("raw_data", msg.GetData()))
		c.metricsSink.IncEventsTotal(parsedSubject.TableName, "unmarshal_error")
		_ = msg.Ack() // Unmarshal error is a data quality issue, Ack.
		return
	}

	if cdcEventData.Metadata.TableName != parsedSubject.TableName {
		c.logger.Warn(ctx, "Mismatch between NATS subject table and payload metadata table",
			zap.String("subject_table", parsedSubject.TableName),
			zap.String("payload_table", cdcEventData.Metadata.TableName))
	}

	// Call the EventTransformer
	// The eventID for logging context will be set based on the enrichedPayload if successful,
	// or use a preliminary one if transform fails early.
	// For now, let's assume context for logging eventID is handled based on outcome.
	enrichedPayload, targetSubject, payloadBytes, transformErr := c.eventTransformer.TransformAndEnrich(ctx, &cdcEventData, originalSubject, parsedSubject.TableName)

	if transformErr != nil {
		c.logger.Error(ctx, "Event transformation failed", zap.Error(transformErr), zap.String("table_name", parsedSubject.TableName))
		// transformService now emits specific metrics for sub-stages of transformation.
		// Decide Ack/Nack based on the nature of transformErr.
		var dataProcessingErr *domain.ErrDataProcessing
		if errors.As(transformErr, &dataProcessingErr) || // If it's a wrapped data processing error
			errors.Is(transformErr, domain.ErrMissingCompanyID) ||
			errors.Is(transformErr, domain.ErrAgentIDEmpty) ||
			errors.Is(transformErr, domain.ErrChatIDMissingForMessages) ||
			errors.Is(transformErr, domain.ErrPKEmpty) ||
			errors.Is(transformErr, domain.ErrUnknownTableNameForTransform) {
			c.metricsSink.IncEventsTotal(parsedSubject.TableName, "transform_data_error") // General metric for consumer
			_ = msg.Ack()                                                                 // Data quality/validation errors from transform are non-retryable here.
		} else {
			c.metricsSink.IncEventsTotal(parsedSubject.TableName, "transform_system_error") // Unexpected error in transform
			c.incrementConsecutiveFailures(ctx)                                             // Panic Guard: Increment
			_ = msg.Nack(0)                                                                 // Assume other transform errors might be transient/system issues.
		}
		return
	}

	// Update context with the final eventID from the successfully transformed payload for subsequent logging
	ctx = logger.ContextWithEventID(ctx, domain.EventID(enrichedPayload.EventID))

	// Deduplication (using the eventID from enrichedPayload)
	isDup, err := c.dedupStore.IsDuplicate(ctx, domain.EventID(enrichedPayload.EventID), c.configProvider.GetDuration(config.KeyDedupTTL))
	if err != nil {
		c.logger.Error(ctx, "Deduplication check failed", zap.Error(err), zap.String("event_id", enrichedPayload.EventID))
		c.metricsSink.IncEventsTotal(parsedSubject.TableName, "dedup_error")
		c.incrementConsecutiveFailures(ctx) // Panic Guard: Increment
		// Consider dedupErr an external service issue, potentially retryable.
		_ = msg.Nack(0)
		return
	}
	if isDup {
		c.logger.Info(ctx, "Duplicate event skipped", zap.String("event_id", enrichedPayload.EventID))
		c.metricsSink.IncEventsTotal(parsedSubject.TableName, "duplicate")
		// A duplicate is not a processing *failure* for the service itself, it's an expected path.
		// Thus, we do not increment failure count here, but we also don't reset it if prior ones failed.
		// If we want duplicates to *also* reset the counter, that logic would go here.
		// For now, let's assume duplicates don't affect the panic guard counter status.
		_ = msg.Ack()
		return
	}
	c.metricsSink.IncRedisHit(false)

	startTime := time.Now()

	publishErr := c.publisher.Publish(ctx, targetSubject, payloadBytes)
	if publishErr != nil {
		c.logger.Error(ctx, "Failed to publish event", zap.Error(publishErr), zap.String("target_subject", targetSubject))
		c.metricsSink.IncPublishErrors()
		c.metricsSink.IncEventsTotal(parsedSubject.TableName, "publish_error")
		c.incrementConsecutiveFailures(ctx) // Panic Guard: Increment
		// Consider publishErr an external service issue, potentially retryable.
		_ = msg.Nack(0)
		return
	}

	processingDuration := time.Since(startTime)
	c.metricsSink.ObserveProcessingDuration(parsedSubject.TableName, processingDuration)
	c.metricsSink.IncEventsTotal(parsedSubject.TableName, "processed")
	c.logger.Info(ctx, "Event processed and published successfully",
		zap.String("event_id", enrichedPayload.EventID),
		zap.String("target_subject", targetSubject),
		zap.Duration("processing_time", processingDuration),
	)

	c.logger.Info(ctx, "Attempting to ACK message", zap.String("event_id", enrichedPayload.EventID), zap.String("subject", originalSubject))
	if ackErr := msg.Ack(); ackErr != nil {
		c.logger.Error(ctx, "Failed to ACK message after successful processing", zap.Error(ackErr), zap.String("event_id", enrichedPayload.EventID))
		// If ACK fails, the message might be redelivered. This is a tricky state.
		// For the panic guard, the core processing was successful *up to the publish*.
		// An ACK failure is a separate NATS issue. We probably should still reset the counter here
		// because the service *did* successfully process and publish the event from its perspective.
		// However, if ACK failure is common and leads to NACKs implicitly by NATS, this could be an issue.
		// For now, let's assume successful processing up to this point warrants a reset.
		c.resetConsecutiveFailures(ctx) // Panic Guard: Reset on successful processing before ACK attempt
	} else {
		c.logger.Info(ctx, "Successfully ACKed message", zap.String("event_id", enrichedPayload.EventID), zap.String("subject", originalSubject))
		c.resetConsecutiveFailures(ctx) // Panic Guard: Reset on successful ACK
	}
}

// HandleCDCEvent is the primary method called by the NATS ingestion adapter
// when a new CDC event message is received.
// It performs initial quick checks and then dispatches the core processing to a worker Pool.
func (c *Consumer) HandleCDCEvent(ctx context.Context, msg CDCEventMessage) error {
	// Store original subject because msg might be mutated or its context lost in async worker
	originalSubject := msg.GetSubject()

	// Quick initial extraction of table name for early filtering and context setup
	parsedSubjectInfo := parseNatsSubject(originalSubject)

	// This eventID is temporary, just for initial logging before full parsing.
	tempEventID := domain.EventID(fmt.Sprintf("prelim_from_subject:%s", originalSubject))
	ctx = logger.ContextWithEventID(ctx, tempEventID)

	if !parsedSubjectInfo.IsValid {
		c.logger.Error(ctx, "Failed to parse NATS subject",
			zap.String("subject", originalSubject),
			zap.String("expected_format", "sequin.changes.<db>.<schema>.<table>.<action>"),
		)
		c.metricsSink.IncEventsTotal("unknown_subject_format", "error")
		if ackErr := msg.Ack(); ackErr != nil { // Ack bad subject to prevent loops
			c.logger.Error(ctx, "Failed to ACK message with invalid subject format", zap.Error(ackErr), zap.String("subject", originalSubject))
		}
		return fmt.Errorf("invalid NATS subject format: %s", originalSubject) // Return error, but message is ACKed.
	}
	// Add crucial parsed subject parts to context for logging
	ctx = context.WithValue(ctx, logger.LogKeyTable, parsedSubjectInfo.TableName)
	// TODO: Consider adding other parsedSubjectInfo parts (DatabaseName, SchemaName, Action) to context if needed for downstream logging or logic

	c.logger.Info(ctx, "Received CDC event, submitting to worker Pool", zap.ByteString("raw_data_snippet", msg.GetData()[:min(len(msg.GetData()), 64)])) // Log snippet

	// Filter based on AllowedTables before submitting to worker Pool to save resources
	if _, isAllowed := domain.AllowedTables[parsedSubjectInfo.TableName]; !isAllowed {
		c.logger.Info(ctx, "Table not allowed, skipping event before worker submission", zap.String("table_name", parsedSubjectInfo.TableName))
		c.metricsSink.IncEventsTotal(parsedSubjectInfo.TableName, "skipped")
		c.logger.Info(ctx, "Attempting to ACK skipped message for disallowed table", zap.String("subject", originalSubject), zap.String("table_name", parsedSubjectInfo.TableName))
		if ackErr := msg.Ack(); ackErr != nil {
			c.logger.Error(ctx, "Failed to ACK skipped message for disallowed table", zap.Error(ackErr), zap.String("table_name", parsedSubjectInfo.TableName))
		}
		return nil // Successfully skipped and ACKed.
	}

	// Submit the core processing to the worker Pool
	// The actual Ack/Nack will be handled within ProcessEvent
	submitErr := c.workerPool.Submit(func() {
		// Create a new context for the worker goroutine, potentially deriving from the input ctx
		// This ensures that if the outer context from NATS ingester is cancelled,
		// the worker still tries to finish its current task if appropriate,
		// or handles cancellation gracefully.
		// For now, we pass the context through. If tracing is added, new spans might start here.

		// Pass the original context (which includes requestID and other relevant values)
		// directly to ProcessEvent. ProcessEvent itself will manage its specific context values like table_name and final event_id.
		c.ProcessEvent(ctx, msg, originalSubject)
	})

	if submitErr != nil {
		c.logger.Error(ctx, "Failed to submit CDC event to worker Pool", zap.Error(submitErr))
		c.metricsSink.IncEventsTotal(parsedSubjectInfo.TableName, "worker_submit_error")
		// Check if the error is due to Pool being closed or other non-retryable submission issues.
		if errors.Is(submitErr, domain.ErrTaskSubmissionToPool) {
			// If ErrTaskSubmissionToPool implies a non-retryable state (like Pool closed for good),
			// then Ack might be appropriate. Otherwise, Nack.
			// For now, assume task submission errors are serious and message might be lost if Acked.
			// Let's Nack to indicate a system-level issue in processing the message submission.
			c.logger.Warn(ctx, "Attempting to NACK message due to worker submission failure (ErrTaskSubmissionToPool)", zap.String("subject", originalSubject), zap.Error(submitErr))
			if nackErr := msg.Nack(0); nackErr != nil {
				c.logger.Error(ctx, "Failed to NACK message after worker submission failure", zap.Error(nackErr))
			}
		} else {
			// For other types of submission errors not wrapped by our custom type.
			c.logger.Warn(ctx, "Attempting to NACK message due to generic worker submission failure", zap.String("subject", originalSubject), zap.Error(submitErr))
			if nackErr := msg.Nack(0); nackErr != nil {
				c.logger.Error(ctx, "Failed to NACK message after generic worker submission failure", zap.Error(nackErr))
			}
		}
		return submitErr // Return the original or wrapped error from Submit.
	}

	// If submission is successful, the responsibility of Ack/Nack is now with ProcessEvent.
	// HandleCDCEvent returns nil, indicating it has accepted the message for processing.
	return nil
}

// incrementConsecutiveFailures safely increments the count of consecutive processing failures.
// It sets the timestamp of the first failure in a sequence.
func (c *Consumer) incrementConsecutiveFailures(ctx context.Context) {
	c.panicGuardMutex.Lock()
	defer c.panicGuardMutex.Unlock()

	c.consecutiveProcessingFailureCount++
	if c.consecutiveProcessingFailureCount == 1 {
		now := time.Now()
		c.firstProcessingFailureTimestamp = &now
		c.logger.Warn(ctx, "First consecutive processing failure detected",
			zap.Uint64("current_consecutive_failures", c.consecutiveProcessingFailureCount),
			zap.Time("first_failure_timestamp", now),
		)
	} else {
		c.logger.Warn(ctx, "Consecutive processing failure detected",
			zap.Uint64("current_consecutive_failures", c.consecutiveProcessingFailureCount),
			zap.Timep("first_failure_timestamp", c.firstProcessingFailureTimestamp),
		)
	}

	// Check for panic guard threshold breach
	if c.firstProcessingFailureTimestamp != nil && c.consecutiveProcessingFailureCount > 0 {
		failureThresholdDuration := c.configProvider.GetDuration(config.KeyPanicGuardFailureThresholdDuration)
		if time.Since(*c.firstProcessingFailureTimestamp) > failureThresholdDuration {
			c.logger.Error(ctx, // Use Error for immediate visibility before Fatal
				"PANIC GUARD: Consecutive processing failures exceeded threshold. Triggering shutdown.",
				zap.Uint64("consecutive_failures", c.consecutiveProcessingFailureCount),
				zap.Time("first_failure_at", *c.firstProcessingFailureTimestamp),
				zap.Duration("threshold_duration", failureThresholdDuration),
			)
			// Using Fatal will cause the program to exit after logging this message.
			// This relies on the orchestrator (e.g., Kubernetes) to restart the pod.
			c.logger.Info(ctx, "Intentionally calling log.Fatal to trigger pod restart due to panic guard.") // Info before fatal
			// In a real scenario, you might want to ensure all logs are flushed before os.Exit or logger.Fatal
			// Zap's Fatal logs and then calls os.Exit(1).
			// For now, directly using logger.Error and then a separate log.Fatal call via a panic or os.Exit might be cleaner
			// or just rely on zap.Fatal. Let's use zap.Fatal directly as it's concise.
			// However, zap.Fatal might not be available on the domain.Logger interface. Let's rethink.

			// The domain.Logger interface does not have Fatal().
			// We should log a critical error and then cause a panic, which the main goroutine can recover
			// and then decide to os.Exit() or use the underlying Zap logger's Fatal if accessible.
			// For simplicity and directness as per PRD "log.Fatal", we might need to ensure our ZapAdapter can expose this
			// or we handle it by panicking here and letting a higher-level recovery mechanism call os.Exit.

			// Let's log the critical error first with the domain logger.
			c.logger.Error(ctx,
				"CRITICAL: PANIC GUARD TRIGGERED. Consecutive processing failures exceeded threshold.",
				zap.Uint64("failure_count", c.consecutiveProcessingFailureCount),
				zap.Duration("failure_window_exceeded", time.Since(*c.firstProcessingFailureTimestamp)),
				zap.Time("first_failure_timestamp", *c.firstProcessingFailureTimestamp),
				zap.Duration("configured_threshold", failureThresholdDuration),
			)

			// To achieve a `log.Fatal` effect (log then exit), we will panic here.
			// The main application goroutine should have a defer recover that logs the panic and calls os.Exit(1).
			// This is a common pattern for controlled shutdowns on critical unrecoverable errors.
			panic(fmt.Sprintf("Panic Guard: Terminating due to %d consecutive failures over %s (threshold %s)",
				c.consecutiveProcessingFailureCount,
				time.Since(*c.firstProcessingFailureTimestamp),
				failureThresholdDuration,
			))
		}
	}
}

// resetConsecutiveFailures safely resets the count of consecutive processing failures.
func (c *Consumer) resetConsecutiveFailures(ctx context.Context) {
	c.panicGuardMutex.Lock()
	defer c.panicGuardMutex.Unlock()

	if c.consecutiveProcessingFailureCount > 0 {
		c.logger.Info(ctx, "Resetting consecutive processing failure count",
			zap.Uint64("previous_consecutive_failures", c.consecutiveProcessingFailureCount),
		)
		c.consecutiveProcessingFailureCount = 0
		c.firstProcessingFailureTimestamp = nil
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
