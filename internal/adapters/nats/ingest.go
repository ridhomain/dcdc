package nats

import (
	"context"
	"fmt"
	"strings" // Import strings package
	"time"    // For placeholder logic

	// Placeholder for the actual NATS library, e.g., "github.com/nats-io/nats.go"
	// "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go" // Import the NATS library

	"gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/adapters/config" // For config keys
	"gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/application"
	"gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/domain"
	"go.uber.org/zap"
)

// JetStreamIngester subscribes to a NATS JetStream and passes messages to an application.Consumer.
type JetStreamIngester struct {
	configProvider domain.ConfigProvider
	logger         domain.Logger
	consumer       *application.Consumer // The application layer consumer to handle messages
	natsConn       *nats.Conn            // NATS connection (now injected)
	jsCtx          nats.JetStreamContext // JetStream context (now injected)
	subscription   *nats.Subscription    // NATS subscription
	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc
}

// NewJetStreamIngester creates a new NATS JetStream ingester.
// It now accepts an existing NATS connection and JetStream context.
func NewJetStreamIngester(
	cfg domain.ConfigProvider,
	log domain.Logger,
	appConsumer *application.Consumer,
	nc *nats.Conn, // Injected NATS connection
	js nats.JetStreamContext, // Injected JetStream context
) (*JetStreamIngester, error) {
	logger := log.With(zap.String("component", "nats_ingester"))

	if nc == nil || !nc.IsConnected() {
		logger.Error(context.Background(), "NATS connection is nil or not connected for ingester")
		return nil, fmt.Errorf("NATS connection is nil or not connected for ingester")
	}
	if js == nil {
		logger.Error(context.Background(), "JetStream context is nil for ingester")
		return nil, fmt.Errorf("JetStream context is nil for ingester")
	}

	logger.Info(context.Background(), "Using provided NATS connection and JetStream context for ingester.")

	// Ensure JetStream Stream and Consumer for CDC Events (this part remains largely the same,
	// but uses the injected js context)
	streamName := cfg.GetString(config.KeyJSCdcStreamName)
	if streamName == "" {
		streamName = "cdc_events_stream"
		logger.Info(context.Background(), "NATS_STREAM_NAME not configured, using default", zap.String("default_stream_name", streamName))
	}

	streamSubjectsCSV := cfg.GetString(config.KeyJSCdcStreamSubjects)
	if streamSubjectsCSV == "" {
		streamSubjectsCSV = "cdc.*.*"
		logger.Info(context.Background(), "NATS_STREAM_SUBJECTS not configured, using default", zap.String("default_stream_subjects_csv", streamSubjectsCSV))
	}
	streamSubjects := strings.Split(streamSubjectsCSV, ",")
	for i, s := range streamSubjects {
		streamSubjects[i] = strings.TrimSpace(s)
	}

	streamInfo, err := js.StreamInfo(streamName)
	if err != nil {
		if err == nats.ErrStreamNotFound {
			logger.Info(context.Background(), "Stream not found, creating it", zap.String("stream_name", streamName), zap.Strings("subjects", streamSubjects))
			_, streamAddErr := js.AddStream(&nats.StreamConfig{
				Name:      streamName,
				Subjects:  streamSubjects,
				Storage:   nats.FileStorage,
				Retention: nats.WorkQueuePolicy,
			})
			if streamAddErr != nil {
				logger.Error(context.Background(), "Failed to create stream", zap.Error(streamAddErr), zap.String("stream_name", streamName))
				// Not closing nc here as it's managed externally
				return nil, fmt.Errorf("failed to create stream %s: %w", streamName, streamAddErr)
			}
			logger.Info(context.Background(), "Stream created successfully", zap.String("stream_name", streamName))
		} else {
			logger.Error(context.Background(), "Failed to get stream info", zap.Error(err), zap.String("stream_name", streamName))
			return nil, fmt.Errorf("failed to get stream info for %s: %w", streamName, err)
		}
	} else {
		logger.Info(context.Background(), "Stream already exists", zap.String("stream_name", streamName), zap.Any("stream_info", streamInfo))
	}

	consumerName := cfg.GetString(config.KeyJSCdcConsumerGroup)
	if consumerName == "" {
		consumerName = "cdc_consumers"
		logger.Info(context.Background(), "NATS_CONSUMER_NAME not configured, using default", zap.String("default_consumer_name", consumerName))
	}

	ackWait := cfg.GetDuration(config.KeyJSAckWait)
	maxAckPending := cfg.GetInt(config.KeyJSMaxAckPending)
	maxDeliver := cfg.GetInt(config.KeyJSMaxDeliver)

	if ackWait == 0 {
		ackWait = 30 * time.Second
	}
	if maxAckPending == 0 {
		maxAckPending = 5000
	}
	if maxDeliver == 0 {
		maxDeliver = 3
	}

	consumerInfo, err := js.ConsumerInfo(streamName, consumerName)
	if err != nil || consumerInfo == nil {
		if err != nats.ErrConsumerNotFound && err != nil {
			logger.Warn(context.Background(), "Could not get consumer info, attempting to create/update", zap.Error(err), zap.String("consumer_name", consumerName))
		}
		logger.Info(context.Background(), "Consumer not found or needs update, creating/updating it",
			zap.String("stream_name", streamName),
			zap.String("consumer_name", consumerName),
		)
		filterSubject := streamSubjects[0] // Default to first subject in list if multiple
		if len(streamInfo.Config.Subjects) > 0 {
			// More robust: if stream has multiple subjects, ensure consumer filter matches one, or is a wildcard.
			// For simplicity, AddConsumer will use this filterSubject. If streamSubjects has "cdc.*.*", this is fine.
			// If streamSubjects is e.g. "cdc.co1.tbl1,cdc.co2.tbl2", consumer needs specific filter or a broader one.
			// For now, we'll stick to this, assuming the first subject is appropriate or a wildcard.
		}

		_, consumerAddErr := js.AddConsumer(streamName, &nats.ConsumerConfig{
			Durable:       consumerName,
			AckPolicy:     nats.AckExplicitPolicy,
			FilterSubject: filterSubject,
			AckWait:       ackWait,
			MaxAckPending: maxAckPending,
			MaxDeliver:    maxDeliver,
		})
		if consumerAddErr != nil {
			logger.Error(context.Background(), "Failed to create/update consumer", zap.Error(consumerAddErr), zap.String("consumer_name", consumerName))
			return nil, fmt.Errorf("failed to create/update consumer %s: %w", consumerName, consumerAddErr)
		}
		logger.Info(context.Background(), "Consumer created/updated successfully", zap.String("consumer_name", consumerName))
	} else {
		logger.Info(context.Background(), "Consumer already exists and configured", zap.String("consumer_name", consumerName))
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &JetStreamIngester{
		configProvider: cfg,
		logger:         logger,
		consumer:       appConsumer,
		natsConn:       nc, // Store injected connection
		jsCtx:          js, // Store injected context
		shutdownCtx:    ctx,
		shutdownCancel: cancel,
	}, nil
}

// Start begins subscribing to JetStream messages and processing them.
// This is a blocking call and should typically be run in a goroutine.
func (j *JetStreamIngester) Start() error {
	j.logger.Info(j.shutdownCtx, "Starting NATS JetStream ingester...")

	queueGroupName := j.configProvider.GetString(config.KeyJSCdcConsumerGroup)
	// The stream subjects are configured on the stream itself. QueueSubscribe for a stream
	// with a queue group will receive messages matching any of the stream's subjects.
	// The FilterSubject in consumer config is what NATS uses for the durable consumer.
	// For QueueSubscribe, we typically provide the specific subject pattern or a wildcard if the stream has one.
	// Here, we use the first subject from config for the subscribe call, assuming it's representative
	// or a wildcard like "cdc.*.*".
	subjectsCSV := j.configProvider.GetString(config.KeyJSCdcStreamSubjects)
	subscribeSubject := strings.Split(subjectsCSV, ",")[0] // Use the first subject for QueueSubscribe

	ackWait := j.configProvider.GetDuration(config.KeyJSAckWait)
	maxAckPending := j.configProvider.GetInt(config.KeyJSMaxAckPending)

	var err error
	j.subscription, err = j.jsCtx.QueueSubscribe(
		subscribeSubject,          // Subject to subscribe to
		queueGroupName,            // Queue group name
		j.processJetStreamMessage, // Message handler callback
		nats.ManualAck(),
		nats.AckWait(ackWait),
		nats.MaxAckPending(maxAckPending),
		// nats.DeliverAll(), // Optional: for new consumer, deliver all available messages
		// nats.Durable(queueGroupName), // QueueSubscribe implies durable with the queue group name
	)

	if err != nil {
		j.logger.Error(j.shutdownCtx, "Failed to subscribe to JetStream",
			zap.String("subject", subscribeSubject),
			zap.String("queue_group", queueGroupName),
			zap.Error(err),
		)
		return fmt.Errorf("failed to subscribe to JetStream subject %s with queue group %s: %w", subscribeSubject, queueGroupName, err)
	}

	j.logger.Info(j.shutdownCtx, "Successfully subscribed to JetStream",
		zap.String("subject", j.subscription.Subject),
		zap.String("queue_group", j.subscription.Queue),
	)

	// Wait for shutdown signal
	<-j.shutdownCtx.Done()
	j.logger.Info(j.shutdownCtx, "NATS JetStream ingester processing loop ended due to shutdown signal.")
	return nil
}

// Shutdown gracefully stops the ingester.
// It should only drain its subscription, not the NATS connection itself, as that's managed externally.
func (j *JetStreamIngester) Shutdown() error {
	j.logger.Info(j.shutdownCtx, "Initiating NATS JetStream ingester shutdown...")
	j.shutdownCancel() // Signal all operations using shutdownCtx to stop

	// Gracefully drain the subscription only
	if j.subscription != nil && j.subscription.IsValid() {
		j.logger.Info(j.shutdownCtx, "Draining NATS JetStream subscription...")
		if err := j.subscription.Drain(); err != nil {
			j.logger.Error(j.shutdownCtx, "Error draining NATS subscription", zap.Error(err))
			// Do not return error here, attempt to continue shutdown
		} else {
			j.logger.Info(j.shutdownCtx, "NATS JetStream subscription drained successfully.")
		}
	}

	// DO NOT DRAIN OR CLOSE j.natsConn here as it is managed by provideNATSConnection.
	j.logger.Info(j.shutdownCtx, "NATS JetStream ingester shutdown complete (NATS connection managed externally).")
	return nil
}

// Placeholder for a NATS message wrapper that implements application.CDCEventMessage
// This will be refined once actual NATS library is used.
type natsJetStreamMessageWrapper struct {
	msg    *nats.Msg     // The underlying NATS message
	logger domain.Logger // For logging within the wrapper if needed, or pass from ingester
}

// NewNatsJetStreamMessageWrapper creates a new wrapper for nats.Msg
func NewNatsJetStreamMessageWrapper(m *nats.Msg, l domain.Logger) application.CDCEventMessage {
	return &natsJetStreamMessageWrapper{msg: m, logger: l}
}

func (w *natsJetStreamMessageWrapper) GetData() []byte {
	return w.msg.Data
}

func (w *natsJetStreamMessageWrapper) GetSubject() string {
	return w.msg.Subject
}

func (w *natsJetStreamMessageWrapper) Ack() error {
	if err := w.msg.AckSync(); err != nil { // Using AckSync for clearer error handling if needed
		// Log error if AckSync fails? Depends on how critical it is / if redelivery is worse.
		w.logger.Error(context.Background(), "Failed to ACK message via AckSync", zap.Error(err), zap.String("subject", w.msg.Subject))
		return err
	}
	return nil
}

func (w *natsJetStreamMessageWrapper) Nack(delay time.Duration) error {
	// PRD implies NACK relies on JetStream's AckWait rather than client-side delay for redelivery.
	// So, Nak() is appropriate. If a delay is explicitly needed, NakWithDelay(delay) could be used.
	if err := w.msg.Nak(); err != nil {
		w.logger.Error(context.Background(), "Failed to NACK message", zap.Error(err), zap.String("subject", w.msg.Subject))
		return err
	}
	return nil
}

// processJetStreamMessage is the callback for JetStream subscriptions.
func (j *JetStreamIngester) processJetStreamMessage(natsMsg *nats.Msg) {
	// Panic recovery for this specific goroutine (message handler)
	defer func() {
		if r := recover(); r != nil {
			j.logger.Error(j.shutdownCtx, "Panic recovered in NATS message handler",
				zap.Any("panic_error", r),
				zap.String("subject", natsMsg.Subject),
				zap.Stack("stacktrace"), // Log stack trace for debugging
			)
			// Attempt to NACK the message if a panic occurred during its processing.
			if err := natsMsg.Nak(); err != nil {
				j.logger.Error(j.shutdownCtx, "Failed to NACK message after panic recovery", zap.Error(err), zap.String("subject", natsMsg.Subject))
			}
		}
	}()

	// Check if shutdown has been initiated. If so, try to Nack and return.
	// This prevents processing new messages when the system is shutting down.
	select {
	case <-j.shutdownCtx.Done():
		j.logger.Info(j.shutdownCtx, "Shutdown initiated, not processing new message, attempting NACK", zap.String("subject", natsMsg.Subject))
		if err := natsMsg.Nak(); err != nil {
			j.logger.Error(j.shutdownCtx, "Failed to NACK message during shutdown check", zap.Error(err), zap.String("subject", natsMsg.Subject))
		}
		return
	default:
		// Continue processing
	}

	wrappedMsg := NewNatsJetStreamMessageWrapper(natsMsg, j.logger)
	// Create a new context for this message processing, possibly linking to a trace ID later.
	// For now, using background context. Ensure logger has trace_id/event_id from HandleCDCEvent.
	msgProcessingCtx := context.Background() // TODO: Consider deriving from j.shutdownCtx or adding tracing

	if err := j.consumer.HandleCDCEvent(msgProcessingCtx, wrappedMsg); err != nil {
		// HandleCDCEvent should have logged its own detailed error.
		// The ingester logs the failure to process and NACKs.
		j.logger.Warn(j.shutdownCtx, "Error processing CDC event, attempting NACK",
			zap.Error(err), // Error from HandleCDCEvent
			zap.String("subject", natsMsg.Subject),
		)
		if nackErr := wrappedMsg.Nack(0); nackErr != nil {
			j.logger.Error(j.shutdownCtx, "Failed to NACK message after processing error",
				zap.Error(nackErr),
				zap.String("subject", natsMsg.Subject),
			)
		}
	} else {
		// Ack on successful processing by HandleCDCEvent
		if ackErr := wrappedMsg.Ack(); ackErr != nil {
			// If Ack fails, message will likely be redelivered. Deduplication should handle it.
			j.logger.Error(j.shutdownCtx, "Failed to ACK message after successful processing",
				zap.Error(ackErr),
				zap.String("subject", natsMsg.Subject),
			)
		}
	}
}

// Example of how the message handler callback might look (simplified):
// func (j *JetStreamIngester) handleNatsMessage(natsMsg *nats.Msg) {
//   wrappedMsg := &natsJetStreamMessageWrapper{msg: natsMsg}
//   ctx := context.Background() // Ideally, a root context or one with trace_id
//   if err := j.consumer.HandleCDCEvent(ctx, wrappedMsg); err != nil {
//     j.logger.Error(ctx, "Error handling CDC event", zap.Error(err), zap.String("subject", natsMsg.Subject))
//     // Nack logic would be here, potentially based on error type
//   }
//   // Ack logic (if not auto-ack) would be here on success.
// }
