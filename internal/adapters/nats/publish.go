package nats

import (
	"context"
	"fmt"
	"strings"

	// For placeholder in Shutdown
	"github.com/nats-io/nats.go"
	"gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/adapters/config"
	"gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/domain"
	"go.uber.org/zap"
)

// JetStreamPublisher implements the domain.Publisher interface for NATS JetStream.
type JetStreamPublisher struct {
	configProvider domain.ConfigProvider
	logger         domain.Logger
	metricsSink    domain.MetricsSink
	natsConn       *nats.Conn
	jsCtx          nats.JetStreamContext
}

// NewJetStreamPublisher creates a new NATS JetStream publisher.
// It expects an active NATS connection to be passed in.
func NewJetStreamPublisher(
	cfg domain.ConfigProvider,
	log domain.Logger,
	metrics domain.MetricsSink,
	nc *nats.Conn, // Accepts an existing NATS connection
) (*JetStreamPublisher, error) {
	logger := log.With(zap.String("component", "nats_publisher"))

	if nc == nil || !nc.IsConnected() {
		logger.Error(context.Background(), "NATS connection is nil or not connected")
		return nil, fmt.Errorf("NATS connection is nil or not connected for publisher")
	}

	js, err := nc.JetStream()
	if err != nil {
		logger.Error(context.Background(), "Failed to get JetStream context for publisher", zap.Error(err))
		return nil, fmt.Errorf("failed to get JetStream context for publisher: %w", err)
	}
	logger.Info(context.Background(), "JetStream context obtained successfully for publisher")

	// Ensure the target stream (e.g., "wa_stream") exists.
	// This makes the publisher more robust, as it can create the stream if it's missing.
	waStreamName := cfg.GetString(config.KeyJSWaStreamName) // Assumed key: "nats.wa_stream.name"
	if waStreamName == "" {
		waStreamName = "wa_stream" // Default if not configured
		logger.Info(context.Background(), "NATS_WA_STREAM_NAME not configured, using default", zap.String("default_stream_name", waStreamName))
	}
	waStreamSubjectsCSV := cfg.GetString(config.KeyJSWaStreamSubjects) // Assumed key: "nats.wa_stream.subjects"
	if waStreamSubjectsCSV == "" {
		waStreamSubjectsCSV = "wa.>" // Default, allowing any subject starting with "wa."
		logger.Info(context.Background(), "NATS_WA_STREAM_SUBJECTS not configured, using default", zap.String("default_stream_subjects_csv", waStreamSubjectsCSV))
	}
	waStreamSubjects := strings.Split(waStreamSubjectsCSV, ",")
	for i, s := range waStreamSubjects {
		waStreamSubjects[i] = strings.TrimSpace(s)
	}

	_, streamErr := js.StreamInfo(waStreamName)
	if streamErr != nil {
		if streamErr == nats.ErrStreamNotFound {
			logger.Info(context.Background(), "Target wa_stream not found, creating it",
				zap.String("stream_name", waStreamName),
				zap.Strings("subjects", waStreamSubjects),
			)
			_, addStreamErr := js.AddStream(&nats.StreamConfig{
				Name:     waStreamName,
				Subjects: waStreamSubjects,
				Storage:  nats.FileStorage, // Or MemoryStorage, depending on requirements
				// Retention: nats.LimitsPolicy, // Default, or configure as needed
				// MaxAge, MaxBytes, etc. can be set here from config.
			})
			if addStreamErr != nil {
				logger.Error(context.Background(), "Failed to create target wa_stream", zap.Error(addStreamErr), zap.String("stream_name", waStreamName))
				return nil, fmt.Errorf("failed to create target wa_stream %s: %w", waStreamName, addStreamErr)
			}
			logger.Info(context.Background(), "Target wa_stream created successfully", zap.String("stream_name", waStreamName))
		} else {
			logger.Error(context.Background(), "Failed to get target wa_stream info", zap.Error(streamErr), zap.String("stream_name", waStreamName))
			return nil, fmt.Errorf("failed to get target wa_stream info for %s: %w", waStreamName, streamErr)
		}
	} else {
		logger.Info(context.Background(), "Target wa_stream already exists", zap.String("stream_name", waStreamName))
	}

	return &JetStreamPublisher{
		configProvider: cfg,
		logger:         logger,
		metricsSink:    metrics,
		natsConn:       nc,
		jsCtx:          js,
	}, nil
}

// Publish sends the given data to the specified subject on NATS JetStream and waits for server ACK.
func (p *JetStreamPublisher) Publish(ctx context.Context, subject string, data []byte) error {
	p.logger.Debug(ctx, "Attempting to publish message and wait for server ACK",
		zap.String("subject", subject),
		zap.Int("data_len", len(data)),
		zap.ByteString("data", data),
	)

	msg := nats.NewMsg(subject)
	msg.Data = data

	// PublishMsg is synchronous and waits for an ACK from the JetStream server.
	ack, err := p.jsCtx.PublishMsg(msg, nats.Context(ctx)) // Pass context for potential cancellation
	if err != nil {
		p.logger.Error(ctx, "Failed to publish message to JetStream",
			zap.Error(err),
			zap.String("subject", subject),
		)
		p.metricsSink.IncPublishErrors()
		p.metricsSink.IncEventsPublished(subject, "failure")

		// Check if context was cancelled, as this is a common reason for publish to fail
		if ctx.Err() != nil {
			return domain.NewErrExternalService("NATS_publisher_ctx_cancelled", ctx.Err())
		}
		return domain.NewErrExternalService("NATS_publisher_send_or_ack", err)
	}

	p.metricsSink.IncEventsPublished(subject, "success")
	p.logger.Info(ctx, "Message published successfully to JetStream and ACKed by server",
		zap.String("subject", subject),
		zap.String("stream_name_from_ack", ack.Stream),
		zap.Uint64("sequence_from_ack", ack.Sequence),
		zap.Bool("duplicate_detected_by_server", ack.Duplicate),
	)
	return nil
}

// Shutdown is a no-op for JetStreamPublisher if the NATS connection is managed externally.
func (p *JetStreamPublisher) Shutdown() error {
	p.logger.Info(context.Background(), "NATS JetStream Publisher shutdown (no-op as NATS connection is managed externally).")
	return nil
}
