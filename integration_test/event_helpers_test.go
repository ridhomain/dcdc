package integration_test

import (
	"encoding/json"
	"testing"
	"time"

	natsIO "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/domain"
)

// publishCDCEvent is a test helper to publish a CDC event to a NATS subject.
// It uses the NATS connection URL from the provided suite.
func publishCDCEvent(t *testing.T, suite *IntegrationTestSuite, subject string, eventData domain.CDCEventData) error {
	t.Helper()
	suite.T().Logf("TestHelper: Attempting to publish CDC event to subject '%s' on NATS server %s", subject, suite.natsURL)

	// Connect a new NATS client for publishing this event.
	// Using a new connection for a helper is generally fine for tests, avoids managing shared publisher state.
	nc, err := natsIO.Connect(suite.natsURL,
		natsIO.Timeout(10*time.Second),
		natsIO.RetryOnFailedConnect(true),
		natsIO.MaxReconnects(3),
		natsIO.ReconnectWait(1*time.Second),
	)
	require.NoError(t, err, "TestHelper publishCDCEvent: Failed to connect to NATS at %s", suite.natsURL)
	defer nc.Close()

	// Marshal the event data to JSON.
	payloadBytes, err := json.Marshal(eventData)
	require.NoError(t, err, "TestHelper publishCDCEvent: Failed to marshal CDCEventData to JSON")

	// Publish the message.
	// For integration tests, direct publishing is fine. The application's ingester
	// should be configured to subscribe to subjects that align with these publications.
	// We don't need to use JetStream publish here because we are simulating the *source* of the event,
	// which might not be a JetStream publisher itself (e.g., Sequin NATS Sink).
	// The application's ingester *subscribes* via JetStream.
	// ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second) // Context for the publish operation - not needed for simple Publish
	// defer cancel()

	err = nc.Publish(subject, payloadBytes)
	// err = nc.PublishRequest(subject, "", payloadBytes) // Using PublishRequest to potentially get an ack if a replier was set up, though not strictly needed here.
	// A simple nc.Publish(subject, payloadBytes) would also work.
	if err != nil {
		suite.T().Logf("TestHelper publishCDCEvent: Error publishing message to subject '%s': %v", subject, err)
		return err
	}

	suite.T().Logf("TestHelper publishCDCEvent: Successfully published message to subject '%s'", subject)
	return nil
}
