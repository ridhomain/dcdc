package integration_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestExampleIntegration serves as a placeholder and basic check for the integration test setup.
// It ensures that tests within this package are discovered and run correctly.
func TestExampleIntegration(t *testing.T) {
	t.Parallel() // Mark this test as safe to run in parallel with other top-level tests.

	t.Run("SubtestTrueIsTrue", func(t *testing.T) {
		t.Parallel() // Mark this subtest as safe to run in parallel.
		actualValue := true
		expectedValue := true
		assert.Equal(t, expectedValue, actualValue, "The values should be equal.")
		t.Log("Example integration subtest completed successfully.")
	})

	t.Run("SubtestAnotherExample", func(t *testing.T) {
		t.Parallel()
		assert.NotEmpty(t, "some string", "This string should not be empty.")
		t.Log("Another example integration subtest completed.")
	})
}
