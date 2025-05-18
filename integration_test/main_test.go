package integration_test

import (
	"os"
	"testing"
	// Add any other necessary imports for global setup/teardown
)

func TestMain(m *testing.M) {
	// --- SETUP ---
	// This is where you would put global setup for all integration tests in this package.
	// For example:
	// - Reading configuration
	// - Initializing a global logger for tests
	// - Setting up any shared resources that don't involve Docker containers (those will be per-test or per-suite)
	println("Running integration test setup...")

	// Run the tests
	exitCode := m.Run()

	// --- TEARDOWN ---
	// This is where you would put global teardown for all integration tests.
	// For example:
	// - Cleaning up temporary files or resources created during setup
	println("Running integration test teardown...")

	os.Exit(exitCode)
}
