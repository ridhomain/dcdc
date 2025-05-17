package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/bootstrap"
	"go.uber.org/zap"
)

func main() {
	// Setup panic recovery at the highest level
	defer func() {
		if r := recover(); r != nil {
			// Attempt to log the panic if possible, then exit
			// This is a last resort; ideally, the logger from DI would be used if available before panic.
			fmt.Fprintf(os.Stderr, "Service panicked: %v\n", r)
			// Optionally, use a pre-initialized basic logger here if the main logger isn't available.
			os.Exit(1)
		}
	}()

	app, cleanup, err := bootstrap.InitializeApp()
	if err != nil {
		// If InitializeApp fails, the logger might not be available yet.
		// Log to stderr and exit.
		fmt.Fprintf(os.Stderr, "Failed to initialize application: %v\n", err)
		os.Exit(1)
	}
	// If cleanup is nil, it means something went wrong, but InitializeApp might not have returned an error
	// (though it should). This is a safeguard.
	if cleanup == nil {
		app.Logger.Error(context.Background(), "Initialization returned nil cleanup function without error. Exiting.")
		os.Exit(1)
	}
	defer cleanup() // Ensure all resources are cleaned up on exit

	app.Logger.Info(context.Background(), "Application initialized successfully", zap.String("service", bootstrap.ServiceName))

	// Start the NATS JetStream Ingester in a goroutine
	// The Ingester's Start method is blocking, so it needs its own goroutine.
	// It also handles its own shutdown via its shutdownCtx.
	go func() {
		app.Logger.Info(context.Background(), "Starting NATS JetStream Ingester...")
		if err := app.Ingester.Start(); err != nil {
			// If Ingester.Start() returns an error (e.g., initial subscription fails catastrophically),
			// it might indicate a non-recoverable state. We log it as fatal.
			// The panic guard within the consumer handles processing panics.
			// This specific error is about the ingester failing to start its listening loop.
			app.Logger.Error(context.Background(), "NATS JetStream Ingester failed to start or exited with error", zap.Error(err))
			// Consider a way to signal main to shut down if ingester fails to start.
			// For now, logging the error. A more robust solution might involve a channel.
			// To ensure service exits if ingester can't start, we can panic here which will be caught by the main defer.
			panic(fmt.Sprintf("NATS JetStream Ingester failed: %v", err))
		}
		app.Logger.Info(context.Background(), "NATS JetStream Ingester stopped.")
	}()

	// The Prometheus metrics server (app.MetricsServer) is started by its provider in a goroutine.
	// Its shutdown is handled by the main cleanup function.

	app.Logger.Info(context.Background(), "Application started. Waiting for interrupt signal to gracefully shutdown...")

	// Wait for interrupt signal for graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	receivedSignal := <-quit

	app.Logger.Info(context.Background(), "Received interrupt signal, initiating graceful shutdown...", zap.String("signal", receivedSignal.String()))

	// Create a context with a timeout for the shutdown process
	// This gives components a deadline to finish their cleanup.
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 30*time.Second) // Adjust timeout as needed
	defer cancelShutdown()

	// Perform application-specific shutdown if necessary before calling the main cleanup.
	// For example, signaling the Ingester to stop (which its Shutdown method called by `cleanup` already does).

	app.Logger.Info(shutdownCtx, "Calling main cleanup function...")
	// The main cleanup function (from InitializeApp) will handle individual component shutdowns.
	// It's already deferred, but calling it here explicitly before exit ensures it runs before potential
	// premature exit due to shutdown timeout. The defer will still run if this explicit call panics.
	// However, Wire cleanup functions are designed to be called once. The `defer cleanup()` is the primary one.
	// We don't call it explicitly here; the defer handles it.

	// The metrics server shutdown is part of the main cleanup function which will be called by defer.
	// if app.MetricsServer != nil {
	// 	app.Logger.Info(shutdownCtx, "Shutting down metrics server...")
	// 	if err := app.MetricsServer.Shutdown(shutdownCtx); err != nil {
	// 		app.Logger.Error(shutdownCtx, "Error shutting down metrics server", zap.Error(err))
	// 	}
	// }

	app.Logger.Info(shutdownCtx, "Graceful shutdown sequence initiated. Main cleanup will run on exit.")
	// The `defer cleanup()` will execute when main exits.
}
