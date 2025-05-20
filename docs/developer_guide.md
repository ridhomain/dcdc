# Daisi CDC Consumer Service - Developer Guide

This guide provides instructions and information for developers working on the Daisi CDC Consumer Service.

## Table of Contents

1.  [Setup Instructions](#1-setup-instructions)
2.  [Project Structure Overview](#2-project-structure-overview)
3.  [Development Workflow](#3-development-workflow)
4.  [Testing Approach](#4-testing-approach)
5.  [Common Troubleshooting Steps](#5-common-troubleshooting-steps)

## 1. Setup Instructions

This section outlines how to set up the development environment.

### Prerequisites

*   **Go:** Version 1.23 or higher (as specified in `go.mod`).
*   **Docker & Docker Compose:** For running dependent services like NATS and Redis, and for building/running the application in a containerized environment.
*   **Make:** For using Makefile shortcuts.
*   **Git:** For version control.
*   **(Optional) `golangci-lint`:** For linting Go code. Install from [https://golangci-lint.run/usage/install/](https://golangci-lint.run/usage/install/).

### Initial Setup

1.  **Clone the Repository:**
    ```bash
    git clone <repository_url>
    cd daisi-cdc-consumer-service
    ```

2.  **Configuration:**
    *   Copy the example environment file:
        ```bash
        cp .env.example .env
        ```
    *   Review and update `.env` if necessary. The defaults are generally suitable for local development using Docker Compose.
    *   The main configuration is in `config.yaml`. Environment variables in `.env` (prefixed with `DAISI_CDC_`) will override values in `config.yaml`.

3.  **Install Go Dependencies:**
    ```bash
    go mod tidy
    go mod download
    ```

4.  **Start Dependent Services (NATS & Redis):**
    The easiest way to start NATS and Redis is using Docker Compose:
    ```bash
    make up
    # or
    # docker-compose up -d
    ```
    This will start NATS (with JetStream enabled) and Redis, with data persisted in the `.data/` directory.

### Running the Application

*   **Locally using `go run`:**
    ```bash
    go run cmd/daisi-cdc-consumer-service/main.go
    ```
    Ensure NATS and Redis are accessible (likely via `make up`).

*   **Using Docker (after building):**
    First, build the Docker image:
    ```bash
    make build
    # or
    # docker build -t daisi-cdc-consumer-service:latest .
    ```
    Then, you can run it via `docker-compose up cdc-consumer` (if other services are already up) or it will be started as part of `make up` / `docker-compose up -d`.

### Useful Makefile Commands

*   `make build`: Builds the application's Docker image.
*   `make up`: Starts all services (application, NATS, Redis) using Docker Compose in detached mode.
*   `make down`: Stops and removes all Docker Compose services.
*   `make logs`: Tails logs specifically for the `cdc-consumer` service.
*   `make ps`: Shows the status of Docker Compose services.
*   `make test`: Runs Go unit and integration tests.
*   `make test-coverage`: Runs tests and generates an HTML coverage report (`coverage.html`).
*   `make bench`: Runs Go benchmarks for the application package.
*   `make lint`: Runs `golangci-lint`.
*   `make generate`: Runs `go generate ./...` (useful for tools like Wire).
*   `make prune` or `make clean`: Prunes unused Docker objects.

## 2. Project Structure Overview

```
daisi-cdc-consumer-service/
├── cmd/                                # Main application entrypoints
│   └── daisi-cdc-consumer-service/
│       └── main.go                     # Application bootstrap and startup
├── docs/                               # Project documentation (like this guide)
├── integration_test/                   # Integration tests (using Go and Testcontainers)
├── internal/                           # Internal application code
│   ├── adapters/                       # Adapters for external services and frameworks
│   │   ├── config/                     # Configuration loading (Viper)
│   │   ├── logger/                     # Logging (Zap)
│   │   ├── metrics/                    # Metrics (Prometheus)
│   │   ├── nats/                       # NATS JetStream client for ingest and publish
│   │   └── redis/                      # Redis client for deduplication
│   ├── application/                    # Core application logic, use cases, services
│   │   ├── consumer.go                 # Core CDC event processing logic
│   │   ├── transform_service.go        # Service for transforming and enriching events
│   │   └── worker_pool.go              # Manages concurrent event processing
│   ├── bootstrap/                      # Dependency injection (Google Wire) and app initialization
│   │   ├── container.go                # DI container setup
│   │   └── wire_gen.go                 # Generated Wire code
│   └── domain/                         # Domain models, interfaces (ports), custom errors
│       ├── model.go                    # Core data structures (CDCEventData, EnrichedEventPayload, etc.)
│       ├── ports.go                    # Interfaces for adapters
│       └── apperrors.go                # Custom error types
├── scripts/                            # Utility scripts (if any)
├── tasks/                              # Task definitions for Task Master (if used)
├── .env.example                        # Example environment variables
├── .gitignore                          # Files and directories ignored by Git
├── config.yaml                         # Default application configuration
├── docker-compose.yaml                 # Docker Compose setup for development
├── Dockerfile                          # Dockerfile for building the application image
├── go.mod                              # Go module definition
├── go.sum                              # Go module checksums
├── Makefile                            # Makefile for common development tasks
├── README.md                           # Project overview
└── sequin.yaml                         # Sequin configuration (for CDC source)
```

## 3. Development Workflow

The typical development workflow involves:

1.  **Understand Requirements:** Clarify the feature or bug fix.
2.  **Branching:** Create a new Git branch for your changes.
3.  **Code Implementation:**
    *   Modify existing code or add new functionality, primarily within the `internal/` directory.
    *   **Domain Logic (`internal/domain`):** Define or update data structures, interfaces, and domain-specific errors.
    *   **Application Logic (`internal/application`):** Implement or modify business logic, services (like `Consumer` or `TransformService`).
    *   **Adapters (`internal/adapters`):** If interacting with new external systems or changing how existing ones are used, update or add adapters.
    *   **Bootstrap (`internal/bootstrap`):** If adding new major components or changing dependencies, update the Wire provider sets. Run `make generate` to update `wire_gen.go`.
4.  **Configuration:** If new configuration parameters are needed, add them to `config.yaml`, `.env.example`, and update the `internal/adapters/config/viper.go` provider (constants and defaults).
5.  **Testing:**
    *   Write unit tests for new logic (place `_test.go` files alongside the code being tested).
    *   Write integration tests in `integration_test/` for flows involving multiple components or external services (like NATS/Redis).
    *   Run tests using `make test`.
6.  **Linting:** Run `make lint` to check for code style issues.
7.  **Running Locally:**
    *   Use `make up` to ensure NATS/Redis are running.
    *   Run the application using `go run cmd/daisi-cdc-consumer-service/main.go` or via Docker Compose.
    *   Check logs using `make logs`.
8.  **Commit and Push:** Commit your changes with clear messages and push to the remote repository.
9.  **Pull Request:** Create a pull request for review.


## 4. Testing Approach

The project employs a combination of unit and integration tests.

### Unit Tests

*   **Location:** Reside in `_test.go` files alongside the code they are testing (e.g., `internal/application/consumer_test.go`).
*   **Purpose:** To test individual functions, methods, or small units of logic in isolation.
*   **Tools:** Standard Go `testing` package, `testify/assert` and `testify/mock` for assertions and mocking dependencies.
*   **Execution:** `go test ./internal/...` or part of `make test`.

### Integration Tests

*   **Location:** Primarily in the `integration_test/` directory.
*   **Purpose:** To test the interaction between different components of the service and with external dependencies like NATS and Redis.
*   **Tools:**
    *   Standard Go `testing` package and `testify/suite` for structuring test suites.
    *   `testcontainers-go` for programmatically managing Docker containers (NATS, Redis) during tests. This ensures a clean, isolated environment for each test run.
    *   Helper functions in `integration_test/` (e.g., `docker_helpers_test.go`, `event_helpers_test.go`) assist in setting up these environments and publishing/subscribing to NATS.
*   **Execution:** `go test ./integration_test/...` or part of `make test`.

### Running Tests

*   **All tests:**
    ```bash
    make test
    ```
*   **Specific package tests:**
    ```bash
    go test gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/application -v
    ```
*   **Test Coverage:**
    ```bash
    make test-coverage
    ```
    This generates an `coverage.html` file which can be opened in a browser to view code coverage.

### Benchmarks

*   Benchmarks are located in `_bench_test.go` files (e.g., `internal/application/consumer_bench_test.go`).
*   **Execution:**
    ```bash
    make bench
    ```

## 5. Common Troubleshooting Steps

*   **Application Fails to Start:**
    *   **Check Logs:** Use `make logs` or `docker-compose logs cdc-consumer`. Look for `panic`, `ERROR`, or `FATAL` messages.
    *   **NATS/Redis Connectivity:** Ensure NATS and Redis are running and accessible.
        *   `docker-compose ps` should show them as `Up`.
        *   Check their respective logs: `docker-compose logs nats`, `docker-compose logs redis`.
        *   Verify connection URLs in `config.yaml` or `.env` match the exposed ports/services in `docker-compose.yaml`.
    *   **Configuration Issues:** Double-check `config.yaml` and `.env` for typos or incorrect values. The service logs any issues reading the config file.
    *   **Port Conflicts:** If running locally without Docker, ensure the metrics port (default `8080`) is not already in use.

*   **Events Not Being Processed:**
    *   **Check Source NATS Stream:** Are events being published to the `cdc_events_stream` (or configured source stream)? Use a NATS client (e.g., `nats cli`) to inspect the stream.
    *   **Check Consumer Logs (`make logs`):**
        *   Are there errors related to unmarshaling, transformation, deduplication, or publishing?
        *   Are messages being ACKed or NACKed? Frequent NACKs indicate a persistent processing problem.
        *   Is the "panic guard" triggering?
    *   **Check Target NATS Stream (`wa_stream`):** Are enriched events appearing on the configured target stream?
    *   **Deduplication Issues:** If events seem to be processed but not published, check Redis. Ensure `dedup_ttl` is reasonably set.
    *   **Table Whitelisting:** Ensure the table from which events originate is listed in `domain.AllowedTables`.
    *   **Subject Mismatch:** Verify the NATS subject patterns in `config.yaml` (`js_cdc_stream_subjects`) match the subjects where events are being published.

*   **High Resource Usage:**
    *   **Worker Pool Size:** The `workers` or `workers_multiplier` configuration might be too high for the available resources.
    *   **Memory Leaks:** Profile the application if memory usage continuously grows.

*   **`golangci-lint` Failures (`make lint`):**
    *   Address the reported linting issues. The linter helps maintain code quality and catch potential bugs.

*   **`go generate` Issues (`make generate`):**
    *   Usually related to Wire. Check the output for errors. Ensure `internal/bootstrap/container.go` has correct provider sets.

*   **Docker Build Failures (`make build`):**
    *   Ensure you have network connectivity to download Go modules and base Docker images.
    *   Check `Dockerfile` for issues.

*   **Integration Test Failures (`make test`):**
    *   These can be complex. Examine the test output carefully.
    *   Ensure Docker is running and `testcontainers-go` can pull necessary images (NATS, Redis).
    *   Look for timeouts or assertion failures. Test logs within `integration_test/` can provide more context. The tests often log detailed steps.

Always refer to the application logs as the first step in troubleshooting. They are configured to be structured (JSON) and provide contextual information. 