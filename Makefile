# Makefile for daisi-cdc-consumer-service

# Variables
APP_NAME := daisi-cdc-consumer-service
CONTAINER_TAG := latest
CDC_CONSUMER_SERVICE_NAME := cdc-consumer

.PHONY: all build up down stop logs ps test test-coverage bench lint generate prune clean help

all: build

# Docker commands
build:
	@echo "Building Docker image $(APP_NAME):$(CONTAINER_TAG)..."
	docker build -t $(APP_NAME):$(CONTAINER_TAG) .

up:
	@echo "Starting Docker services in detached mode..."
	docker-compose up -d

down:
	@echo "Stopping Docker services..."
	docker-compose down

stop: down # Alias for down

logs:
	@echo "Tailing logs for $(CDC_CONSUMER_SERVICE_NAME)..."
	docker-compose logs -f $(CDC_CONSUMER_SERVICE_NAME)

ps:
	@echo "Current Docker services status..."
	docker-compose ps

# Go commands
test:
	@echo "Running Go tests..."
	go test -v ./...

test-coverage:
	@echo "Running Go tests with coverage..."
	go test -v -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

bench:
	@echo "Running Go benchmarks for the application package (verbose)..."
	@go clean -testcache
	cd ./internal/application && go test -v -run=^$ -bench=. -benchmem

lint:
	@echo "Running golangci-lint..."
	golangci-lint run

generate:
	@echo "Running go generate..."
	go generate ./...

# System cleanup
prune:
	@echo "Pruning Docker system (all unused containers, networks, images) and volumes..."
	docker system prune -af
	docker volume prune -af

clean: prune # Alias for prune

help:
	@echo "Available commands:"
	@echo "  build          - Build the Docker image for the application."
	@echo "  up             - Start all services defined in docker-compose.yaml in detached mode."
	@echo "  down           - Stop and remove all services defined in docker-compose.yaml."
	@echo "  stop           - Alias for down."
	@echo "  logs           - Tail logs from the application container."
	@echo "  ps             - Show the status of Docker services."
	@echo "  test           - Run Go unit and integration tests."
	@echo "  test-coverage  - Run Go tests and generate an HTML coverage report."
	@echo "  bench          - Run Go benchmarks for the application package (includes memory stats)."
	@echo "  lint           - Run golangci-lint (if installed)."
	@echo "  generate       - Run go generate (e.g., for Wire)."
	@echo "  prune          - Remove all unused Docker containers, networks, images, and volumes."
	@echo "  clean          - Alias for prune."
	@echo "  help           - Show this help message." 