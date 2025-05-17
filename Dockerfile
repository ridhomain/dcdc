# Stage 1: Build the application
FROM golang:1.23-bookworm AS builder

# Set environment variables for the build
ENV CGO_ENABLED=0

# Set the working directory
WORKDIR /app

# Copy go.mod and go.sum first to leverage Docker cache
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the rest of the source code
COPY . .

# Build the application
# Using -ldflags "-s -w" to strip debug information and symbols, reducing binary size.
RUN go build -ldflags "-s -w" -o /app/main cmd/daisi-cdc-consumer-service/main.go

# Stage 2: Create the runtime image
FROM debian:bookworm-slim

# Set the working directory
WORKDIR /app

# Create a non-root user and group
RUN groupadd -r appgroup && useradd --no-log-init -r -g appgroup appuser

# Copy the built binary from the builder stage
COPY --from=builder /app/main /app/main

# Copy the configuration file
COPY config.yaml /app/config.yaml

# Ensure the binary is executable
RUN chmod +x /app/main

# Switch to the non-root user
USER appuser

# Expose the metrics port
EXPOSE 8080

# Set the entrypoint for the container
ENTRYPOINT ["/app/main"] 