package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"

	"gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/domain"
)

const (
	// envPrefix is the prefix for environment variables.
	// For example, NATS_URL will be looked up as DAISI_CDC_NATS_URL.
	envPrefix = "DAISI_CDC"

	// KeyNatsURL is the config key for the NATS server URL.
	KeyNatsURL = "nats_url"
	// KeyRedisAddr is the config key for the Redis server address.
	KeyRedisAddr = "redis_addr"
	// KeyDedupTTL is the config key for the deduplication key TTL in Redis.
	KeyDedupTTL = "dedup_ttl"
	// KeyJSCdcStreamName is the config key for the NATS JetStream CDC stream name to subscribe to.
	KeyJSCdcStreamName = "js_cdc_stream_name"
	// KeyJSWaStreamName is the config key for the NATS JetStream stream name to publish wa events to.
	KeyJSWaStreamName = "js_wa_stream_name"
	// KeyJSCdcConsumerGroup is the config key for the NATS JetStream CDC consumer group name.
	KeyJSCdcConsumerGroup = "js_cdc_consumer_group"
	// KeyJSAckWait is the config key for NATS JetStream AckWait duration.
	KeyJSAckWait = "js_ack_wait"
	// KeyJSMaxDeliver is the config key for NATS JetStream MaxDeliver attempts.
	KeyJSMaxDeliver = "js_max_deliver"
	// KeyJSMaxAckPending is the config key for NATS JetStream MaxAckPending.
	KeyJSMaxAckPending = "js_max_ack_pending"
	// KeyWorkers is an absolute override for the number of worker goroutines.
	// If <= 0, the multiplier logic is used.
	KeyWorkers = "workers"
	// KeyWorkersMultiplier is multiplied by GOMAXPROCS to determine pool size if KeyWorkers is not set.
	KeyWorkersMultiplier = "workers_multiplier"
	// KeyMinWorkers is the minimum number of workers for the pool.
	KeyMinWorkers = "min_workers"
	// KeyLogLevel is the config key for the application's log level.
	KeyLogLevel = "log_level"
	// KeyMetricsPort is the config key for the port of the /metrics HTTP endpoint.
	KeyMetricsPort = "metrics_port"
	// KeyJSCdcStreamSubjects is the config key for the NATS JetStream CDC stream subjects.
	KeyJSCdcStreamSubjects = "js_cdc_stream_subjects"
	// KeyJSWaStreamSubjects is the config key for the NATS JetStream subjects for the wa_stream.
	KeyJSWaStreamSubjects = "js_wa_stream_subjects"
	// KeyPanicGuardFailureThresholdDuration is the config key for the duration after which consecutive failures trigger a panic.
	KeyPanicGuardFailureThresholdDuration = "panic_guard_failure_threshold_duration"
)

// viperConfigProvider implements the domain.ConfigProvider interface using Viper.
type viperConfigProvider struct {
	viper *viper.Viper
}

// NewViperConfigProvider creates and initializes a new ConfigProvider using Viper.
// It sets up Viper to read from environment variables and a config.yaml file,
// with environment variables taking precedence. Default values are also set.
// Note: Logging of the configuration loading process itself (e.g., which file was used)
// should be handled by the bootstrap process once a logger is available,
// by calling methods like v.viper.ConfigFileUsed() or v.viper.AllSettings().
func NewViperConfigProvider() domain.ConfigProvider {
	v := viper.New()

	// Set environment variable prefix and automatic loading.
	// This allows DAISI_CDC_NATS_URL to override nats_url from a config file.
	v.SetEnvPrefix(envPrefix)
	v.AutomaticEnv()
	// Useful if keys have dots or dashes, e.g., "database.url" -> DAISI_CDC_DATABASE_URL
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))

	// Configure config file properties.
	v.SetConfigName("config") // Name of config file (without extension: config.yaml, config.json, etc.)
	v.SetConfigType("yaml")   // Explicitly set the config type.

	// Add paths to search for the config file.
	v.AddConfigPath(".")                                        // Current working directory.
	v.AddConfigPath("./config")                                 // ./config/config.yaml
	v.AddConfigPath("/etc/daisi-cdc-consumer-service/")         // Standard path for Linux services.
	v.AddConfigPath("$HOME/.config/daisi-cdc-consumer-service") // User-specific config path.

	// Set default values for configuration keys.
	v.SetDefault(KeyNatsURL, "nats://localhost:4222")
	v.SetDefault(KeyRedisAddr, "redis://localhost:6379") // Assumes Redis is running locally without auth.
	v.SetDefault(KeyDedupTTL, 300*time.Second)           // 5 minutes, as per PRD.
	v.SetDefault(KeyJSCdcStreamName, "cdc_events_stream")
	v.SetDefault(KeyJSWaStreamName, "wa_stream")
	v.SetDefault(KeyJSCdcConsumerGroup, "cdc_consumers")
	v.SetDefault(KeyJSAckWait, 30*time.Second) // As per PRD.
	v.SetDefault(KeyJSMaxDeliver, 3)           // As per PRD.
	v.SetDefault(KeyJSMaxAckPending, 5000)     // As per PRD.
	v.SetDefault(KeyWorkers, 0)                // Default to 0, meaning use multiplier logic
	v.SetDefault(KeyWorkersMultiplier, 4)      // Default GOMAXPROCS multiplier
	v.SetDefault(KeyMinWorkers, 2)             // Default minimum workers
	v.SetDefault(KeyLogLevel, "info")          // Default log level.
	v.SetDefault(KeyMetricsPort, "8080")       // Default port for /metrics.
	// Default subjects for the CDC stream, aligning with Sequin NATS sink documentation.
	v.SetDefault(KeyJSCdcStreamSubjects, "sequin.changes.*.*.*.*")
	// KeyJSCdcStreamSubjects defines the pattern our service subscribes to.
	// Sequin NATS sink publishes to: sequin.changes.<database_name>.<schema_name>.<table_name>.<action>
	// So, "sequin.changes.*.*.*.*" allows us to capture all such messages.
	// The actual stream name (e.g., cdc_events_stream) is a separate NATS concept and remains configurable
	// via KeyJSCdcStreamName. The subjects defined here are bound to that stream by NATS JetStream configuration.
	v.SetDefault(KeyJSWaStreamSubjects, "wa.>")                         // Default subjects for the WA stream.
	v.SetDefault(KeyPanicGuardFailureThresholdDuration, 15*time.Minute) // Default 15 minutes for panic guard

	// Attempt to read the config file.
	// It's not an error if the file doesn't exist, as ENV vars and defaults can be used.
	// Actual error handling (e.g., logging if file not found or parsing error) can be
	// done by the bootstrap process using the returned provider.
	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			// Config file was found but another error was produced
			// This should be logged by the bootstrap process.
			// For now, we can print a warning to stderr during early dev.
			fmt.Printf("Warning: Error reading config file (%s): %v. Using defaults and ENV vars.\n", v.ConfigFileUsed(), err)
		}
		// If ConfigFileNotFoundError, we proceed silently, relying on defaults/ENV.
	}

	return &viperConfigProvider{
		viper: v,
	}
}

// GetString retrieves a string configuration value for the given key.
func (vcp *viperConfigProvider) GetString(key string) string {
	return vcp.viper.GetString(key)
}

// GetDuration retrieves a time.Duration configuration value for the given key.
func (vcp *viperConfigProvider) GetDuration(key string) time.Duration {
	return vcp.viper.GetDuration(key)
}

// GetInt retrieves an integer configuration value for the given key.
func (vcp *viperConfigProvider) GetInt(key string) int {
	return vcp.viper.GetInt(key)
}

// GetBool retrieves a boolean configuration value for the given key.
func (vcp *viperConfigProvider) GetBool(key string) bool {
	return vcp.viper.GetBool(key)
}

// Set allows overriding a configuration value. This is primarily intended for testing.
func (vcp *viperConfigProvider) Set(key string, value interface{}) {
	vcp.viper.Set(key, value)
}
