package application

import (
	"context" // Added context for logging
	"fmt"
	"runtime" // Import runtime for GOMAXPROCS
	"sync"    // For placeholder WaitGroup or similar if needed later
	"time"    // For placeholder shutdown logic

	"github.com/panjf2000/ants/v2" // As per PRD §5 and Technical Arch §5.3

	"gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/adapters/config" // To access config keys
	"gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/domain"
	"go.uber.org/zap" // For placeholder logging fields
)

// WorkerPool manages a Pool of goroutines to process tasks concurrently.
// It uses the panjf2000/ants library as specified in the PRD.
type WorkerPool struct {
	configProvider domain.ConfigProvider
	logger         domain.Logger
	Pool           *ants.Pool
	wg             sync.WaitGroup // To wait for tasks to complete during shutdown
}

var GetMaxProcs = func() int {
	return runtime.GOMAXPROCS(0)
}

// NewWorkerPool creates a new worker Pool.
// It initializes the ants Pool based on configuration.
func NewWorkerPool(cfg domain.ConfigProvider, log domain.Logger) (*WorkerPool, error) {
	numWorkers := 0
	logFields := make([]zap.Field, 0, 3) // For logging how numWorkers was determined

	// 1. Check for absolute override
	numWorkersOverride := cfg.GetInt(config.KeyWorkers)
	if numWorkersOverride > 0 {
		numWorkers = numWorkersOverride
		logFields = append(logFields, zap.String("reason", "absolute_override"), zap.Int("override_value", numWorkers))
	} else {
		// 2. If no override, use multiplier logic
		multiplier := cfg.GetInt(config.KeyWorkersMultiplier)
		if multiplier <= 0 {
			multiplier = 4 // Fallback default multiplier
			logFields = append(logFields, zap.Int("effective_multiplier_fallback", multiplier))
		} else {
			logFields = append(logFields, zap.Int("effective_multiplier_config", multiplier))
		}
		calculatedWorkers := GetMaxProcs() * multiplier
		numWorkers = calculatedWorkers
		logFields = append(logFields, zap.String("reason", "calculated_gomaxprocs_x_multiplier"), zap.Int("gomaxprocs", GetMaxProcs()), zap.Int("calculated_value", numWorkers))
	}

	// 3. Ensure minimum number of workers
	minWorkers := cfg.GetInt(config.KeyMinWorkers)
	if minWorkers <= 0 {
		minWorkers = 2 // Fallback default minimum
	}
	if numWorkers < minWorkers {
		logFields = append(logFields, zap.Int("original_value_before_min_cap", numWorkers), zap.Int("min_workers_cap", minWorkers))
		numWorkers = minWorkers
		logFields = append(logFields, zap.String("reason_for_final_value", "capped_at_min_workers"))
	}

	logFields = append(logFields, zap.Int("final_pool_size", numWorkers))
	log.Info(context.Background(), "Determined worker Pool size", logFields...)

	options := ants.Options{
		ExpiryDuration:   10 * time.Second, // Default, can be configured
		Nonblocking:      false,            // Block if Pool is full
		MaxBlockingTasks: 0,                // No limit on blocking tasks
		PanicHandler: func(err interface{}) {
			log.Error(context.Background(), "Worker panic recovered", zap.Any("panic_error", err))
		},
	}

	pool, err := ants.NewPool(numWorkers, ants.WithOptions(options))
	if err != nil {
		log.Error(context.Background(), "Failed to create worker Pool", zap.Error(err), zap.Int("attempted_size", numWorkers))
		return nil, fmt.Errorf("%w: %v", domain.ErrAntsPoolCreation, err)
	}

	log.Info(context.Background(), "Worker Pool initialized successfully", zap.Int("actual_pool_size", pool.Cap()))

	return &WorkerPool{
		configProvider: cfg,
		logger:         log.With(zap.String("component", "worker_pool")),
		Pool:           pool,
	}, nil
}

// Submit enqueues a task (function) to be executed by a worker goroutine.
// It returns an error if the task cannot be submitted (e.g., Pool is closed).
func (wp *WorkerPool) Submit(task func()) error {
	wp.wg.Add(1) // Increment counter before submitting
	err := wp.Pool.Submit(func() {
		defer wp.wg.Done() // Decrement counter when task finishes
		task()
	})
	if err != nil {
		wp.wg.Done() // Decrement if submission failed
		wp.logger.Error(context.Background(), "Failed to submit task to worker Pool", zap.Error(err))
		if err == ants.ErrPoolClosed {
			return fmt.Errorf("%w: Pool is closed", domain.ErrTaskSubmissionToPool)
		}
		return fmt.Errorf("%w: %v", domain.ErrTaskSubmissionToPool, err)
	}
	return nil
}

// Release stops the worker Pool and waits for all submitted tasks to complete.
func (wp *WorkerPool) Release() {
	wp.logger.Info(context.Background(), "Releasing worker Pool, waiting for tasks to complete...")
	wp.Pool.Release() // Stop accepting new tasks
	wp.wg.Wait()      // Wait for all submitted tasks to finish
	wp.logger.Info(context.Background(), "Worker Pool released, all tasks completed.")
}

// Running returns the number of workers currently running.
func (wp *WorkerPool) Running() int {
	return wp.Pool.Running()
}

// Cap returns the capacity of the Pool.
func (wp *WorkerPool) Cap() int {
	return wp.Pool.Cap()
}

// Free returns the number of available workers.
func (wp *WorkerPool) Free() int {
	return wp.Pool.Free()
}
