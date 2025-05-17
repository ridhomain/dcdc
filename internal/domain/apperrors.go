package domain

import (
	"errors"
	"fmt"
)

// --- Sentinel Errors (specific, fixed error instances) ---
var (
	// Data validation / quality errors (typically non-retryable from consumer perspective)
	ErrMissingCompanyID             = errors.New("authoritative companyID from payload is empty")
	ErrAgentIDEmpty                 = errors.New("extracted agent_id is empty")
	ErrChatIDMissingForMessages     = errors.New("chat_id is missing for messages table, cannot form target subject")
	ErrPKEmpty                      = errors.New("extracted primary key value is empty")
	ErrUnknownTableNameForTransform = errors.New("unknown table name for typed data unmarshalling or PK extraction")

	// System / configuration errors (potentially retryable or config issue)
	ErrAntsPoolCreation     = errors.New("failed to create new ants worker pool")
	ErrTaskSubmissionToPool = errors.New("failed to submit task to worker pool")
)

// --- Custom Error Structs (can wrap underlying errors) ---

// ErrDataProcessing wraps errors that occur during data processing stages like unmarshalling, population, PK extraction.
// It implies the issue is likely with the data itself.
type ErrDataProcessing struct {
	Stage string // e.g., "unmarshal_raw_cdc", "populate_typed_data", "pk_extraction", "marshal_enriched_payload"
	Table string // Optional, if relevant to the stage
	Err   error
}

func NewErrDataProcessing(stage, table string, cause error) *ErrDataProcessing {
	return &ErrDataProcessing{Stage: stage, Table: table, Err: cause}
}
func (e *ErrDataProcessing) Error() string {
	if e.Table != "" {
		return fmt.Sprintf("data processing failed at stage '%s' for table '%s': %v", e.Stage, e.Table, e.Err)
	}
	return fmt.Sprintf("data processing failed at stage '%s': %v", e.Stage, e.Err)
}
func (e *ErrDataProcessing) Unwrap() error { return e.Err }

// ErrExternalService wraps errors from external services like NATS publisher or Redis deduper.
// These might be transient and could warrant a retry (Nack).
type ErrExternalService struct {
	Service string // e.g., "NATS_publisher", "Redis_deduplicator"
	Err     error
}

func NewErrExternalService(service string, cause error) *ErrExternalService {
	return &ErrExternalService{Service: service, Err: cause}
}
func (e *ErrExternalService) Error() string {
	return fmt.Sprintf("external service '%s' operation failed: %v", e.Service, e.Err)
}
func (e *ErrExternalService) Unwrap() error { return e.Err }
