package application

import (
	"context"
	"fmt"

	jsoniter "github.com/json-iterator/go"
	"gitlab.com/timkado/api/daisi-cdc-consumer-service/internal/domain"
	"go.uber.org/zap"
)

var tsJson = jsoniter.ConfigFastest // Renamed to avoid conflict if consumer.go is in same view

// EventTransformer defines the interface for transforming raw CDC data into a publishable format.
type EventTransformer interface {
	TransformAndEnrich(ctx context.Context, cdcEventData *domain.CDCEventData, originalSubject string, tableName string) (*domain.EnrichedEventPayload, string /* targetSubject */, []byte /* payloadBytes */, error)
}

// transformService implements the EventTransformer interface.
type transformService struct {
	logger      domain.Logger
	metricsSink domain.MetricsSink
}

// NewTransformService creates a new instance of transformService.
func NewTransformService(logger domain.Logger, metricsSink domain.MetricsSink) EventTransformer {
	return &transformService{
		logger:      logger.With(zap.String("component", "transform_service")),
		metricsSink: metricsSink,
	}
}

// populateTypedDataAndGetUnhandledFields processes the raw record into a typed struct
// and identifies unhandled fields.
// This function is moved from consumer.go
func (ts *transformService) populateTypedDataAndGetUnhandledFields(rawRecord map[string]interface{}, tableName string, typedDataTarget interface{}) (unhandledFields []string, err error) {
	rawRecordBytes, marshalErr := tsJson.Marshal(rawRecord)
	if marshalErr != nil {
		return nil, domain.NewErrDataProcessing("marshal_raw_record_for_typed", tableName, marshalErr)
	}

	if unmarshalErr := tsJson.Unmarshal(rawRecordBytes, typedDataTarget); unmarshalErr != nil {
		return nil, domain.NewErrDataProcessing("unmarshal_to_typed_data", tableName, unmarshalErr)
	}

	var knownJsonFields map[string]struct{}
	switch td := typedDataTarget.(type) {
	case *domain.AgentData:
		knownJsonFields = td.GetKnownJSONFields()
	case *domain.ChatData:
		knownJsonFields = td.GetKnownJSONFields()
	case *domain.MessageData:
		knownJsonFields = td.GetKnownJSONFields()
	default:
		return nil, domain.NewErrDataProcessing("get_known_fields_unknown_type", tableName, fmt.Errorf("unknown typedDataTarget type: %T", td))
	}

	for key := range rawRecord {
		if _, isKnown := knownJsonFields[key]; !isKnown {
			unhandledFields = append(unhandledFields, key)
		}
	}
	return unhandledFields, nil
}

// extractPKValue now uses the typed data structs.
// This function is moved from consumer.go
func (ts *transformService) extractPKValue(typedData interface{}, tableName string) (string, error) {
	switch data := typedData.(type) {
	case *domain.AgentData:
		if data.AgentID == "" {
			return "", domain.NewErrDataProcessing("extract_pk", tableName, domain.ErrPKEmpty)
		}
		return data.AgentID, nil
	case *domain.ChatData:
		if data.ChatID == "" {
			return "", domain.NewErrDataProcessing("extract_pk", tableName, domain.ErrPKEmpty)
		}
		return data.ChatID, nil
	case *domain.MessageData:
		if data.MessageID == "" {
			return "", domain.NewErrDataProcessing("extract_pk", tableName, domain.ErrPKEmpty)
		}
		return data.MessageID, nil
	default:
		return "", domain.NewErrDataProcessing("extract_pk_unknown_type", tableName, fmt.Errorf("cannot extract PK from unknown typed data structure %T", typedData))
	}
}

// TransformAndEnrich implements the EventTransformer interface.
func (ts *transformService) TransformAndEnrich(ctx context.Context, cdcEventData *domain.CDCEventData, originalSubject string, tableNameFromSubject string) (*domain.EnrichedEventPayload, string, []byte, error) {
	// Use the table name from Sequin's metadata as the authoritative source.
	authoritativeTableName := cdcEventData.Metadata.TableName
	if authoritativeTableName == "" {
		ts.logger.Error(ctx, "Authoritative table name from Sequin metadata is empty", zap.String("subject_table_name", tableNameFromSubject))
		return nil, "", nil, domain.NewErrDataProcessing("empty_metadata_table_name", tableNameFromSubject, fmt.Errorf("authoritative table name missing from Sequin metadata"))
	}

	// Log if subject-derived table name differs from metadata, but proceed with metadata's.
	if tableNameFromSubject != "" && authoritativeTableName != tableNameFromSubject {
		ts.logger.Warn(ctx, "Mismatch between NATS subject table and payload metadata table. Using metadata table name.",
			zap.String("subject_table", tableNameFromSubject),
			zap.String("payload_table", authoritativeTableName))
	}

	// Populate typed data and identify unhandled fields
	var typedDataForTable interface{}
	switch authoritativeTableName { // Use authoritativeTableName
	case "agents":
		typedDataForTable = new(domain.AgentData)
	case "chats":
		typedDataForTable = new(domain.ChatData)
	case "messages":
		typedDataForTable = new(domain.MessageData)
	default:
		ts.logger.Error(ctx, "Unknown table name for typed data unmarshalling", zap.String("authoritative_table_name", authoritativeTableName))
		return nil, "", nil, domain.ErrUnknownTableNameForTransform // Sentinel error
	}

	unhandledFields, typePopulationErr := ts.populateTypedDataAndGetUnhandledFields(cdcEventData.Record, authoritativeTableName, typedDataForTable)
	if typePopulationErr != nil {
		ts.logger.Error(ctx, "Failed to populate typed data or identify unhandled fields", zap.Error(typePopulationErr), zap.String("authoritative_table_name", authoritativeTableName))
		ts.metricsSink.IncEventsTotal(authoritativeTableName, "typed_data_population_error")
		return nil, "", nil, typePopulationErr // Already a domain.ErrDataProcessing
	}

	if len(unhandledFields) > 0 {
		ts.logger.Warn(ctx, "Unhandled fields detected in CDC record", zap.String("authoritative_table_name", authoritativeTableName), zap.Strings("unhandled_fields", unhandledFields))
		for _, fieldName := range unhandledFields {
			ts.metricsSink.IncUnhandledFieldsTotal(authoritativeTableName, fieldName)
		}
	}
	cdcEventData.TypedData = typedDataForTable

	// Extract Primary Key (PK)
	pkValueStr, err := ts.extractPKValue(cdcEventData.TypedData, authoritativeTableName) // Use authoritativeTableName
	if err != nil {
		ts.logger.Error(ctx, "Failed to extract PK for EventID generation", zap.Error(err))
		ts.metricsSink.IncEventsTotal(authoritativeTableName, "pk_extraction_error")
		return nil, "", nil, err // Already a domain.ErrDataProcessing
	}

	// Construct robust EventID (LSN:Table:PK)
	commitLSNStr := fmt.Sprintf("%v", cdcEventData.Metadata.CommitLSN)
	eventIDStr := fmt.Sprintf("%s:%s:%s", commitLSNStr, authoritativeTableName, pkValueStr) // Use authoritativeTableName
	// Note: eventID is part of EnrichedEventPayload, context update for logging happens in consumer or by logger itself.

	// Extract agent_id, chat_id, and authoritativeCompanyID from payload.
	var agentID, chatID, authoritativeCompanyID string
	switch data := cdcEventData.TypedData.(type) {
	case *domain.AgentData:
		agentID = data.AgentID
		authoritativeCompanyID = data.CompanyID
	case *domain.ChatData:
		agentID = data.AgentID
		chatID = data.ChatID
		authoritativeCompanyID = data.CompanyID
	case *domain.MessageData:
		agentID = data.AgentID
		chatID = data.ChatID
		authoritativeCompanyID = data.CompanyID
	default:
		ts.logger.Error(ctx, "Unhandled typed data structure for ID extraction")
		// This path should be rare given the switch on tableName for typedDataForTable instantiation
		return nil, "", nil, domain.NewErrDataProcessing("id_extraction_unknown_type", authoritativeTableName, fmt.Errorf("unhandled typed data for ID extraction: %T", data))
	}

	if authoritativeCompanyID == "" {
		ts.logger.Error(ctx, domain.ErrMissingCompanyID.Error(), zap.String("authoritative_table_name", authoritativeTableName))
		return nil, "", nil, domain.ErrMissingCompanyID
	}
	if agentID == "" {
		ts.logger.Error(ctx, domain.ErrAgentIDEmpty.Error(), zap.String("authoritative_table_name", authoritativeTableName))
		return nil, "", nil, domain.ErrAgentIDEmpty
	}

	// Create EnrichedEventPayload
	enrichedPayload := &domain.EnrichedEventPayload{
		EventID: eventIDStr,
		AgentID: agentID,
		ChatID:  chatID,
		RowData: cdcEventData.Record, // Original full record
	}

	// Determine Target Publish Subject
	var targetSubject string
	switch authoritativeTableName { // Use authoritativeTableName
	case "messages":
		if chatID == "" {
			ts.logger.Error(ctx, domain.ErrChatIDMissingForMessages.Error(), zap.String("table", authoritativeTableName))
			return nil, "", nil, domain.ErrChatIDMissingForMessages
		}
		targetSubject = fmt.Sprintf("wa.%s.%s.messages.%s", authoritativeCompanyID, agentID, chatID)
	case "chats":
		targetSubject = fmt.Sprintf("wa.%s.%s.chats", authoritativeCompanyID, agentID)
	case "agents":
		targetSubject = fmt.Sprintf("wa.%s.%s.agents", authoritativeCompanyID, agentID)
	default:
		return nil, "", nil, domain.ErrUnknownTableNameForTransform // Should have been caught earlier
	}

	// Marshal EnrichedEventPayload to JSON
	payloadBytes, err := tsJson.Marshal(enrichedPayload)
	if err != nil {
		ts.logger.Error(ctx, "Failed to marshal enriched event payload", zap.Error(err))
		ts.metricsSink.IncEventsTotal(authoritativeTableName, "marshal_error")
		return nil, "", nil, domain.NewErrDataProcessing("marshal_enriched_payload", authoritativeTableName, err)
	}

	return enrichedPayload, targetSubject, payloadBytes, nil
}
