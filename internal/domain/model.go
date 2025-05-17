package domain

import (
	"reflect"
	"strings"
	"time"
)

// EventID represents a unique identifier for a CDC event, typically in the format LSN:table:PK.
type EventID string

// AllowedTables specifies the set of tables that this service will process.
// Events from other tables will be acknowledged and skipped.
var AllowedTables = map[string]struct{}{
	"messages": {},
	"chats":    {},
	"agents":   {},
}

// CDCEventData represents the relevant data from a Sequin Change Data Capture event.
// It's structured to facilitate unmarshalling the Sequin JSON message.
type CDCEventData struct {
	Record   map[string]interface{} `json:"record"`            // The new row data, maps to Sequin's 'record'
	Changes  map[string]interface{} `json:"changes,omitempty"` // Previous values for changed fields, maps to Sequin's 'changes'
	Action   string                 `json:"action"`            // "insert", "update", "delete", "read", maps to Sequin's 'action'
	Metadata struct {
		// TableSchema string `json:"table_schema"` // Can be added if needed
		TableName string `json:"table_name"` // Name of the table that changed
		// CommitTimestamp string `json:"commit_timestamp"` // Can be added if needed
		CommitLSN interface{} `json:"commit_lsn"` // Logical replication LSN (Sequin doc says integer, using interface{} for flexibility before converting to string)
		// IdempotencyKey string `json:"idempotency_key"` // Can be added if needed
	} `json:"metadata"` // Maps to Sequin's 'metadata' object

	// PK is intended to store the extracted primary key(s) for easier access by the application.
	// This field will be populated by application logic after the initial JSON unmarshalling,
	// by inspecting the Record field. It's marked `json:"-"` to be ignored during unmarshalling.
	PK map[string]interface{} `json:"-"`

	// TypedData holds the strongly-typed representation of Record after parsing, based on table name.
	// Only one of these will be populated.
	TypedData interface{} `json:"-"` // Can be *AgentData, *ChatData, or *MessageData
}

// getJSONFieldName extracts the primary JSON field name from a struct tag.
func getJSONFieldName(field reflect.StructField) string {
	jsonTag := field.Tag.Get("json")
	if jsonTag == "" || jsonTag == "-" {
		return "" // No JSON tag or explicitly ignored
	}
	return strings.Split(jsonTag, ",")[0]
}

// AgentData represents the fields extracted from an 'agents' table CDC event record.
// Fields are based on the provided Agent struct, focusing on those relevant to the consumer.
// We will unmarshal cdcEventData.Record into this.
type AgentData struct {
	AgentID      string      `json:"agent_id"`
	CompanyID    string      `json:"company_id,omitempty"`
	QRCode       string      `json:"qr_code,omitempty"`
	Status       string      `json:"status,omitempty"`
	AgentName    string      `json:"agent_name,omitempty"`
	HostName     string      `json:"host_name,omitempty"`
	Version      string      `json:"version,omitempty"`
	CreatedAt    time.Time   `json:"created_at,omitempty"`
	UpdatedAt    time.Time   `json:"updated_at,omitempty"`
	LastMetadata interface{} `json:"last_metadata,omitempty"` // Was datatypes.JSON
}

// GetKnownJSONFields returns a set of JSON field names known to AgentData.
func (ad *AgentData) GetKnownJSONFields() map[string]struct{} {
	known := make(map[string]struct{})
	t := reflect.TypeOf(*ad)
	for i := 0; i < t.NumField(); i++ {
		if jsonName := getJSONFieldName(t.Field(i)); jsonName != "" {
			known[jsonName] = struct{}{}
		}
	}
	return known
}

// ChatData represents the fields extracted from a 'chats' table CDC event record.
type ChatData struct {
	ChatID                string      `json:"id"` // Note: your Chat struct uses `json:"id"` for ChatID
	AgentID               string      `json:"agent_id,omitempty"`
	CompanyID             string      `json:"company_id,omitempty"`
	Jid                   string      `json:"jid,omitempty"`
	CustomName            string      `json:"custom_name,omitempty"`
	PushName              string      `json:"push_name,omitempty"`
	IsGroup               bool        `json:"is_group,omitempty"`
	GroupName             string      `json:"group_name,omitempty"`
	UnreadCount           int32       `json:"unread_count,omitempty"`
	AssignedTo            string      `json:"assigned_to,omitempty"`
	LastMessageObj        interface{} `json:"last_message,omitempty"` // Was datatypes.JSON
	ConversationTimestamp int64       `json:"conversation_timestamp,omitempty"`
	NotSpam               bool        `json:"not_spam,omitempty"`
	PhoneNumber           string      `json:"phone_number,omitempty"`
	LastMetadata          interface{} `json:"last_metadata,omitempty"` // Was datatypes.JSON
	CreatedAt             time.Time   `json:"created_at,omitempty"`
	UpdatedAt             time.Time   `json:"updated_at,omitempty"`
}

// GetKnownJSONFields returns a set of JSON field names known to ChatData.
func (cd *ChatData) GetKnownJSONFields() map[string]struct{} {
	known := make(map[string]struct{})
	t := reflect.TypeOf(*cd)
	for i := 0; i < t.NumField(); i++ {
		if jsonName := getJSONFieldName(t.Field(i)); jsonName != "" {
			known[jsonName] = struct{}{}
		}
	}
	return known
}

// MessageData represents the fields extracted from a 'messages' table CDC event record.
type MessageData struct {
	MessageID        string      `json:"id"` // Note: your Message struct uses `json:"id"` for MessageID
	ChatID           string      `json:"chat_id,omitempty"`
	AgentID          string      `json:"agent_id,omitempty"`
	CompanyID        string      `json:"company_id,omitempty"`
	From             string      `json:"from,omitempty"`
	To               string      `json:"to,omitempty"`
	Jid              string      `json:"jid,omitempty"`
	Flow             string      `json:"flow,omitempty"`
	MessageObj       interface{} `json:"message_obj,omitempty"` // Was datatypes.JSON
	Key              interface{} `json:"key,omitempty"`         // Was datatypes.JSON
	Status           string      `json:"status,omitempty"`
	IsDeleted        bool        `json:"is_deleted,omitempty"`
	MessageTimestamp int64       `json:"message_timestamp,omitempty"`
	MessageDate      time.Time   `json:"message_date"` // Assuming it's not omitempty since not null
	CreatedAt        time.Time   `json:"created_at,omitempty"`
	UpdatedAt        time.Time   `json:"updated_at,omitempty"`
	LastMetadata     interface{} `json:"last_metadata,omitempty"` // Was datatypes.JSON
}

// GetKnownJSONFields returns a set of JSON field names known to MessageData.
func (md *MessageData) GetKnownJSONFields() map[string]struct{} {
	known := make(map[string]struct{})
	t := reflect.TypeOf(*md)
	for i := 0; i < t.NumField(); i++ {
		if jsonName := getJSONFieldName(t.Field(i)); jsonName != "" {
			known[jsonName] = struct{}{}
		}
	}
	return known
}

// EnrichedEventPayload is the structure of the event published by this service
// to downstream consumers (e.g., daisi-ws-service). It includes routing
// metadata and the core data from the original table row.
type EnrichedEventPayload struct {
	EventID string `json:"event_id"`          // Derived unique event ID (e.g., LSN:Table:PKs)
	AgentID string `json:"agent_id"`          // Agent ID associated with the event
	ChatID  string `json:"chat_id,omitempty"` // Chat ID, relevant primarily for message events

	// RowData contains the actual data from the table's row involved in the CDC event.
	// This is typically the `Data` field from CDCEventData, potentially after normalization.
	RowData map[string]interface{} `json:"row_data"`
}
