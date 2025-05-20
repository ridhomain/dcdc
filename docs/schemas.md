# Schemas and NATS Subjects

This document outlines the JSON schema for events published by the Daisi CDC Consumer Service and the NATS subject patterns used for publishing.

## 1. Published JSON Schema (`EnrichedEventPayload`)

The service publishes messages with the following JSON structure, derived from the `EnrichedEventPayload` Go struct.

```json
{
  "type": "object",
  "properties": {
    "event_id": {
      "type": "string",
      "description": "Unique identifier for the event, typically in the format LSN:TableName:PK. Example: '1632321:messages:msg-789'"
    },
    "company_id": {
      "type": "string",
      "description": "The company ID associated with the event. This is a mandatory field."
    },
    "agent_id": {
      "type": "string",
      "description": "The agent ID associated with the event. Present if applicable, otherwise empty. Mandatory for routing."
    },
    "message_id": {
      "type": "string",
      "description": "The message ID, primarily for events originating from the 'messages' table. Present if applicable, otherwise empty."
    },
    "chat_id": {
      "type": "string",
      "description": "The chat ID, relevant for events from 'messages' and 'chats' tables. Present if applicable, otherwise empty."
    },
    "row_data": {
      "type": "object",
      "description": "Contains the actual data from the table's row involved in the CDC event. The structure of this object depends on the source table ('messages', 'chats', or 'agents').",
      "additionalProperties": true,
      "examples": [
        {
          "comment": "Example for 'messages' table",
          "id": 12345,
          "message_id": "msg-789",
          "chat_id": "chat-456",
          "agent_id": "agent-123",
          "company_id": "test-company",
          "from": "user@example.com",
          "to": "agent@example.com",
          "jid": "user@example.com/resource",
          "flow": "inbound",
          "message_obj": {"type": "text", "text": "Hello world"},
          "key": {"id": "key-abc", "remoteJid": "user@example.com", "fromMe": false},
          "status": "delivered",
          "is_deleted": false,
          "message_timestamp": 1678886400000,
          "message_date": "2025-03-15",
          "created_at": "2025-03-15T10:00:00Z",
          "updated_at": "2025-03-15T10:00:00Z",
          "last_metadata": null
        },
        {
          "comment": "Example for 'chats' table",
          "id": 67890,
          "chat_id": "chat-456",
          "agent_id": "agent-123",
          "company_id": "test-company",
          "jid": "group-jid@g.us",
          "custom_name": "Support Chat",
          "push_name": "Wira",
          "is_group": true,
          "group_name": "Customer Support Q1",
          "unread_count": 3,
          "assigned_to": "agent-123",
          "last_message": {"text": "Thanks!"},
          "conversation_timestamp": 1678886400000,
          "not_spam": true,
          "phone_number": "1234567890",
          "last_metadata": null,
          "created_at": "2025-03-15T09:00:00Z",
          "updated_at": "2025-03-15T09:30:00Z"
        },
        {
          "comment": "Example for 'agents' table",
          "id": 11223,
          "agent_id": "agent-123",
          "company_id": "test-company",
          "qr_code": "base64encodedqrcode==",
          "status": "online",
          "agent_name": "Wira Agent",
          "host_name": "agent-desktop-01",
          "version": "1.2.3",
          "created_at": "2025-01-10T12:00:00Z",
          "updated_at": "2025-03-01T15:00:00Z",
          "last_metadata": null
        }
      ]
    }
  },
  "required": [
    "event_id",
    "company_id",
    "row_data"
  ]
}
```

### 2.1 `RowData` Structure Details

The `row_data` field contains a direct mapping of the columns from the source database table.

*   **For `messages` table:** The `row_data` will conform to the fields defined in `domain.MessageData`.
*   **For `chats` table:** The `row_data` will conform to the fields defined in `domain.ChatData`.
*   **For `agents` table:** The `row_data` will conform to the fields defined in `domain.AgentData`.

Refer to `internal/domain/model.go` for the exact Go struct definitions which correspond to these JSON structures. Fields like `message_obj`, `key`, and `last_metadata` can be complex JSON objects themselves.

## 2. NATS Publish Subject Patterns

The service publishes enriched events to a NATS JetStream currently named **`wa_stream`** (this can be configured via the `DAISI_CDC_JS_WA_STREAM_NAME` environment variable).

The subject patterns depend on the source table of the CDC event:

*   **For `messages` table events:**
    *   Pattern: `wa.<company_id>.<agent_id>.messages.<chat_id>`
    *   Example: `wa.company_xyz.agent_123.messages.chat_abc`

*   **For `chats` table events:**
    *   Pattern: `wa.<company_id>.<agent_id>.chats`
    *   Example: `wa.company_xyz.agent_123.chats`

*   **For `agents` table events:**
    *   Pattern: `wa.<company_id>.<agent_id>.agents`
    *   Example: `wa.company_xyz.agent_123.agents`

Where:
*   `<company_id>`: The ID of the company associated with the event.
*   `<agent_id>`: The ID of the agent associated with the event.
*   `<chat_id>`: The ID of the chat, specifically for message events.

Downstream services should subscribe to these subjects on the `wa_stream` (or its configured name) to receive the processed CDC events. 