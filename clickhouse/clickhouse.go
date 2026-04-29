package clickhouse

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/cespare/xxhash/v2"
)

const (
	// TableName is the name of the table in Clickhouse.
	TableName = "cloud_event"
	// SubjectColumn is the name of the subject column in Clickhouse.
	SubjectColumn = "subject"
	// TimestampColumn is the name of the timestamp column in Clickhouse.
	TimestampColumn = "event_time"
	// TypeColumn is the name of the cloud event type column in Clickhouse.
	TypeColumn = "event_type"
	// IDColumn is the name of the ID column in Clickhouse.
	IDColumn = "id"
	// SourceColumn is the name of the source column in Clickhouse.
	SourceColumn = "source"
	// ProducerColumn is the name of the producer column in Clickhouse.
	ProducerColumn = "producer"
	// DataContentTypeColumn is the name of the data content type column in Clickhouse.
	DataContentTypeColumn = "data_content_type"
	// DataVersionColumn is the name of the data version column in Clickhouse.
	DataVersionColumn = "data_version"
	// ExtraColumn is the name of the extra column in Clickhouse.
	ExtrasColumn = "extras"
	// IndexKeyColumn is the name of the index name column in Clickhouse.
	IndexKeyColumn = "index_key"
	// DataIndexKeyColumn is the name of the data index name column in Clickhouse.
	DataIndexKeyColumn = "data_index_key"
	// VoidsIDColumn is the name of the voids_id column in Clickhouse. For
	// dimo.tombstone events it holds the id of the attestation being
	// tombstoned; empty for all other event types.
	VoidsIDColumn = "voids_id"

	// InsertStmt is the SQL statement for inserting a row into Clickhouse.
	InsertStmt = "INSERT INTO " + TableName + " (" +
		SubjectColumn + ", " +
		TimestampColumn + ", " +
		TypeColumn + ", " +
		IDColumn + ", " +
		SourceColumn + ", " +
		ProducerColumn + ", " +
		DataContentTypeColumn + ", " +
		DataVersionColumn + ", " +
		ExtrasColumn + ", " +
		IndexKeyColumn + ", " +
		DataIndexKeyColumn + ", " +
		VoidsIDColumn +
		") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

	// hexChars contains the characters used for hex representation
	hexChars = "0123456789abcdef"
)

// CloudEventToSlice converts a CloudEvent to an array of any for Clickhouse insertion.
// The order of the elements in the array match the order of the columns in the table.
// index_key is computed from the event's headers.
func CloudEventToSlice(event *cloudevent.CloudEventHeader) []any {
	return CloudEventToSliceWithKey(event, CloudEventToObjectKey(event))
}

// CloudEventToSliceWithKey converts a CloudEvent to an array of any for Clickhouse insertion.
// The order of the elements in the array match the order of the columns in the table.
// This variant allows the caller to specify a value for index_key.
func CloudEventToSliceWithKey(event *cloudevent.CloudEventHeader, key string) []any {
	return cloudEventToSlice(event, key, "", "")
}

// StoredEventToSlice converts a StoredEvent to an array of any for Clickhouse
// insertion, populating index_key (caller-supplied), data_index_key, and
// voids_id (both carried on the wrapper). The order of the elements matches
// the column order in the table.
func StoredEventToSlice(stored *cloudevent.StoredEvent, indexKey string) []any {
	return cloudEventToSlice(&stored.CloudEventHeader, indexKey, stored.DataIndexKey, stored.VoidsID)
}

// TombstoneEventToSlice converts a tombstone CloudEvent to an array of any for
// Clickhouse insertion, populating index_key (caller-supplied) and voids_id
// (the id of the attestation being tombstoned). data_index_key is left empty:
// tombstone payloads are small and never externalized.
func TombstoneEventToSlice(event *cloudevent.CloudEventHeader, indexKey, voidsID string) []any {
	return cloudEventToSlice(event, indexKey, "", voidsID)
}

func cloudEventToSlice(event *cloudevent.CloudEventHeader, indexKey, dataIndexKey, voidsID string) []any {
	// Add non-column fields to extras
	extras := cloudevent.AddNonColumnFieldsToExtras(event)

	var jsonExtra []byte
	if extras == nil {
		jsonExtra = []byte("{}")
	} else {
		jsonExtra, _ = json.Marshal(extras)
	}
	return []any{
		event.Subject,
		event.Time,
		event.Type,
		event.ID,
		event.Source,
		event.Producer,
		event.DataContentType,
		event.DataVersion,
		string(jsonExtra),
		indexKey,
		dataIndexKey,
		voidsID,
	}
}

// UnmarshalCloudEventSlice unmarshals a byte slice into an array of any for Clickhouse insertion.
func UnmarshalCloudEventSlice(jsonArray []byte) ([]any, error) {
	var rawSlice []json.RawMessage
	if err := json.Unmarshal(jsonArray, &rawSlice); err != nil {
		return nil, fmt.Errorf("failed to unmarshal cloud event slice: %w", err)
	}
	if len(rawSlice) != 12 {
		return nil, fmt.Errorf("invalid cloud event slice length: %d", len(rawSlice))
	}

	// Column order: subject, timestamp, eventType, id, source, producer, dataContentType, dataVersion, extras, indexKey, dataIndexKey, voidsID
	var (
		subject         string
		timestamp       time.Time
		eventType       string
		id              string
		source          string
		producer        string
		dataContentType string
		dataVersion     string
		extras          string
		indexKey        string
		dataIndexKey    string
		voidsID         string
	)
	unmarshal := func(i int, name string, ptr any) error {
		if err := json.Unmarshal(rawSlice[i], ptr); err != nil {
			return fmt.Errorf("failed to unmarshal %s: %w", name, err)
		}
		return nil
	}
	if err := unmarshal(0, "subject", &subject); err != nil {
		return nil, err
	}
	if err := unmarshal(1, "timestamp", &timestamp); err != nil {
		return nil, err
	}
	if err := unmarshal(2, "event type", &eventType); err != nil {
		return nil, err
	}
	if err := unmarshal(3, "id", &id); err != nil {
		return nil, err
	}
	if err := unmarshal(4, "source", &source); err != nil {
		return nil, err
	}
	if err := unmarshal(5, "producer", &producer); err != nil {
		return nil, err
	}
	if err := unmarshal(6, "data content type", &dataContentType); err != nil {
		return nil, err
	}
	if err := unmarshal(7, "data version", &dataVersion); err != nil {
		return nil, err
	}
	if err := unmarshal(8, "extras", &extras); err != nil {
		return nil, err
	}
	if err := unmarshal(9, "index key", &indexKey); err != nil {
		return nil, err
	}
	if err := unmarshal(10, "data index key", &dataIndexKey); err != nil {
		return nil, err
	}
	if err := unmarshal(11, "voids id", &voidsID); err != nil {
		return nil, err
	}
	return []any{subject, timestamp, eventType, id, source, producer, dataContentType, dataVersion, extras, indexKey, dataIndexKey, voidsID}, nil
}

// CloudEventToObjectKey generates a unique key for storing cloud events.
// The key is composed of the event's key with a hex prefix derived from the hash of the key.
func CloudEventToObjectKey(event *cloudevent.CloudEventHeader) string {
	if event == nil {
		return ""
	}
	key := event.Key()

	hash := xxhash.Sum64String(key)
	firstDigit := hash >> 60

	var b strings.Builder
	b.Grow(1 + len(key))
	b.WriteByte(hexChars[firstDigit])
	b.WriteString(key)
	return b.String()
}
