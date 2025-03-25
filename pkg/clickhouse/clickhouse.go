package clickhouse

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/cespare/xxhash"
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
		IndexKeyColumn +
		") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

	// hexChars contains the characters used for hex representation
	hexChars = "0123456789abcdef"
)

// CloudEventToSlice converts a CloudEvent to an array of any for Clickhouse insertion.
// The order of the elements in the array match the order of the columns in the table.
func CloudEventToSlice(event *cloudevent.CloudEventHeader) []any {
	return CloudEventToSliceWithKey(event, CloudEventToObjectKey(event))
}

// CloudEventToSliceWithKey converts a CloudEvent to an array of any for Clickhouse insertion.
// The order of the elements in the array match the order of the columns in the table.
func CloudEventToSliceWithKey(event *cloudevent.CloudEventHeader, key string) []any {
	jsonExtra, _ := json.Marshal(event.Extras)
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
		key,
	}
}

// UnmarshalCloudEventSlice unmarshals a byte slice into an array of any for Clickhouse insertion.
func UnmarshalCloudEventSlice(jsonArray []byte) ([]any, error) {
	rawSlice := []json.RawMessage{}
	err := json.Unmarshal(jsonArray, &rawSlice)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal cloud event slice: %w", err)
	}
	if len(rawSlice) != 10 {
		return nil, fmt.Errorf("invalid cloud event slice length: %d", len(rawSlice))
	}
	var subject string
	var timestamp time.Time
	var eventType string
	var id string
	var source string
	var producer string
	var dataContentType string
	var dataVersion string
	var extras string
	var indexKey string
	err = json.Unmarshal(rawSlice[0], &subject)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal subject: %w", err)
	}
	err = json.Unmarshal(rawSlice[1], &timestamp)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal timestamp: %w", err)
	}
	err = json.Unmarshal(rawSlice[2], &eventType)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal event type: %w", err)
	}
	err = json.Unmarshal(rawSlice[3], &id)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal id: %w", err)
	}
	err = json.Unmarshal(rawSlice[4], &source)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal source: %w", err)
	}
	err = json.Unmarshal(rawSlice[5], &producer)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal producer: %w", err)
	}
	err = json.Unmarshal(rawSlice[6], &dataContentType)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal data content type: %w", err)
	}
	err = json.Unmarshal(rawSlice[7], &dataVersion)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal data version: %w", err)
	}
	err = json.Unmarshal(rawSlice[8], &extras)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal extras: %w", err)
	}
	err = json.Unmarshal(rawSlice[9], &indexKey)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal index key: %w", err)
	}
	return []any{subject, timestamp, eventType, id, source, producer, dataContentType, dataVersion, extras, indexKey}, nil
}

// CloudEventToObjectKey generates a unique key for storing cloud events.
// The key is composed of the event's key with a hex prefix derived from the hash of the key.
func CloudEventToObjectKey(event *cloudevent.CloudEventHeader) string {
	if event == nil {
		return ""
	}
	key := event.Key()

	// Hash the base key and extract the first hex digit
	hash := xxhash.Sum64String(key)
	firstDigit := hash >> 60 // Extract first hex digit by shifting right 60 bits (getting highest 4 bits)
	hexPrefix := hexChars[firstDigit]

	// Create final key with hex prefix
	return string(hexPrefix) + key
}
