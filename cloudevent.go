// Package cloudevent provides types for working with CloudEvents.
package cloudevent

import (
	"encoding/json"
	"mime"
	"strings"
	"time"
)

// SpecVersion is the version of the CloudEvents spec.
const SpecVersion = "1.0"

// CloudEventHeader contains the metadata for any CloudEvent.
// Field order matches the JSON Event Format: specversion, type, source, subject, id, time, then optional/extension attributes.
// See https://github.com/cloudevents/spec/blob/main/cloudevents/formats/json-format.md
// To add extra headers to the CloudEvent, add them to the Extras map.
type CloudEventHeader struct {
	// SpecVersion is the version of CloudEvents specification used.
	// This is always hardcoded "1.0".
	SpecVersion string `json:"specversion"`

	// Type describes the type of event. It should generally be a reverse-DNS
	// name.
	Type string `json:"type"`

	// Source is the context in which the event happened. In a distributed system it might consist of multiple Producers.
	Source string `json:"source"`

	// Subject is an optional field identifying the subject of the event within
	// the context of the event producer. In practice, we always set this.
	Subject string `json:"subject"`

	// ID is an identifier for the event. The combination of ID and Source must
	// be unique.
	ID string `json:"id"`

	// Time is an optional field giving the time at which the event occurred. In
	// practice, we always set this.
	Time time.Time `json:"time"`

	// DataContentType is an optional MIME type for the data field. We almost
	// always serialize to JSON and in that case this field is implicitly
	// "application/json".
	DataContentType string `json:"datacontenttype,omitempty"`

	// DataSchema is an optional URI pointing to a schema for the data field.
	DataSchema string `json:"dataschema,omitempty"`

	// DataVersion is the version of the data type.
	DataVersion string `json:"dataversion,omitempty"`

	// Producer is a specific instance, process or device that creates the data structure describing the CloudEvent.
	Producer string `json:"producer"`

	// Signature hold the signature of the a cloudevent's data field.
	Signature string `json:"signature,omitempty"`

	// Tags are a list of tags that can be used to filter events.
	Tags []string `json:"tags,omitempty"`

	// Extras contains any additional fields that are not part of the CloudEvent excluding the data field.
	Extras map[string]any `json:"-"`
}

// CloudEvent represents an event according to the CloudEvents spec.
// To Add extra headers to the CloudEvent, add them to the Extras map.
// See https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md
type CloudEvent[A any] struct {
	CloudEventHeader
	// Data contains domain-specific information about the event.
	Data A `json:"data"`

	DataBase64 string `json:"data_base64,omitempty"`
}

// RawEvent is a cloudevent with a json.RawMessage data field.
// It supports both "data" and "data_base64" (CloudEvents JSON spec).
type RawEvent = CloudEvent[json.RawMessage]

// BytesForSignature returns the bytes that were signed (wire form of data or data_base64) for a RawEvent.
// Use for signature verification; not the same as Data when the CE used data_base64.
func BytesForSignature(ev RawEvent) []byte {
	if ev.DataBase64 != "" {
		return []byte(ev.DataBase64)
	}
	return ev.Data
}

// IsJSONDataContentType returns true if the MIME type indicates a JSON payload.
// Matches "application/json" and any "+json" suffix type (e.g. "application/cloudevents+json").
func IsJSONDataContentType(ct string) bool {
	parsed, _, err := mime.ParseMediaType(strings.TrimSpace(ct))
	return err == nil && (parsed == "application/json" || strings.HasSuffix(parsed, "+json"))
}

// Equals returns true if the two CloudEventHeaders share the same IndexKey.
func (c *CloudEventHeader) Equals(other CloudEventHeader) bool {
	return c.Key() == other.Key()
}

// Key returns the unique identifier for the CloudEvent.
func (c CloudEventHeader) Key() string {
	timeStr := c.Time.Format(time.RFC3339)
	var b strings.Builder
	b.Grow(len(c.Subject) + 1 + len(timeStr) + 1 + len(c.Type) + 1 + len(c.Source) + 1 + len(c.ID))
	b.WriteString(c.Subject)
	b.WriteByte('!')
	b.WriteString(timeStr)
	b.WriteByte('!')
	b.WriteString(c.Type)
	b.WriteByte('!')
	b.WriteString(c.Source)
	b.WriteByte('!')
	b.WriteString(c.ID)
	return b.String()
}
