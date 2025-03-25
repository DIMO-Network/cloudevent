// Package cloudevent provides types for working with CloudEvents.
package cloudevent

import (
	"encoding/json"
	"time"
)

const (
	// TypeStatus is the event type for status updates.
	TypeStatus = "dimo.status"

	// TypeFingerprint is the event type for fingerprint updates.
	TypeFingerprint = "dimo.fingerprint"

	// TypeVerifableCredential is the event type for verifiable credentials.
	TypeVerifableCredential = "dimo.verifiablecredential" //nolint:gosec // This is not a credential.

	// TypeUnknown is the event type for unknown events.
	TypeUnknown = "dimo.unknown"
)

// SpecVersion is the version of the CloudEvents spec.
const SpecVersion = "1.0"

// CloudEventHeader contains the metadata for any CloudEvent.
// To add extra headers to the CloudEvent, add them to the Extras map.
type CloudEventHeader struct {
	// ID is an identifier for the event. The combination of ID and Source must
	// be unique.
	ID string `json:"id"`

	// Source is the context in which the event happened. In a distributed system it might consist of multiple Producers.
	Source string `json:"source"`

	// Producer is a specific instance, process or device that creates the data structure describing the CloudEvent.
	Producer string `json:"producer"`

	// SpecVersion is the version of CloudEvents specification used.
	// This is always hardcoded "1.0".
	SpecVersion string `json:"specversion"`

	// Subject is an optional field identifying the subject of the event within
	// the context of the event producer. In practice, we always set this.
	Subject string `json:"subject"`

	// Time is an optional field giving the time at which the event occurred. In
	// practice, we always set this.
	Time time.Time `json:"time"`

	// Type describes the type of event. It should generally be a reverse-DNS
	// name.
	Type string `json:"type"`

	// DataContentType is an optional MIME type for the data field. We almost
	// always serialize to JSON and in that case this field is implicitly
	// "application/json".
	DataContentType string `json:"datacontenttype,omitempty"`

	// DataSchema is an optional URI pointing to a schema for the data field.
	DataSchema string `json:"dataschema,omitempty"`

	// DataVersion is the version of the data type.
	DataVersion string `json:"dataversion,omitempty"`

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
}

// RawEvent is a cloudevent with a json.RawMessage data field.
type RawEvent = CloudEvent[json.RawMessage]

// Equals returns true if the two CloudEventHeaders share the same IndexKey.
func (c *CloudEventHeader) Equals(other CloudEventHeader) bool {
	return c.Key() == other.Key()
}

// Key returns the unique identifier for the CloudEvent.
func (c CloudEventHeader) Key() string {
	return c.Subject + ":" + c.Time.Format(time.RFC3339) + ":" + c.Type + ":" + c.Source + ":" + c.ID
}
