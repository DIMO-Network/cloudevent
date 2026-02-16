// Package cloudevent provides types for working with CloudEvents.
package cloudevent

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"mime"
	"strings"
	"time"

	"github.com/tidwall/sjson"
)

const (
	// TypeStatus is the event type for status updates.
	TypeStatus = "dimo.status"

	// TypeFingerprint is the event type for fingerprint updates.
	TypeFingerprint = "dimo.fingerprint"

	// TypeVerifableCredential is the event type for verifiable credentials.
	TypeVerifableCredential = "dimo.verifiablecredential" //nolint:gosec // This is not a credential.

	// TypeAttestation is the event type for 3rd party attestations
	TypeAttestation = "dimo.attestation"

	// TypeUnknown is the event type for unknown events.
	TypeUnknown = "dimo.unknown"

	// TypeEvent is the event type for vehicle events
	TypeEvent = "dimo.event"

	// TypeTrigger is the event type from a vehicle trigger.
	TypeTrigger = "dimo.trigger"

	// TypeSACD is the event type for SACD events.
	TypeSACD = "dimo.sacd"

	// TypeSACDTemplate is the event type for SACD template events.
	TypeSACDTemplate = "dimo.sacd.template"
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

	// Signature hold the signature of the a cloudevent's data field.
	Signature string `json:"signature,omitempty" cloudevent:"leaveInExtras"`

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
}

// RawEvent is a cloudevent with a json.RawMessage data field.
// It supports both "data" and "data_base64" (CloudEvents JSON spec).
type RawEvent struct {
	CloudEventHeader
	Data json.RawMessage `json:"data,omitempty"`

	// DataBase64 is the raw "data_base64" string when the event was received with
	// data_base64 (CloudEvents spec). When set, MarshalJSON emits data_base64 for
	// round-trip; otherwise wire form is chosen from DataContentType and Data.
	DataBase64 string `json:"data_base64,omitempty"`
}

// BytesForSignature returns the bytes that were signed (wire form of data or data_base64).
// Use for signature verification; not the same as Data when the CE used data_base64.
func (r RawEvent) BytesForSignature() []byte {
	if r.DataBase64 != "" {
		return []byte(r.DataBase64)
	}
	return r.Data
}

// UnmarshalJSON implements json.Unmarshaler so that both "data" and "data_base64"
// are supported; Data is always set to the resolved payload bytes.
func (r *RawEvent) UnmarshalJSON(data []byte) error {
	var dataRaw json.RawMessage
	var dataBase64 string
	header, err := unmarshalCloudEventWithPayload(data, func(d json.RawMessage, b64 string) error {
		dataRaw = d
		dataBase64 = b64
		return nil
	})
	if err != nil {
		return err
	}
	r.CloudEventHeader = header
	if dataRaw != nil && dataBase64 != "" {
		return fmt.Errorf("cloudevent: both \"data\" and \"data_base64\" present; only one allowed")
	}
	if dataBase64 != "" {
		decoded, err := base64.StdEncoding.DecodeString(dataBase64)
		if err != nil {
			return err
		}
		r.Data = decoded
		r.DataBase64 = dataBase64
	} else {
		r.Data = dataRaw
		r.DataBase64 = ""
	}
	return nil
}

// IsJSONDataContentType returns true if the MIME type indicates a JSON payload.
// Matches "application/json" and any "+json" suffix type (e.g. "application/cloudevents+json").
func IsJSONDataContentType(ct string) bool {
	parsed, _, err := mime.ParseMediaType(strings.TrimSpace(ct))
	return err == nil && (parsed == "application/json" || strings.HasSuffix(parsed, "+json"))
}

// MarshalJSON implements json.Marshaler. Uses DataContentType to choose wire form:
// application/json -> "data"; otherwise -> "data_base64" (CloudEvents spec).
func (r RawEvent) MarshalJSON() ([]byte, error) {
	data, err := json.Marshal(r.CloudEventHeader)
	if err != nil {
		return nil, err
	}
	if len(r.Data) > 0 || r.DataBase64 != "" {
		if r.DataBase64 != "" {
			data, err = sjson.SetBytes(data, "data_base64", r.DataBase64)
		} else if IsJSONDataContentType(r.DataContentType) || (r.DataContentType == "" && json.Valid(r.Data)) {
			data, err = sjson.SetRawBytes(data, "data", r.Data)
		} else {
			data, err = sjson.SetBytes(data, "data_base64", base64.StdEncoding.EncodeToString(r.Data))
		}
		if err != nil {
			return nil, err
		}
	}
	return data, nil
}

// Equals returns true if the two CloudEventHeaders share the same IndexKey.
func (c *CloudEventHeader) Equals(other CloudEventHeader) bool {
	return c.Key() == other.Key()
}

// Key returns the unique identifier for the CloudEvent.
func (c CloudEventHeader) Key() string {
	return c.Subject + "!" + c.Time.Format(time.RFC3339) + "!" + c.Type + "!" + c.Source + "!" + c.ID
}
