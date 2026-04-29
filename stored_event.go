package cloudevent

// StoredEvent wraps a RawEvent with metadata produced when an event is
// persisted to backing storage (ClickHouse, Parquet bundles, S3). Use this
// at the boundary between the in-memory event and the storage layer.
//
// DataIndexKey, when non-empty, is the storage key (e.g. S3 object key) of
// an external object holding the payload. In that case the embedded
// RawEvent's Data and DataBase64 fields will typically be empty.
//
// VoidsID, when non-empty, is the id of the attestation that this event
// tombstones. It is only meaningful for events whose Type is
// TypeAttestationTombstone and is extracted server-side from the
// signed Data payload — it must never be set from producer-supplied input
// directly. It is used to cheaply populate the voids_id column in ClickHouse,
// but is not stored as a column in Parquet bundles: you would have to parse
// the data section of a Parquet row to recover the id.
//
// StoredEvent is deliberately separate from CloudEventHeader / RawEvent so
// that the wire-format types remain pure and shared safely across services
// — DataIndexKey and VoidsID point into trusted internal storage and must
// never be set from producer-supplied input.
type StoredEvent struct {
	RawEvent
	DataIndexKey string
	VoidsID      string
}
