package cloudevent

// StoredEvent wraps a RawEvent with metadata produced when an event is
// persisted to backing storage (ClickHouse, Parquet bundles, S3). Use this
// at the boundary between the in-memory event and the storage layer.
//
// DataIndexKey, when non-empty, is the storage key (e.g. S3 object key) of
// an external object holding the payload. In that case the embedded
// RawEvent's Data and DataBase64 fields will typically be empty.
//
// StoredEvent is deliberately separate from CloudEventHeader / RawEvent so
// that the wire-format types remain pure and shared safely across services
// — DataIndexKey points into trusted internal storage and must never be set
// from producer-supplied input.
type StoredEvent struct {
	RawEvent
	DataIndexKey string
}
