package clickhouse

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloudEventToSlice(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Millisecond)
	event := &cloudevent.CloudEventHeader{
		ID:              "test-id",
		Source:          "test-source",
		Producer:        "test-producer",
		SpecVersion:     "1.0",
		Subject:         "test-subject",
		Time:            now,
		Type:            "test.type",
		DataContentType: "application/json",
		DataVersion:     "v1",
		DataSchema:      "https://example.com/schema",
		Extras: map[string]any{
			"extra1": "value1",
			"extra2": 123,
		},
	}

	// Test CloudEventToSlice
	slice := CloudEventToSlice(event)
	require.Len(t, slice, 12)

	// Verify the order and values of the slice
	assert.Equal(t, event.Subject, slice[0])
	assert.Equal(t, event.Time, slice[1])
	assert.Equal(t, event.Type, slice[2])
	assert.Equal(t, event.ID, slice[3])
	assert.Equal(t, event.Source, slice[4])
	assert.Equal(t, event.Producer, slice[5])
	assert.Equal(t, event.DataContentType, slice[6])
	assert.Equal(t, event.DataVersion, slice[7])

	// Verify extras JSON
	var extras map[string]any
	err := json.Unmarshal([]byte(slice[8].(string)), &extras)
	require.NoError(t, err)
	assert.Equal(t, "value1", extras["extra1"])
	assert.Equal(t, float64(123), extras["extra2"])
	assert.Equal(t, "https://example.com/schema", extras["dataschema"])
	assert.NotContains(t, extras, "specversion", "specversion is not stored in extras")

	// Verify index key
	expectedKey := CloudEventToObjectKey(event)
	assert.Equal(t, expectedKey, slice[9])

	// data_index_key is empty for CloudEventToSlice (payload is not external)
	assert.Equal(t, "", slice[10])

	// voids_id is empty for non-tombstone events
	assert.Equal(t, "", slice[11])
}

func TestCloudEventToSliceWithKey(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Millisecond)
	event := &cloudevent.CloudEventHeader{
		ID:              "test-id",
		Source:          "test-source",
		Producer:        "test-producer",
		SpecVersion:     "1.0",
		Subject:         "test-subject",
		Time:            now,
		Type:            "test.type",
		DataContentType: "application/json",
		DataVersion:     "v1",
		DataSchema:      "https://example.com/schema",
		Extras: map[string]any{
			"extra1": "value1",
			"extra2": 123,
		},
	}

	customKey := "custom-key"
	slice := CloudEventToSliceWithKey(event, customKey)
	require.Len(t, slice, 12)

	// Verify the order and values of the slice
	assert.Equal(t, event.Subject, slice[0])
	assert.Equal(t, event.Time, slice[1])
	assert.Equal(t, event.Type, slice[2])
	assert.Equal(t, event.ID, slice[3])
	assert.Equal(t, event.Source, slice[4])
	assert.Equal(t, event.Producer, slice[5])
	assert.Equal(t, event.DataContentType, slice[6])
	assert.Equal(t, event.DataVersion, slice[7])

	// Verify extras JSON
	var extras map[string]any
	err := json.Unmarshal([]byte(slice[8].(string)), &extras)
	require.NoError(t, err)
	assert.Equal(t, "value1", extras["extra1"])
	assert.Equal(t, float64(123), extras["extra2"])
	assert.Equal(t, "https://example.com/schema", extras["dataschema"])
	assert.NotContains(t, extras, "specversion", "specversion is not stored in extras")

	// Verify custom key is used
	assert.Equal(t, customKey, slice[9])

	// data_index_key is empty for CloudEventToSliceWithKey (payload is not external)
	assert.Equal(t, "", slice[10])

	// voids_id is empty for non-tombstone events
	assert.Equal(t, "", slice[11])
}

func TestStoredEventToSlice(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Millisecond)
	stored := &cloudevent.StoredEvent{
		RawEvent: cloudevent.RawEvent{
			CloudEventHeader: cloudevent.CloudEventHeader{
				ID:              "test-id",
				Source:          "test-source",
				Producer:        "test-producer",
				SpecVersion:     "1.0",
				Subject:         "test-subject",
				Time:            now,
				Type:            "test.type",
				DataContentType: "image/jpeg",
				DataVersion:     "v1",
			},
		},
		DataIndexKey: "payloads/abc123.jpg",
	}

	slice := StoredEventToSlice(stored, "bundle/key#7")
	require.Len(t, slice, 12)

	assert.Equal(t, stored.Subject, slice[0])
	assert.Equal(t, "bundle/key#7", slice[9])
	assert.Equal(t, "payloads/abc123.jpg", slice[10])
	assert.Equal(t, "", slice[11])

	// VoidsID on the wrapper propagates into the slice.
	stored.VoidsID = "voided-id-1"
	slice = StoredEventToSlice(stored, "bundle/key#7")
	assert.Equal(t, "voided-id-1", slice[11])
}

func TestUnmarshalCloudEventSlice(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Millisecond)
	expectedSlice := []any{
		"test-subject",
		now,
		"test.type",
		"test-id",
		"test-source",
		"test-producer",
		"application/json",
		"v1",
		`{"extra1":"value1","extra2":123}`,
		"test-key",
		"test-data-key",
		"test-voids-id",
	}

	// Marshal the slice to JSON
	jsonData, err := json.Marshal(expectedSlice)
	require.NoError(t, err)

	// Test successful unmarshaling
	slice, err := UnmarshalCloudEventSlice(jsonData)
	require.NoError(t, err)
	assert.Equal(t, expectedSlice, slice)

	// Test invalid JSON
	_, err = UnmarshalCloudEventSlice([]byte("invalid json"))
	assert.Error(t, err)

	// Test invalid slice length
	invalidSlice := []any{"only", "two", "elements"}
	jsonData, err = json.Marshal(invalidSlice)
	require.NoError(t, err)
	_, err = UnmarshalCloudEventSlice(jsonData)
	assert.Error(t, err)
}

func TestCloudEventToObjectKey(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Millisecond)
	event := &cloudevent.CloudEventHeader{
		ID:       "test-id",
		Source:   "test-source",
		Producer: "test-producer",
		Subject:  "test-subject",
		Time:     now,
		Type:     "test.type",
	}

	// Test with valid event
	key := CloudEventToObjectKey(event)
	require.NotEmpty(t, key)
	assert.Len(t, key, len(event.Key())+1) // +1 for the hex prefix
	assert.Equal(t, event.Key(), key[1:])  // Skip the hex prefix

	// Test with nil event
	key = CloudEventToObjectKey(nil)
	assert.Empty(t, key)

	// Test consistency - same event should always produce same key
	key1 := CloudEventToObjectKey(event)
	key2 := CloudEventToObjectKey(event)
	assert.Equal(t, key1, key2)

	// Test different events produce different keys
	event2 := *event
	event2.ID = "different-id"
	key2 = CloudEventToObjectKey(&event2)
	assert.NotEqual(t, key1, key2)
}

func TestAddNonColumnFieldsToExtras(t *testing.T) {
	t.Parallel()

	t.Run("with nil extras", func(t *testing.T) {
		event := &cloudevent.CloudEventHeader{
			ID:          "test-id",
			SpecVersion: "1.0",
			DataSchema:  "https://example.com/schema",
			Signature:   "test-signature",
			RawEventID:  "raw-event-123",
			Tags:        []string{"tag1", "tag2"},
		}

		extras := cloudevent.AddNonColumnFieldsToExtras(event)

		// specversion is NOT added to extras (always hardcoded "1.0")
		assert.NotContains(t, extras, "specversion")
		assert.Equal(t, "https://example.com/schema", extras["dataschema"])
		assert.Equal(t, "test-signature", extras["signature"])
		assert.Equal(t, "raw-event-123", extras["raweventid"])
		assert.Equal(t, []string{"tag1", "tag2"}, extras["tags"])
	})

	t.Run("with existing extras", func(t *testing.T) {
		event := &cloudevent.CloudEventHeader{
			ID:          "test-id",
			SpecVersion: "1.0",
			DataSchema:  "https://example.com/schema",
			Signature:   "test-signature",
			RawEventID:  "raw-event-123",
			Tags:        []string{"tag1", "tag2"},
			Extras: map[string]any{
				"existing": "value",
				"number":   42,
			},
		}

		extras := cloudevent.AddNonColumnFieldsToExtras(event)

		// Check that existing extras are preserved
		assert.Equal(t, "value", extras["existing"])
		assert.Equal(t, 42, extras["number"])

		// Check that non-column fields are added (specversion excluded)
		assert.NotContains(t, extras, "specversion")
		assert.Equal(t, "https://example.com/schema", extras["dataschema"])
		assert.Equal(t, "test-signature", extras["signature"])
		assert.Equal(t, "raw-event-123", extras["raweventid"])
		assert.Equal(t, []string{"tag1", "tag2"}, extras["tags"])

		// Verify original extras map is not modified
		assert.NotContains(t, event.Extras, "dataschema")
		assert.NotContains(t, event.Extras, "signature")
		assert.NotContains(t, event.Extras, "raweventid")
		assert.NotContains(t, event.Extras, "tags")
	})

	t.Run("with zero values", func(t *testing.T) {
		event := &cloudevent.CloudEventHeader{
			ID:          "test-id",
			SpecVersion: "",  // zero value
			DataSchema:  "",  // zero value
			Signature:   "",  // zero value
			RawEventID:  "",  // zero value
			Tags:        nil, // zero value
		}

		extras := cloudevent.AddNonColumnFieldsToExtras(event)

		// Zero values should not be added to extras
		assert.NotContains(t, extras, "specversion")
		assert.NotContains(t, extras, "dataschema")
		assert.NotContains(t, extras, "signature")
		assert.NotContains(t, extras, "raweventid")
		assert.NotContains(t, extras, "tags")
	})

	t.Run("with empty tags slice", func(t *testing.T) {
		event := &cloudevent.CloudEventHeader{
			ID:          "test-id",
			SpecVersion: "1.0",
			Tags:        []string{}, // empty slice
		}

		extras := cloudevent.AddNonColumnFieldsToExtras(event)

		assert.NotContains(t, extras, "specversion")
		assert.NotContains(t, extras, "tags") // empty slice should not be added
	})
}

func TestRestoreNonColumnFields(t *testing.T) {
	t.Parallel()

	t.Run("with nil extras", func(t *testing.T) {
		event := &cloudevent.CloudEventHeader{
			ID:     "test-id",
			Extras: nil,
		}

		// Should not panic with nil extras
		cloudevent.RestoreNonColumnFields(event)

		// SpecVersion is always set to "1.0"
		assert.Equal(t, "1.0", event.SpecVersion)
		assert.Empty(t, event.DataSchema)
		assert.Empty(t, event.Signature)
		assert.Empty(t, event.RawEventID)
		assert.Nil(t, event.Tags)
	})

	t.Run("with empty extras", func(t *testing.T) {
		event := &cloudevent.CloudEventHeader{
			ID:     "test-id",
			Extras: map[string]any{},
		}

		cloudevent.RestoreNonColumnFields(event)

		// SpecVersion is always set to "1.0"
		assert.Equal(t, "1.0", event.SpecVersion)
		assert.Empty(t, event.DataSchema)
		assert.Empty(t, event.Signature)
		assert.Empty(t, event.RawEventID)
		assert.Nil(t, event.Tags)
	})

	t.Run("restore all fields", func(t *testing.T) {
		event := &cloudevent.CloudEventHeader{
			ID: "test-id",
			Extras: map[string]any{
				"specversion": "1.0",
				"dataschema":  "https://example.com/schema",
				"signature":   "test-signature",
				"raweventid":  "raw-event-123",
				"tags":        []any{"tag1", "tag2"},
				"other":       "should-remain",
			},
		}

		cloudevent.RestoreNonColumnFields(event)

		// Check that fields are restored
		assert.Equal(t, "1.0", event.SpecVersion)
		assert.Equal(t, "https://example.com/schema", event.DataSchema)
		assert.Equal(t, "test-signature", event.Signature)
		assert.Equal(t, "raw-event-123", event.RawEventID)
		assert.Equal(t, []string{"tag1", "tag2"}, event.Tags)

		// Check that non-column fields are removed from extras
		assert.NotContains(t, event.Extras, "specversion")
		assert.NotContains(t, event.Extras, "dataschema")
		assert.NotContains(t, event.Extras, "signature")
		assert.NotContains(t, event.Extras, "raweventid")
		assert.NotContains(t, event.Extras, "tags")
		assert.Contains(t, event.Extras, "other") // other fields remain
	})

	t.Run("with wrong types in extras", func(t *testing.T) {
		event := &cloudevent.CloudEventHeader{
			ID: "test-id",
			Extras: map[string]any{
				"specversion": 123,           // wrong type
				"dataschema":  []int{},       // wrong type
				"raweventid":  999,           // wrong type
				"tags":        "not-a-slice", // wrong type
			},
		}

		// Should not panic with wrong types
		cloudevent.RestoreNonColumnFields(event)

		// SpecVersion is always hardcoded to "1.0" regardless of extras
		assert.Equal(t, "1.0", event.SpecVersion)
		assert.Empty(t, event.DataSchema)
		assert.Empty(t, event.RawEventID)
		assert.Nil(t, event.Tags)

		// Wrong-typed values should still be removed from extras for some fields
		assert.NotContains(t, event.Extras, "specversion")
		assert.NotContains(t, event.Extras, "dataschema")
		assert.NotContains(t, event.Extras, "raweventid")
		assert.NotContains(t, event.Extras, "tags")
	})

	t.Run("partial restoration", func(t *testing.T) {
		event := &cloudevent.CloudEventHeader{
			ID: "test-id",
			Extras: map[string]any{
				"specversion": "1.0",
				// missing dataschema, signature, raweventid, tags
				"other": "value",
			},
		}

		cloudevent.RestoreNonColumnFields(event)

		// Only specversion should be restored
		assert.Equal(t, "1.0", event.SpecVersion)
		assert.Empty(t, event.DataSchema)
		assert.Empty(t, event.Signature)
		assert.Empty(t, event.RawEventID)
		assert.Nil(t, event.Tags)

		// specversion should be removed, other should remain
		assert.NotContains(t, event.Extras, "specversion")
		assert.Contains(t, event.Extras, "other")
	})
}
