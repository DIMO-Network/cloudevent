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
	require.Len(t, slice, 10)

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
	assert.Equal(t, "1.0", extras["specversion"])
	assert.Equal(t, "https://example.com/schema", extras["dataschema"])

	// Verify index key
	expectedKey := CloudEventToObjectKey(event)
	assert.Equal(t, expectedKey, slice[9])
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
	require.Len(t, slice, 10)

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
	assert.Equal(t, "1.0", extras["specversion"])
	assert.Equal(t, "https://example.com/schema", extras["dataschema"])

	// Verify custom key is used
	assert.Equal(t, customKey, slice[9])
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
