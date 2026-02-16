package cloudevent_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestData struct {
	Message string `json:"message"`
	Count   int    `json:"count"`
}

func TestCloudEvent_MarshalJSON(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC().Truncate(time.Millisecond)
	tests := []struct {
		name     string
		event    cloudevent.CloudEvent[TestData]
		expected string
	}{
		{
			name: "basic event",
			event: cloudevent.CloudEvent[TestData]{
				CloudEventHeader: cloudevent.CloudEventHeader{
					ID:       "123",
					Source:   "test-source",
					Producer: "test-producer",
					Subject:  "test-subject",
					Time:     now,
					Type:     cloudevent.TypeStatus,
				},
				Data: TestData{
					Message: "hello",
					Count:   42,
				},
			},
			expected: `{
				"id": "123",
				"source": "test-source",
				"producer": "test-producer",
				"specversion": "1.0",
				"subject": "test-subject",
				"time": "` + now.Format(time.RFC3339Nano) + `",
				"type": "dimo.status",
				"data": {
					"message": "hello",
					"count": 42
				}
			}`,
		},
		{
			name: "event with extras",
			event: cloudevent.CloudEvent[TestData]{
				CloudEventHeader: cloudevent.CloudEventHeader{
					ID:          "456",
					Source:      "test-source",
					Producer:    "test-producer",
					SpecVersion: cloudevent.SpecVersion,
					Subject:     "test-subject",
					Time:        now,
					Type:        cloudevent.TypeFingerprint,
					Extras: map[string]any{
						"extra1": "value1",
						"extra2": 123,
					},
				},
				Data: TestData{
					Message: "test",
					Count:   1,
				},
			},
			expected: `{
				"id": "456",
				"source": "test-source",
				"producer": "test-producer",
				"specversion": "1.0",
				"subject": "test-subject",
				"time": "` + now.Format(time.RFC3339Nano) + `",
				"type": "dimo.fingerprint",
				"extra1": "value1",
				"extra2": 123,
				"data": {
					"message": "test",
					"count": 1
				}
			}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			actual, err := json.Marshal(tt.event)
			require.NoError(t, err)

			// Compare JSON objects instead of strings to avoid formatting issues
			var expectedObj, actualObj map[string]any
			require.NoError(t, json.Unmarshal([]byte(tt.expected), &expectedObj))
			require.NoError(t, json.Unmarshal(actual, &actualObj))

			assert.Equal(t, expectedObj, actualObj)
		})
	}
}

func TestCloudEvent_UnmarshalJSON(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC().Truncate(time.Millisecond)

	tests := []struct {
		name     string
		json     string
		expected cloudevent.CloudEvent[TestData]
	}{
		{
			name: "basic event",
			json: `{
				"id": "123",
				"source": "test-source",
				"producer": "test-producer",
				"subject": "test-subject",
				"time": "` + now.Format(time.RFC3339Nano) + `",
				"type": "dimo.status",
				"data": {
					"message": "hello",
					"count": 42
				}
			}`,
			expected: cloudevent.CloudEvent[TestData]{
				CloudEventHeader: cloudevent.CloudEventHeader{
					ID:          "123",
					Source:      "test-source",
					Producer:    "test-producer",
					SpecVersion: cloudevent.SpecVersion,
					Subject:     "test-subject",
					Time:        now,
					Type:        cloudevent.TypeStatus,
				},
				Data: TestData{
					Message: "hello",
					Count:   42,
				},
			},
		},
		{
			name: "event with extras",
			json: `{
				"id": "456",
				"source": "test-source",
				"producer": "test-producer",
				"specversion": "1.0",
				"subject": "test-subject",
				"time": "` + now.Format(time.RFC3339Nano) + `",
				"type": "dimo.fingerprint",
				"extra1": "value1",
				"extra2": 123,
				"data": {
					"message": "test",
					"count": 1
				}
			}`,
			expected: cloudevent.CloudEvent[TestData]{
				CloudEventHeader: cloudevent.CloudEventHeader{
					ID:          "456",
					Source:      "test-source",
					Producer:    "test-producer",
					SpecVersion: cloudevent.SpecVersion,
					Subject:     "test-subject",
					Time:        now,
					Type:        cloudevent.TypeFingerprint,
					Extras: map[string]any{
						"extra1": "value1",
						"extra2": float64(123), // JSON numbers are unmarshaled as float64
					},
				},
				Data: TestData{
					Message: "test",
					Count:   1,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var actual cloudevent.CloudEvent[TestData]
			err := json.Unmarshal([]byte(tt.json), &actual)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func TestCloudEventHeader_MarshalJSON(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC().Truncate(time.Millisecond)
	tests := []struct {
		name     string
		header   cloudevent.CloudEventHeader
		expected string
	}{
		{
			name: "basic header",
			header: cloudevent.CloudEventHeader{
				ID:          "123",
				Source:      "test-source",
				Producer:    "test-producer",
				SpecVersion: cloudevent.SpecVersion,
				Subject:     "test-subject",
				Time:        now,
				Type:        cloudevent.TypeStatus,
			},
			expected: `{
				"id": "123",
				"source": "test-source",
				"producer": "test-producer",
				"specversion": "1.0",
				"subject": "test-subject",
				"time": "` + now.Format(time.RFC3339Nano) + `",
				"type": "dimo.status"
			}`,
		},
		{
			name: "header with extras",
			header: cloudevent.CloudEventHeader{
				ID:          "456",
				Source:      "test-source",
				Producer:    "test-producer",
				SpecVersion: cloudevent.SpecVersion,
				Subject:     "test-subject",
				Time:        now,
				Type:        cloudevent.TypeFingerprint,
				Extras: map[string]any{
					"extra1": "value1",
					"extra2": 123,
				},
			},
			expected: `{
				"id": "456",
				"source": "test-source",
				"producer": "test-producer",
				"specversion": "1.0",
				"subject": "test-subject",
				"time": "` + now.Format(time.RFC3339Nano) + `",
				"type": "dimo.fingerprint",
				"extra1": "value1",
				"extra2": 123
			}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			actual, err := json.Marshal(tt.header)
			require.NoError(t, err)

			var expectedObj, actualObj map[string]any
			require.NoError(t, json.Unmarshal([]byte(tt.expected), &expectedObj))
			require.NoError(t, json.Unmarshal(actual, &actualObj))

			assert.Equal(t, expectedObj, actualObj)
		})
	}
}

func TestCloudEventHeader_UnmarshalJSON(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC().Truncate(time.Millisecond)

	tests := []struct {
		name     string
		json     string
		expected cloudevent.CloudEventHeader
	}{
		{
			name: "basic header",
			json: `{
				"id": "123",
				"source": "test-source",
				"producer": "test-producer",
				"specversion": "1.0",
				"subject": "test-subject",
				"time": "` + now.Format(time.RFC3339Nano) + `",
				"type": "dimo.status"
			}`,
			expected: cloudevent.CloudEventHeader{
				ID:          "123",
				Source:      "test-source",
				Producer:    "test-producer",
				SpecVersion: cloudevent.SpecVersion,
				Subject:     "test-subject",
				Time:        now,
				Type:        cloudevent.TypeStatus,
			},
		},
		{
			name: "header with optional fields",
			json: `{
				"id": "456",
				"source": "test-source",
				"producer": "test-producer",
				"specversion": "1.0",
				"subject": "test-subject",
				"time": "` + now.Format(time.RFC3339Nano) + `",
				"type": "dimo.fingerprint",
				"datacontenttype": "application/json",
				"dataschema": "https://example.com/schema",
				"dataversion": "v2.4",
				"extra1": "value1",
				"extra2": 123
			}`,
			expected: cloudevent.CloudEventHeader{
				ID:              "456",
				Source:          "test-source",
				Producer:        "test-producer",
				SpecVersion:     "1.0",
				Subject:         "test-subject",
				Time:            now,
				Type:            cloudevent.TypeFingerprint,
				DataContentType: "application/json",
				DataSchema:      "https://example.com/schema",
				DataVersion:     "v2.4",
				Extras: map[string]any{
					"extra1": "value1",
					"extra2": float64(123),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var actual cloudevent.CloudEventHeader
			err := json.Unmarshal([]byte(tt.json), &actual)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func TestRawEvent_UnmarshalJSON_InvalidBase64(t *testing.T) {
	t.Parallel()
	jsonStr := `{"id":"1","source":"s","type":"t","data_base64":"$$not-base64$$"}`
	var ev cloudevent.RawEvent
	err := json.Unmarshal([]byte(jsonStr), &ev)
	require.Error(t, err, "expected error for invalid base64 in data_base64")
}

func TestRawEvent_UnmarshalJSON_BothDataAndDataBase64(t *testing.T) {
	t.Parallel()
	jsonStr := `{"id":"1","source":"s","type":"t","data":{"x":1},"data_base64":"Zm9v"}`
	var ev cloudevent.RawEvent
	err := json.Unmarshal([]byte(jsonStr), &ev)
	require.Error(t, err, "expected error when both data and data_base64 are present")
	assert.Contains(t, err.Error(), "both")
}

func TestCloudEvent_UnmarshalJSON_InvalidTime(t *testing.T) {
	t.Parallel()
	jsonStr := `{"id":"1","source":"s","type":"t","time":12345,"data":{"message":"hi","count":1}}`
	var ev cloudevent.CloudEvent[TestData]
	err := json.Unmarshal([]byte(jsonStr), &ev)
	require.Error(t, err, "expected error for invalid time field type")
}

func TestCloudEvent_UnmarshalJSON_NoDataField(t *testing.T) {
	t.Parallel()
	jsonStr := `{"id":"1","source":"s","type":"t","subject":"sub","time":"2025-01-01T00:00:00Z"}`
	var ev cloudevent.CloudEvent[TestData]
	err := json.Unmarshal([]byte(jsonStr), &ev)
	require.NoError(t, err, "CloudEvent without data field should succeed")
	assert.Equal(t, "1", ev.ID)
	assert.Equal(t, TestData{}, ev.Data)
}
