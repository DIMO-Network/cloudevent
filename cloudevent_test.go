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

func TestCloudEvent_UnmarshalJSON_DataBase64(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC().Truncate(time.Millisecond)

	// base64 of `{"message":"hello","count":42}`
	jsonStr := `{
		"id": "b64-1",
		"source": "test-source",
		"producer": "test-producer",
		"subject": "test-subject",
		"time": "` + now.Format(time.RFC3339Nano) + `",
		"type": "dimo.status",
		"data_base64": "eyJtZXNzYWdlIjoiaGVsbG8iLCJjb3VudCI6NDJ9"
	}`
	var ev cloudevent.CloudEvent[TestData]
	err := json.Unmarshal([]byte(jsonStr), &ev)
	require.NoError(t, err)
	assert.Equal(t, "b64-1", ev.ID)
	assert.Equal(t, TestData{}, ev.Data, "Data should not be populated from data_base64")
	assert.Equal(t, "eyJtZXNzYWdlIjoiaGVsbG8iLCJjb3VudCI6NDJ9", ev.DataBase64)
}

func TestCloudEvent_MarshalJSON_DataBase64(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC().Truncate(time.Millisecond)
	ev := cloudevent.CloudEvent[TestData]{
		CloudEventHeader: cloudevent.CloudEventHeader{
			ID:       "b64-m",
			Source:   "test-source",
			Producer: "test-producer",
			Subject:  "test-subject",
			Time:     now,
			Type:     cloudevent.TypeStatus,
		},
		Data:       TestData{Message: "hello", Count: 42},
		DataBase64: "eyJtZXNzYWdlIjoiaGVsbG8iLCJjb3VudCI6NDJ9",
	}
	out, err := json.Marshal(ev)
	require.NoError(t, err)

	var m map[string]any
	require.NoError(t, json.Unmarshal(out, &m))
	assert.Equal(t, "eyJtZXNzYWdlIjoiaGVsbG8iLCJjb3VudCI6NDJ9", m["data_base64"])
	assert.Nil(t, m["data"], "data field should not be present when data_base64 is set")
}

func TestCloudEvent_UnmarshalJSON_BothDataAndDataBase64(t *testing.T) {
	t.Parallel()
	jsonStr := `{"id":"1","source":"s","type":"t","data":{"message":"hi","count":1},"data_base64":"Zm9v"}`
	var ev cloudevent.CloudEvent[TestData]
	err := json.Unmarshal([]byte(jsonStr), &ev)
	require.Error(t, err, "expected error when both data and data_base64 are present")
	assert.Contains(t, err.Error(), "both")
}

func TestCloudEvent_UnmarshalJSON_InvalidBase64(t *testing.T) {
	t.Parallel()
	jsonStr := `{"id":"1","source":"s","type":"t","data_base64":"$$not-base64$$"}`
	var ev cloudevent.CloudEvent[TestData]
	err := json.Unmarshal([]byte(jsonStr), &ev)
	require.NoError(t, err, "CloudEvent[A] should not validate base64 encoding")
	assert.Equal(t, "$$not-base64$$", ev.DataBase64)
	assert.Equal(t, TestData{}, ev.Data)
}

func TestCloudEvent_DataBase64_RoundTrip(t *testing.T) {
	t.Parallel()
	input := `{
		"id": "rt-1",
		"source": "s",
		"producer": "p",
		"subject": "sub",
		"time": "2025-01-01T00:00:00Z",
		"type": "dimo.status",
		"data_base64": "eyJtZXNzYWdlIjoicnQiLCJjb3VudCI6N30="
	}`
	var ev cloudevent.CloudEvent[TestData]
	require.NoError(t, json.Unmarshal([]byte(input), &ev))
	assert.Equal(t, TestData{}, ev.Data, "Data should not be populated from data_base64")
	assert.Equal(t, "eyJtZXNzYWdlIjoicnQiLCJjb3VudCI6N30=", ev.DataBase64)

	out, err := json.Marshal(ev)
	require.NoError(t, err)

	var m map[string]any
	require.NoError(t, json.Unmarshal(out, &m))
	assert.Equal(t, "eyJtZXNzYWdlIjoicnQiLCJjb3VudCI6N30=", m["data_base64"])
	assert.Nil(t, m["data"])
}

// --- RawEvent positive tests ---

func TestRawEvent_UnmarshalJSON_DataField(t *testing.T) {
	t.Parallel()
	input := `{
		"id":"r1","source":"s","producer":"p","subject":"sub",
		"time":"2025-06-01T00:00:00Z","type":"dimo.status",
		"data":{"temp":72}
	}`
	var ev cloudevent.RawEvent
	require.NoError(t, json.Unmarshal([]byte(input), &ev))

	assert.Equal(t, "r1", ev.ID)
	assert.JSONEq(t, `{"temp":72}`, string(ev.Data))
	assert.Empty(t, ev.DataBase64, "DataBase64 should be empty when data field is used")
}

func TestRawEvent_UnmarshalJSON_DataBase64Field(t *testing.T) {
	t.Parallel()
	// base64("hello world") = "aGVsbG8gd29ybGQ="
	input := `{
		"id":"r2","source":"s","producer":"p","subject":"sub",
		"time":"2025-06-01T00:00:00Z","type":"dimo.status",
		"data_base64":"aGVsbG8gd29ybGQ="
	}`
	var ev cloudevent.RawEvent
	require.NoError(t, json.Unmarshal([]byte(input), &ev))

	assert.Equal(t, "r2", ev.ID)
	assert.Equal(t, []byte("hello world"), []byte(ev.Data))
	assert.Equal(t, "aGVsbG8gd29ybGQ=", ev.DataBase64)
}

func TestRawEvent_UnmarshalJSON_DataBase64_WithExtras(t *testing.T) {
	t.Parallel()
	input := `{
		"id":"r3","source":"s","producer":"p","subject":"sub",
		"time":"2025-06-01T00:00:00Z","type":"dimo.status",
		"data_base64":"Zm9v",
		"customfield":"bar"
	}`
	var ev cloudevent.RawEvent
	require.NoError(t, json.Unmarshal([]byte(input), &ev))

	assert.Equal(t, "Zm9v", ev.DataBase64)
	assert.Equal(t, []byte("foo"), []byte(ev.Data))
	require.Contains(t, ev.Extras, "customfield")
	assert.Equal(t, "bar", ev.Extras["customfield"])
}

func TestRawEvent_RoundTrip_DataBase64(t *testing.T) {
	t.Parallel()
	input := `{
		"id":"rt","source":"s","producer":"p","subject":"sub",
		"time":"2025-06-01T00:00:00Z","type":"dimo.status",
		"data_base64":"aGVsbG8gd29ybGQ="
	}`
	var ev cloudevent.RawEvent
	require.NoError(t, json.Unmarshal([]byte(input), &ev))

	out, err := json.Marshal(ev)
	require.NoError(t, err)

	var m map[string]any
	require.NoError(t, json.Unmarshal(out, &m))
	assert.Equal(t, "aGVsbG8gd29ybGQ=", m["data_base64"])
	assert.Nil(t, m["data"], "data should not be present when round-tripping data_base64")
}

func TestRawEvent_RoundTrip_DataJSON(t *testing.T) {
	t.Parallel()
	input := `{
		"id":"rt2","source":"s","producer":"p","subject":"sub",
		"time":"2025-06-01T00:00:00Z","type":"dimo.status",
		"data":{"key":"value"}
	}`
	var ev cloudevent.RawEvent
	require.NoError(t, json.Unmarshal([]byte(input), &ev))

	out, err := json.Marshal(ev)
	require.NoError(t, err)

	var m map[string]any
	require.NoError(t, json.Unmarshal(out, &m))
	assert.Nil(t, m["data_base64"], "data_base64 should not be present when data is JSON")
	assert.Equal(t, map[string]any{"key": "value"}, m["data"])
}

func TestRawEvent_MarshalJSON_NonJSONData_EmitsBase64(t *testing.T) {
	t.Parallel()
	ev := cloudevent.RawEvent{
		CloudEventHeader: cloudevent.CloudEventHeader{
			ID:              "nj1",
			Source:          "s",
			Producer:        "p",
			Subject:         "sub",
			Time:            time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC),
			Type:            cloudevent.TypeStatus,
			DataContentType: "application/octet-stream",
		},
		Data: []byte("binary\x00data"),
	}
	out, err := json.Marshal(ev)
	require.NoError(t, err)

	var m map[string]any
	require.NoError(t, json.Unmarshal(out, &m))
	assert.Nil(t, m["data"], "non-JSON content type should not emit data field")
	assert.NotEmpty(t, m["data_base64"])
}

func TestRawEvent_MarshalJSON_ExplicitJSONContentType(t *testing.T) {
	t.Parallel()
	ev := cloudevent.RawEvent{
		CloudEventHeader: cloudevent.CloudEventHeader{
			ID:              "jct",
			Source:          "s",
			Producer:        "p",
			Subject:         "sub",
			Time:            time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC),
			Type:            cloudevent.TypeStatus,
			DataContentType: "application/json",
		},
		Data: json.RawMessage(`{"ok":true}`),
	}
	out, err := json.Marshal(ev)
	require.NoError(t, err)

	var m map[string]any
	require.NoError(t, json.Unmarshal(out, &m))
	assert.Equal(t, map[string]any{"ok": true}, m["data"])
	assert.Nil(t, m["data_base64"])
}

func TestRawEvent_MarshalJSON_DataBase64TakesPrecedence(t *testing.T) {
	t.Parallel()
	ev := cloudevent.RawEvent{
		CloudEventHeader: cloudevent.CloudEventHeader{
			ID:      "bp",
			Source:  "s",
			Subject: "sub",
			Time:    time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC),
			Type:    cloudevent.TypeStatus,
		},
		Data:       json.RawMessage(`{"ignored":true}`),
		DataBase64: "cHJlc2V0",
	}
	out, err := json.Marshal(ev)
	require.NoError(t, err)

	var m map[string]any
	require.NoError(t, json.Unmarshal(out, &m))
	assert.Equal(t, "cHJlc2V0", m["data_base64"])
	assert.Nil(t, m["data"], "data_base64 should take precedence over data")
}

func TestRawEvent_BytesForSignature_DataBase64(t *testing.T) {
	t.Parallel()
	ev := cloudevent.RawEvent{
		Data:       []byte("decoded payload"),
		DataBase64: "b3JpZ2luYWw=",
	}
	assert.Equal(t, []byte("b3JpZ2luYWw="), ev.BytesForSignature())
}

func TestRawEvent_BytesForSignature_DataOnly(t *testing.T) {
	t.Parallel()
	ev := cloudevent.RawEvent{
		Data: json.RawMessage(`{"sig":"data"}`),
	}
	assert.Equal(t, json.RawMessage(`{"sig":"data"}`), json.RawMessage(ev.BytesForSignature()))
}

// --- IsJSONDataContentType tests ---

func TestIsJSONDataContentType(t *testing.T) {
	t.Parallel()
	tests := []struct {
		ct       string
		expected bool
	}{
		{"application/json", true},
		{"application/json; charset=utf-8", true},
		{"application/cloudevents+json", true},
		{"application/vnd.custom+json", true},
		{"application/octet-stream", false},
		{"text/plain", false},
		{"", false},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.expected, cloudevent.IsJSONDataContentType(tt.ct), "content type: %q", tt.ct)
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
