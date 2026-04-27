package parquet

import (
	"bytes"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/DIMO-Network/cloudevent"
	pq "github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/snappy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeEvent creates a minimal RawEvent for testing.
func makeEvent(id string, data json.RawMessage) cloudevent.RawEvent {
	return cloudevent.RawEvent{
		CloudEventHeader: cloudevent.CloudEventHeader{
			SpecVersion:     cloudevent.SpecVersion,
			Type:            "dimo.status",
			Source:          "0xABCD",
			Subject:         "did:erc721:137:0xABCD:1",
			ID:              id,
			Time:            time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC),
			DataContentType: "application/json",
			DataVersion:     "v2.0.0",
			Producer:        "0x1234",
		},
		Data: data,
	}
}

// encodeToBuffer is a test helper that encodes events and returns the buffer.
// Each RawEvent is wrapped in a StoredEvent with empty DataIndexKey; for
// tests that need a non-empty DataIndexKey, build []cloudevent.StoredEvent
// directly and call Encode.
func encodeToBuffer(t *testing.T, events []cloudevent.RawEvent, objectKey string, opts ...Option) (*bytes.Buffer, map[int]string) {
	t.Helper()
	stored := make([]cloudevent.StoredEvent, len(events))
	for i, ev := range events {
		stored[i] = cloudevent.StoredEvent{RawEvent: ev}
	}
	var buf bytes.Buffer
	keys, err := Encode(&buf, stored, objectKey, opts...)
	require.NoError(t, err)
	return &buf, keys
}

// --- Encode tests ---

func TestEncode_SingleEvent(t *testing.T) {
	t.Parallel()
	events := []cloudevent.RawEvent{
		makeEvent("evt-1", json.RawMessage(`{"speed":55}`)),
	}

	buf, keys := encodeToBuffer(t, events, "obj/key")

	assert.Greater(t, buf.Len(), 0, "parquet output should not be empty")
	require.Len(t, keys, 1)
	assert.Equal(t, "obj/key#0", keys[0])
}

func TestEncode_MultipleEvents(t *testing.T) {
	t.Parallel()
	events := []cloudevent.RawEvent{
		makeEvent("evt-1", json.RawMessage(`{"a":1}`)),
		makeEvent("evt-2", json.RawMessage(`{"a":2}`)),
		makeEvent("evt-3", json.RawMessage(`{"a":3}`)),
	}

	buf, keys := encodeToBuffer(t, events, "batch")

	assert.Greater(t, buf.Len(), 0)
	require.Len(t, keys, 3)
	for i := 0; i < 3; i++ {
		assert.Equal(t, "batch#"+strconv.Itoa(i), keys[i])
	}
}

func TestEncode_EmptySlice(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	keys, err := Encode(&buf, nil, "empty")
	require.NoError(t, err)
	assert.Empty(t, keys)
}

func TestEncode_WithOptions(t *testing.T) {
	t.Parallel()
	events := []cloudevent.StoredEvent{
		{RawEvent: makeEvent("evt-1", json.RawMessage(`{"x":1}`))},
	}

	var buf bytes.Buffer
	keys, err := Encode(&buf, events, "opts",
		WithMaxRowsPerRowGroup(5),
		WithPageBufferSize(1024),
		WithWriteBufferSize(2048),
	)
	require.NoError(t, err)
	require.Len(t, keys, 1)
	assert.Greater(t, buf.Len(), 0)
}

func TestEncode_DataBase64Event(t *testing.T) {
	t.Parallel()
	event := makeEvent("evt-b64", nil)
	event.DataBase64 = "SGVsbG8gV29ybGQ="
	event.Data = nil

	buf, keys := encodeToBuffer(t, []cloudevent.RawEvent{event}, "b64")

	require.Len(t, keys, 1)
	assert.Greater(t, buf.Len(), 0)
}

func TestEncode_NilDataAndEmptyBase64(t *testing.T) {
	t.Parallel()
	event := makeEvent("evt-nil", nil)
	event.Data = nil
	event.DataBase64 = ""

	buf, keys := encodeToBuffer(t, []cloudevent.RawEvent{event}, "nildata")
	require.Len(t, keys, 1)
	assert.Greater(t, buf.Len(), 0)
}

func TestEncode_IndexKeyFormat(t *testing.T) {
	t.Parallel()
	events := make([]cloudevent.RawEvent, 12)
	for i := range events {
		events[i] = makeEvent("evt-"+strconv.Itoa(i), json.RawMessage(`{}`))
	}

	_, keys := encodeToBuffer(t, events, "prefix/path")

	for i := 0; i < 12; i++ {
		assert.Equal(t, "prefix/path#"+strconv.Itoa(i), keys[i])
	}
}

// --- Decode tests ---

func TestDecode_SingleEvent(t *testing.T) {
	t.Parallel()
	original := makeEvent("evt-1", json.RawMessage(`{"speed":55}`))
	buf, _ := encodeToBuffer(t, []cloudevent.RawEvent{original}, "key")

	r := bytes.NewReader(buf.Bytes())
	events, err := Decode(r, int64(buf.Len()))
	require.NoError(t, err)
	require.Len(t, events, 1)

	got := events[0]
	assert.Equal(t, original.Type, got.Type)
	assert.Equal(t, original.Source, got.Source)
	assert.Equal(t, original.Subject, got.Subject)
	assert.Equal(t, original.ID, got.ID)
	assert.Equal(t, original.DataContentType, got.DataContentType)
	assert.Equal(t, original.DataVersion, got.DataVersion)
	assert.Equal(t, original.Producer, got.Producer)
	assert.JSONEq(t, `{"speed":55}`, string(got.Data))
}

func TestDecode_MultipleEvents(t *testing.T) {
	t.Parallel()
	originals := []cloudevent.RawEvent{
		makeEvent("a", json.RawMessage(`{"x":1}`)),
		makeEvent("b", json.RawMessage(`{"x":2}`)),
		makeEvent("c", json.RawMessage(`{"x":3}`)),
	}
	buf, _ := encodeToBuffer(t, originals, "multi")

	r := bytes.NewReader(buf.Bytes())
	events, err := Decode(r, int64(buf.Len()))
	require.NoError(t, err)
	require.Len(t, events, 3)

	for i, orig := range originals {
		assert.Equal(t, orig.ID, events[i].ID)
		assert.JSONEq(t, string(orig.Data), string(events[i].Data))
	}
}

func TestDecode_InvalidParquet(t *testing.T) {
	t.Parallel()
	r := bytes.NewReader([]byte("not a parquet file"))
	_, err := Decode(r, 18)
	assert.Error(t, err)
}

// --- Roundtrip tests ---

func TestRoundtrip_AllHeaderFields(t *testing.T) {
	t.Parallel()
	original := cloudevent.RawEvent{
		CloudEventHeader: cloudevent.CloudEventHeader{
			SpecVersion:     cloudevent.SpecVersion,
			Type:            "dimo.status",
			Source:          "0xSourceAddr",
			Subject:         "did:erc721:137:0xContract:42",
			ID:              "unique-id-123",
			Time:            time.Date(2025, 3, 15, 10, 30, 0, 0, time.UTC),
			DataContentType: "application/json",
			DataSchema:      "https://schema.example.com/v1",
			DataVersion:     "v3.0.0",
			Producer:        "0xProducerAddr",
		},
		Data: json.RawMessage(`{"temp":72.5}`),
	}

	buf, _ := encodeToBuffer(t, []cloudevent.RawEvent{original}, "roundtrip")

	r := bytes.NewReader(buf.Bytes())
	events, err := Decode(r, int64(buf.Len()))
	require.NoError(t, err)
	require.Len(t, events, 1)

	got := events[0]
	assert.Equal(t, cloudevent.SpecVersion, got.SpecVersion)
	assert.Equal(t, original.Type, got.Type)
	assert.Equal(t, original.Source, got.Source)
	assert.Equal(t, original.Subject, got.Subject)
	assert.Equal(t, original.ID, got.ID)
	assert.True(t, original.Time.Equal(got.Time), "time mismatch: want %v got %v", original.Time, got.Time)
	assert.Equal(t, original.DataContentType, got.DataContentType)
	assert.Equal(t, original.DataSchema, got.DataSchema)
	assert.Equal(t, original.DataVersion, got.DataVersion)
	assert.Equal(t, original.Producer, got.Producer)
	assert.JSONEq(t, string(original.Data), string(got.Data))
}

func TestRoundtrip_Signature(t *testing.T) {
	t.Parallel()
	original := makeEvent("sig-evt", json.RawMessage(`{}`))
	original.Signature = "0xdeadbeef"

	buf, _ := encodeToBuffer(t, []cloudevent.RawEvent{original}, "sig")

	r := bytes.NewReader(buf.Bytes())
	events, err := Decode(r, int64(buf.Len()))
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Equal(t, "0xdeadbeef", events[0].Signature)
}

func TestRoundtrip_RawEventID(t *testing.T) {
	t.Parallel()
	original := makeEvent("raw-ref-evt", json.RawMessage(`{}`))
	original.RawEventID = "raw-event-123"

	buf, _ := encodeToBuffer(t, []cloudevent.RawEvent{original}, "rawref")

	r := bytes.NewReader(buf.Bytes())
	events, err := Decode(r, int64(buf.Len()))
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Equal(t, "raw-event-123", events[0].RawEventID)
}

func TestRoundtrip_Tags(t *testing.T) {
	t.Parallel()
	original := makeEvent("tag-evt", json.RawMessage(`{}`))
	original.Tags = []string{"firmware", "ota", "v2"}

	buf, _ := encodeToBuffer(t, []cloudevent.RawEvent{original}, "tags")

	r := bytes.NewReader(buf.Bytes())
	events, err := Decode(r, int64(buf.Len()))
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Equal(t, []string{"firmware", "ota", "v2"}, events[0].Tags)
}

func TestRoundtrip_Extras(t *testing.T) {
	t.Parallel()
	original := makeEvent("ext-evt", json.RawMessage(`{}`))
	original.Extras = map[string]any{
		"custom_field": "custom_value",
		"priority":     float64(5),
	}

	buf, _ := encodeToBuffer(t, []cloudevent.RawEvent{original}, "extras")

	r := bytes.NewReader(buf.Bytes())
	events, err := Decode(r, int64(buf.Len()))
	require.NoError(t, err)
	require.Len(t, events, 1)

	assert.Equal(t, "custom_value", events[0].Extras["custom_field"])
	assert.Equal(t, float64(5), events[0].Extras["priority"])
}

func TestRoundtrip_DataBase64(t *testing.T) {
	t.Parallel()
	original := makeEvent("b64-evt", nil)
	original.Data = nil
	original.DataBase64 = "SGVsbG8gV29ybGQ="

	buf, _ := encodeToBuffer(t, []cloudevent.RawEvent{original}, "b64rt")

	r := bytes.NewReader(buf.Bytes())
	events, err := Decode(r, int64(buf.Len()))
	require.NoError(t, err)
	require.Len(t, events, 1)

	assert.Equal(t, "SGVsbG8gV29ybGQ=", events[0].DataBase64)
	assert.Nil(t, events[0].Data)
}

func TestRoundtrip_NilData(t *testing.T) {
	t.Parallel()
	original := makeEvent("nil-evt", nil)
	original.Data = nil

	buf, _ := encodeToBuffer(t, []cloudevent.RawEvent{original}, "nilrt")

	r := bytes.NewReader(buf.Bytes())
	events, err := Decode(r, int64(buf.Len()))
	require.NoError(t, err)
	require.Len(t, events, 1)

	assert.Nil(t, events[0].Data)
	assert.Empty(t, events[0].DataBase64)
}

func TestRoundtrip_EmptyExtrasStaysNil(t *testing.T) {
	t.Parallel()
	original := makeEvent("no-extras", json.RawMessage(`{"a":1}`))
	// No extras, no signature, no tags, no dataschema
	original.Extras = nil

	buf, _ := encodeToBuffer(t, []cloudevent.RawEvent{original}, "noext")

	r := bytes.NewReader(buf.Bytes())
	events, err := Decode(r, int64(buf.Len()))
	require.NoError(t, err)
	require.Len(t, events, 1)

	assert.Nil(t, events[0].Extras)
}

func TestRoundtrip_LargePayload(t *testing.T) {
	t.Parallel()
	// Build a ~10KB JSON payload
	m := make(map[string]string, 100)
	for i := 0; i < 100; i++ {
		m["field_"+string(rune('a'+i%26))+string(rune('0'+i/26))] = "value_with_some_length_to_fill_space"
	}
	data, err := json.Marshal(m)
	require.NoError(t, err)

	original := makeEvent("large", json.RawMessage(data))
	buf, _ := encodeToBuffer(t, []cloudevent.RawEvent{original}, "large")

	r := bytes.NewReader(buf.Bytes())
	events, err := Decode(r, int64(buf.Len()))
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.JSONEq(t, string(data), string(events[0].Data))
}

func TestRoundtrip_TimeMillisecondPrecision(t *testing.T) {
	t.Parallel()
	// Parquet stores time as millisecond timestamp — sub-ms is lost
	original := makeEvent("time-evt", json.RawMessage(`{}`))
	original.Time = time.Date(2025, 12, 31, 23, 59, 59, 123_000_000, time.UTC)

	buf, _ := encodeToBuffer(t, []cloudevent.RawEvent{original}, "time")

	r := bytes.NewReader(buf.Bytes())
	events, err := Decode(r, int64(buf.Len()))
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.True(t, original.Time.Truncate(time.Millisecond).Equal(events[0].Time),
		"want %v got %v", original.Time.Truncate(time.Millisecond), events[0].Time)
}

func TestRoundtrip_ManyEvents(t *testing.T) {
	t.Parallel()
	n := 500
	events := make([]cloudevent.RawEvent, n)
	for i := range events {
		events[i] = makeEvent("evt-"+strconv.Itoa(i), json.RawMessage(`{"idx":`+strconv.Itoa(i)+`}`))
	}

	buf, keys := encodeToBuffer(t, events, "many")
	require.Len(t, keys, n)

	r := bytes.NewReader(buf.Bytes())
	decoded, err := Decode(r, int64(buf.Len()))
	require.NoError(t, err)
	require.Len(t, decoded, n)

	for i := range decoded {
		assert.Equal(t, "evt-"+strconv.Itoa(i), decoded[i].ID)
	}
}

// --- Reader / SeekToRow tests ---

func TestReader_SeekToRow(t *testing.T) {
	t.Parallel()
	events := []cloudevent.RawEvent{
		makeEvent("first", json.RawMessage(`{"pos":"first"}`)),
		makeEvent("second", json.RawMessage(`{"pos":"second"}`)),
		makeEvent("third", json.RawMessage(`{"pos":"third"}`)),
	}
	buf, _ := encodeToBuffer(t, events, "seek")

	pr, err := OpenReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	defer func() { _ = pr.Close() }()

	assert.Equal(t, int64(3), pr.NumRows())

	// Seek to middle row
	got, err := pr.SeekToRow(1)
	require.NoError(t, err)
	assert.Equal(t, "second", got.ID)
	assert.JSONEq(t, `{"pos":"second"}`, string(got.Data))

	// Seek to first row
	got, err = pr.SeekToRow(0)
	require.NoError(t, err)
	assert.Equal(t, "first", got.ID)

	// Seek to last row
	got, err = pr.SeekToRow(2)
	require.NoError(t, err)
	assert.Equal(t, "third", got.ID)
}

func TestReader_SeekToRow_OutOfRange(t *testing.T) {
	t.Parallel()
	events := []cloudevent.RawEvent{makeEvent("only", json.RawMessage(`{}`))}
	buf, _ := encodeToBuffer(t, events, "oor")

	pr, err := OpenReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	defer func() { _ = pr.Close() }()

	_, err = pr.SeekToRow(-1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "out of range")

	_, err = pr.SeekToRow(1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "out of range")

	_, err = pr.SeekToRow(100)
	assert.Error(t, err)
}

func TestSeekToRow_PackageLevelFunction(t *testing.T) {
	t.Parallel()
	events := []cloudevent.RawEvent{
		makeEvent("a", json.RawMessage(`{"v":1}`)),
		makeEvent("b", json.RawMessage(`{"v":2}`)),
	}
	buf, _ := encodeToBuffer(t, events, "plseek")
	r := bytes.NewReader(buf.Bytes())

	got, err := SeekToRow(r, int64(buf.Len()), 1)
	require.NoError(t, err)
	assert.Equal(t, "b", got.ID)
	assert.JSONEq(t, `{"v":2}`, string(got.Data))
}

func TestSeekToRow_InvalidParquet(t *testing.T) {
	t.Parallel()
	r := bytes.NewReader([]byte("garbage"))
	_, err := SeekToRow(r, 7, 0)
	assert.Error(t, err)
}

// --- IsParquetRef tests ---

func TestIsParquetRef(t *testing.T) {
	t.Parallel()
	tests := []struct {
		key  string
		want bool
	}{
		{"obj/key#0", true},
		{"obj/key#123", true},
		{"#0", true},
		{"key#", true},
		{"no-hash-here", false},
		{"", false},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, IsParquetRef(tt.key), "IsParquetRef(%q)", tt.key)
	}
}

// --- ParseIndexKey tests ---

func TestParseIndexKey_Valid(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input   string
		wantKey string
		wantRow int64
	}{
		{"obj/key#0", "obj/key", 0},
		{"obj/key#42", "obj/key", 42},
		{"a/b/c#999", "a/b/c", 999},
		{"key#with#hashes#3", "key#with#hashes", 3},
	}
	for _, tt := range tests {
		key, row, err := ParseIndexKey(tt.input)
		require.NoError(t, err, "ParseIndexKey(%q)", tt.input)
		assert.Equal(t, tt.wantKey, key)
		assert.Equal(t, tt.wantRow, row)
	}
}

func TestParseIndexKey_NoSeparator(t *testing.T) {
	t.Parallel()
	_, _, err := ParseIndexKey("no-hash")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not contain '#'")
}

func TestParseIndexKey_InvalidRowOffset(t *testing.T) {
	t.Parallel()
	_, _, err := ParseIndexKey("key#notanumber")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parsing row offset")
}

func TestParseIndexKey_EmptyRowOffset(t *testing.T) {
	t.Parallel()
	_, _, err := ParseIndexKey("key#")
	require.Error(t, err)
}

// --- Integration: Encode → ParseIndexKey → SeekToRow ---

func TestEncodeDecodeSeek_Integration(t *testing.T) {
	t.Parallel()
	events := []cloudevent.RawEvent{
		makeEvent("ev-0", json.RawMessage(`{"sensor":"gps","lat":40.7}`)),
		makeEvent("ev-1", json.RawMessage(`{"sensor":"obd","rpm":3200}`)),
		makeEvent("ev-2", json.RawMessage(`{"sensor":"accel","g":1.02}`)),
	}

	buf, keys := encodeToBuffer(t, events, "s3://bucket/obj")
	r := bytes.NewReader(buf.Bytes())

	// Use the returned index keys to seek individual rows
	for i, indexKey := range keys {
		objKey, rowOffset, err := ParseIndexKey(indexKey)
		require.NoError(t, err)
		assert.Equal(t, "s3://bucket/obj", objKey)
		assert.Equal(t, int64(i), rowOffset)
		assert.True(t, IsParquetRef(indexKey))

		got, err := SeekToRow(r, int64(buf.Len()), rowOffset)
		require.NoError(t, err)
		assert.Equal(t, events[i].ID, got.ID)
		assert.JSONEq(t, string(events[i].Data), string(got.Data))
	}
}

// --- Roundtrip: combined non-column fields ---

func TestRoundtrip_AllNonColumnFields(t *testing.T) {
	t.Parallel()
	original := makeEvent("full-evt", json.RawMessage(`{"complete":true}`))
	original.DataSchema = "https://schema.dimo.zone/status/v2"
	original.Signature = "0xabcdef1234567890"
	original.RawEventID = "raw-event-123"
	original.Tags = []string{"tagged", "event"}
	original.Extras = map[string]any{
		"custom": "value",
	}

	buf, _ := encodeToBuffer(t, []cloudevent.RawEvent{original}, "allfields")

	r := bytes.NewReader(buf.Bytes())
	events, err := Decode(r, int64(buf.Len()))
	require.NoError(t, err)
	require.Len(t, events, 1)

	got := events[0]
	assert.Equal(t, original.DataSchema, got.DataSchema)
	assert.Equal(t, original.Signature, got.Signature)
	assert.Equal(t, original.RawEventID, got.RawEventID)
	assert.Equal(t, original.Tags, got.Tags)
	assert.Equal(t, "value", got.Extras["custom"])
	assert.NotContains(t, got.Extras, "signature")
	assert.NotContains(t, got.Extras, "raweventid")
}

func TestRoundtrip_DataIndexKey(t *testing.T) {
	t.Parallel()
	stored := []cloudevent.StoredEvent{
		{
			RawEvent:     makeEvent("inline", json.RawMessage(`{"x":1}`)),
			DataIndexKey: "",
		},
		{
			RawEvent:     makeEvent("external", nil),
			DataIndexKey: "payloads/2025/06/15/external-1.bin",
		},
	}
	// External event has no inline payload.
	stored[1].Data = nil

	var buf bytes.Buffer
	_, err := Encode(&buf, stored, "mixed")
	require.NoError(t, err)

	r := bytes.NewReader(buf.Bytes())
	got, err := Decode(r, int64(buf.Len()))
	require.NoError(t, err)
	require.Len(t, got, 2)

	assert.Equal(t, "", got[0].DataIndexKey)
	assert.JSONEq(t, `{"x":1}`, string(got[0].Data))

	assert.Equal(t, "payloads/2025/06/15/external-1.bin", got[1].DataIndexKey)
	assert.Empty(t, got[1].Data)
	assert.Empty(t, got[1].DataBase64)
}

// oldParquetRow mirrors the pre-data_index_key schema. Used to verify that
// the current decoder reads bundles written before data_index_key was added,
// returning empty DataIndexKey for those rows.
type oldParquetRow struct {
	Subject         string    `parquet:"subject"`
	Time            time.Time `parquet:"time,timestamp(millisecond)"`
	Type            string    `parquet:"type"`
	ID              string    `parquet:"id"`
	Source          string    `parquet:"source"`
	Producer        string    `parquet:"producer"`
	DataContentType string    `parquet:"data_content_type"`
	DataVersion     string    `parquet:"data_version"`
	Extras          string    `parquet:"extras"`
	Data            *string   `parquet:"data,optional"`
	DataBase64      []byte    `parquet:"data_base64,optional"`
}

func TestDecode_OldBundleWithoutDataIndexKey(t *testing.T) {
	t.Parallel()

	payload := `{"speed":55}`
	row := oldParquetRow{
		Subject:         "did:erc721:137:0xABCD:1",
		Time:            time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC),
		Type:            "dimo.status",
		ID:              "old-evt",
		Source:          "0xABCD",
		Producer:        "0x1234",
		DataContentType: "application/json",
		DataVersion:     "v2.0.0",
		Extras:          "{}",
		Data:            &payload,
	}

	var buf bytes.Buffer
	w := pq.NewGenericWriter[oldParquetRow](&buf, pq.Compression(&snappy.Codec{}))
	_, err := w.Write([]oldParquetRow{row})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	got, err := Decode(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	require.Len(t, got, 1)

	assert.Equal(t, "old-evt", got[0].ID)
	assert.JSONEq(t, payload, string(got[0].Data))
	assert.Equal(t, "", got[0].DataIndexKey, "old bundle should read back with empty DataIndexKey")
}

func TestRoundtrip_EmptyStringFields(t *testing.T) {
	t.Parallel()
	original := cloudevent.RawEvent{
		CloudEventHeader: cloudevent.CloudEventHeader{
			SpecVersion: cloudevent.SpecVersion,
			Type:        "dimo.status",
			Source:      "",
			Subject:     "",
			ID:          "empty-fields",
			Time:        time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		Data: json.RawMessage(`{}`),
	}

	buf, _ := encodeToBuffer(t, []cloudevent.RawEvent{original}, "empty")

	r := bytes.NewReader(buf.Bytes())
	events, err := Decode(r, int64(buf.Len()))
	require.NoError(t, err)
	require.Len(t, events, 1)

	assert.Equal(t, "", events[0].Source)
	assert.Equal(t, "", events[0].Subject)
	assert.Equal(t, "empty-fields", events[0].ID)
}
