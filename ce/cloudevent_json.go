package cloudevent

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/tidwall/gjson"
)

var knownHeaderFields = map[string]struct{}{
	"specversion": {}, "type": {}, "source": {}, "subject": {}, "id": {},
	"time": {}, "datacontenttype": {}, "dataschema": {}, "dataversion": {},
	"producer": {}, "signature": {}, "tags": {},
}

const hexTable = "0123456789abcdef"

var bufPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}

// appendJSONString writes a JSON-escaped string (with quotes) to buf.
func appendJSONString(buf *bytes.Buffer, s string) {
	buf.WriteByte('"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '"':
			buf.WriteString(`\"`)
		case c == '\\':
			buf.WriteString(`\\`)
		case c < 0x20:
			buf.WriteString(`\u00`)
			buf.WriteByte(hexTable[c>>4])
			buf.WriteByte(hexTable[c&0xF])
		default:
			buf.WriteByte(c)
		}
	}
	buf.WriteByte('"')
}

// writeStringField writes ,"key":"value" to buf. key must not need escaping.
func writeStringField(buf *bytes.Buffer, key, value string) {
	buf.WriteString(`,"`)
	buf.WriteString(key)
	buf.WriteString(`":`)
	appendJSONString(buf, value)
}

// marshalHeaderTo writes header JSON fields to buf (without braces).
func (c *CloudEventHeader) marshalHeaderTo(buf *bytes.Buffer) error {
	buf.WriteString(`"specversion":"1.0"`)
	writeStringField(buf, "type", c.Type)
	writeStringField(buf, "source", c.Source)
	writeStringField(buf, "subject", c.Subject)
	writeStringField(buf, "id", c.ID)

	buf.WriteString(`,"time":"`)
	buf.WriteString(c.Time.Format(time.RFC3339Nano))
	buf.WriteByte('"')

	if c.DataContentType != "" {
		writeStringField(buf, "datacontenttype", c.DataContentType)
	}
	if c.DataSchema != "" {
		writeStringField(buf, "dataschema", c.DataSchema)
	}
	if c.DataVersion != "" {
		writeStringField(buf, "dataversion", c.DataVersion)
	}

	writeStringField(buf, "producer", c.Producer)

	if c.Signature != "" {
		writeStringField(buf, "signature", c.Signature)
	}
	if len(c.Tags) > 0 {
		buf.WriteString(`,"tags":[`)
		for i, tag := range c.Tags {
			if i > 0 {
				buf.WriteByte(',')
			}
			appendJSONString(buf, tag)
		}
		buf.WriteByte(']')
	}

	for k, v := range c.Extras {
		buf.WriteByte(',')
		appendJSONString(buf, k)
		buf.WriteByte(':')
		vb, err := json.Marshal(v)
		if err != nil {
			return err
		}
		buf.Write(vb)
	}
	return nil
}

// unmarshalHeader parses CloudEvent JSON with gjson and returns the populated
// header, raw data bytes, and data_base64 string.
func unmarshalHeader(data []byte) (CloudEventHeader, []byte, string, error) {
	result := gjson.ParseBytes(data)
	if !result.IsObject() {
		return CloudEventHeader{}, nil, "", fmt.Errorf("cloudevent: expected JSON object")
	}

	var header CloudEventHeader
	header.SpecVersion = SpecVersion
	header.Type = result.Get("type").Str
	header.Source = result.Get("source").Str
	header.Subject = result.Get("subject").Str
	header.ID = result.Get("id").Str
	header.Producer = result.Get("producer").Str
	header.DataContentType = result.Get("datacontenttype").Str
	header.DataSchema = result.Get("dataschema").Str
	header.DataVersion = result.Get("dataversion").Str
	header.Signature = result.Get("signature").Str

	if tr := result.Get("time"); tr.Exists() {
		if tr.Type != gjson.String {
			return CloudEventHeader{}, nil, "", fmt.Errorf("cloudevent: time must be a string")
		}
		t, err := time.Parse(time.RFC3339Nano, tr.Str)
		if err != nil {
			return CloudEventHeader{}, nil, "", fmt.Errorf("cloudevent: invalid time: %w", err)
		}
		header.Time = t
	}

	if tr := result.Get("tags"); tr.Exists() && tr.IsArray() {
		tags := make([]string, 0, int(tr.Get("#").Int()))
		tr.ForEach(func(_, v gjson.Result) bool {
			tags = append(tags, v.Str)
			return true
		})
		header.Tags = tags
	}

	// Extras: iterate all keys, skip known + data fields
	result.ForEach(func(key, value gjson.Result) bool {
		k := key.Str
		if k == "data" || k == "data_base64" {
			return true
		}
		if _, known := knownHeaderFields[k]; known {
			return true
		}
		if header.Extras == nil {
			header.Extras = make(map[string]any)
		}
		header.Extras[k] = value.Value()
		return true
	})

	// data_base64
	var dataBase64 string
	if db64 := result.Get("data_base64"); db64.Exists() {
		if db64.Type != gjson.String {
			return CloudEventHeader{}, nil, "", fmt.Errorf("cloudevent: data_base64 must be a string")
		}
		dataBase64 = db64.Str
	}

	// data (raw bytes)
	var dataRaw []byte
	if dr := result.Get("data"); dr.Exists() {
		dataRaw = []byte(dr.Raw)
	}

	return header, dataRaw, dataBase64, nil
}

// UnmarshalJSON implements custom JSON unmarshaling for CloudEvent.
// It transparently handles both "data" and "data_base64" wire formats.
// For RawEvent (CloudEvent[json.RawMessage]), Data is set to the raw payload bytes.
func (c *CloudEvent[A]) UnmarshalJSON(data []byte) error {
	header, dataRaw, dataBase64, err := unmarshalHeader(data)
	if err != nil {
		return err
	}

	if dataRaw != nil && dataBase64 != "" {
		return fmt.Errorf("cloudevent: both \"data\" and \"data_base64\" present; only one allowed")
	}
	c.CloudEventHeader = header

	// RawEvent: assign payload bytes directly to Data
	if ptr, ok := (any)(&c.Data).(*json.RawMessage); ok {
		if dataBase64 != "" {
			decoded, err := base64.StdEncoding.DecodeString(dataBase64)
			if err != nil {
				return err
			}
			*ptr = decoded
			c.DataBase64 = dataBase64
		} else {
			*ptr = dataRaw
			c.DataBase64 = ""
		}
		return nil
	}

	// Typed CloudEvent: unmarshal into Data
	if dataBase64 != "" {
		decoded, err := base64.StdEncoding.DecodeString(dataBase64)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(decoded, &c.Data); err != nil {
			return err
		}
		c.DataBase64 = dataBase64
	} else if dataRaw != nil {
		if err := json.Unmarshal(dataRaw, &c.Data); err != nil {
			return err
		}
	}
	return nil
}

// MarshalJSON implements custom JSON marshaling for CloudEvent[A].
func (c CloudEvent[A]) MarshalJSON() ([]byte, error) {
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()

	buf.WriteByte('{')
	if err := c.CloudEventHeader.marshalHeaderTo(buf); err != nil {
		bufPool.Put(buf)
		return nil, err
	}

	if raw, ok := (any)(c.Data).(json.RawMessage); ok {
		if len(raw) > 0 || c.DataBase64 != "" {
			if c.DataBase64 != "" {
				writeStringField(buf, "data_base64", c.DataBase64)
			} else if IsJSONDataContentType(c.DataContentType) || (c.DataContentType == "" && json.Valid(raw)) {
				buf.WriteString(`,"data":`)
				buf.Write(raw)
			} else {
				writeStringField(buf, "data_base64", base64.StdEncoding.EncodeToString(raw))
			}
		}
	} else {
		if c.DataBase64 != "" {
			writeStringField(buf, "data_base64", c.DataBase64)
		} else {
			dataBytes, err := json.Marshal(c.Data)
			if err != nil {
				bufPool.Put(buf)
				return nil, err
			}
			buf.WriteString(`,"data":`)
			buf.Write(dataBytes)
		}
	}

	buf.WriteByte('}')

	result := make([]byte, buf.Len())
	copy(result, buf.Bytes())
	bufPool.Put(buf)
	return result, nil
}

// UnmarshalJSON implements custom JSON unmarshaling for CloudEventHeader.
func (c *CloudEventHeader) UnmarshalJSON(data []byte) error {
	header, _, _, err := unmarshalHeader(data)
	if err != nil {
		return err
	}
	*c = header
	return nil
}

// MarshalJSON implements custom JSON marshaling for CloudEventHeader.
func (c CloudEventHeader) MarshalJSON() ([]byte, error) {
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()

	buf.WriteByte('{')
	if err := c.marshalHeaderTo(buf); err != nil {
		bufPool.Put(buf)
		return nil, err
	}
	buf.WriteByte('}')

	result := make([]byte, buf.Len())
	copy(result, buf.Bytes())
	bufPool.Put(buf)
	return result, nil
}

// DecodeHeader parses only the CloudEvent header fields from JSON, skipping the data payload.
// More efficient than unmarshaling a full event when only metadata is needed (e.g. for scaling).
func DecodeHeader(data []byte) (CloudEventHeader, error) {
	var hdr CloudEventHeader
	if err := json.Unmarshal(data, &hdr); err != nil {
		return CloudEventHeader{}, err
	}
	return hdr, nil
}
