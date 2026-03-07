package cloudevent

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"sync"
	"time"
)

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

// MarshalJSON implements custom JSON marshaling for CloudEvent[A].
func (c CloudEvent[A]) MarshalJSON() ([]byte, error) {
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()

	buf.WriteByte('{')
	if err := c.marshalHeaderTo(buf); err != nil {
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
