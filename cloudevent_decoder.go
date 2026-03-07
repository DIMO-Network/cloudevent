package cloudevent

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/tidwall/gjson"
)

var knownHeaderFields = map[string]struct{}{
	"specversion": {}, "type": {}, "source": {}, "subject": {}, "id": {},
	"time": {}, "datacontenttype": {}, "dataschema": {}, "dataversion": {},
	"producer": {}, "signature": {}, "tags": {},
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

// UnmarshalJSON implements custom JSON unmarshaling for CloudEventHeader.
func (c *CloudEventHeader) UnmarshalJSON(data []byte) error {
	header, _, _, err := unmarshalHeader(data)
	if err != nil {
		return err
	}
	*c = header
	return nil
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
