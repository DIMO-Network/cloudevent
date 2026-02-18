package cloudevent

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/tidwall/sjson"
)

var definedCloudeEventHdrFields = getJSONFieldNames(reflect.TypeFor[CloudEventHeader]())

type cloudEventHeader CloudEventHeader

// UnmarshalJSON implements custom JSON unmarshaling for CloudEvent.
// It transparently handles both "data" and "data_base64" wire formats.
// For RawEvent (CloudEvent[json.RawMessage]), Data is set to the raw payload bytes.
func (c *CloudEvent[A]) UnmarshalJSON(data []byte) error {
	var dataRaw json.RawMessage
	var dataBase64 string
	header, err := unmarshalCloudEventWithPayload(data, func(d json.RawMessage, b64 string) error {
		dataRaw = d
		dataBase64 = b64
		return nil
	})
	if err != nil {
		return err
	}
	c.CloudEventHeader = header
	if dataRaw != nil && dataBase64 != "" {
		return fmt.Errorf("cloudevent: both \"data\" and \"data_base64\" present; only one allowed")
	}
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
// When DataBase64 is set, emits "data_base64"; otherwise emits "data".
// For RawEvent, uses DataContentType to choose wire form when DataBase64 is empty.
func (c CloudEvent[A]) MarshalJSON() ([]byte, error) {
	data, err := json.Marshal(c.CloudEventHeader)
	if err != nil {
		return nil, err
	}
	// RawEvent: choose data vs data_base64 by content type when DataBase64 not set
	if raw, ok := (any)(c.Data).(json.RawMessage); ok {
		if len(raw) > 0 || c.DataBase64 != "" {
			if c.DataBase64 != "" {
				data, err = sjson.SetBytes(data, "data_base64", c.DataBase64)
			} else if IsJSONDataContentType(c.DataContentType) || (c.DataContentType == "" && json.Valid(raw)) {
				data, err = sjson.SetRawBytes(data, "data", raw)
			} else {
				data, err = sjson.SetBytes(data, "data_base64", base64.StdEncoding.EncodeToString(raw))
			}
			if err != nil {
				return nil, err
			}
		}
		return data, nil
	}
	if c.DataBase64 != "" {
		data, err = sjson.SetBytes(data, "data_base64", c.DataBase64)
	} else {
		data, err = sjson.SetBytes(data, "data", c.Data)
	}
	if err != nil {
		return nil, err
	}
	return data, nil
}

// UnmarshalJSON implements custom JSON unmarshaling for CloudEventHeader.
func (c *CloudEventHeader) UnmarshalJSON(data []byte) error {
	var err error
	*c, err = unmarshalCloudEvent(data, ignoreDataField)
	return err
}

// MarshalJSON implements custom JSON marshaling for CloudEventHeader.
func (c CloudEventHeader) MarshalJSON() ([]byte, error) {
	aux := (cloudEventHeader)(c)
	aux.SpecVersion = SpecVersion
	data, err := json.Marshal(aux)
	if err != nil {
		return nil, err
	}
	for k, v := range c.Extras {
		data, err = sjson.SetBytes(data, k, v)
		if err != nil {
			return nil, err
		}
	}
	return data, nil
}

// jsonFieldKey returns the JSON object key for f, or "" if the field has no json tag or is "-".
func jsonFieldKey(f reflect.StructField) string {
	tag := f.Tag.Get("json")
	if tag == "" {
		return ""
	}
	name := tag
	if comma := strings.Index(tag, ","); comma != -1 {
		name = tag[:comma]
	}
	if name == "-" {
		return ""
	}
	return name
}

// getJSONFieldNames returns a map of the JSON field names for the given type.
// It is used to determine which fields are defined in the CloudEventHeader and which should be added to the Extras map.
func getJSONFieldNames(t reflect.Type) map[string]struct{} {
	fields := map[string]struct{}{}
	for i := range t.NumField() {
		if key := jsonFieldKey(t.Field(i)); key != "" {
			fields[key] = struct{}{}
		}
	}
	return fields
}

// unmarshalCloudEvent unmarshals the CloudEventHeader and data field.
func unmarshalCloudEvent(data []byte, dataFunc func(json.RawMessage) error) (CloudEventHeader, error) {
	return unmarshalCloudEventWithPayload(data, func(dataRaw json.RawMessage, _ string) error {
		return dataFunc(dataRaw)
	})
}

// unmarshalCloudEventWithPayload unmarshals the CloudEventHeader and returns both
// "data" and "data_base64" for CloudEvent payload. Single parse into
// map[string]json.RawMessage; known keys are unmarshaled into the header struct,
// unknown keys go to Extras, then data/data_base64 are passed to payloadFunc.
func unmarshalCloudEventWithPayload(data []byte, payloadFunc func(dataRaw json.RawMessage, dataBase64 string) error) (CloudEventHeader, error) {
	rawFields := make(map[string]json.RawMessage)
	if err := json.Unmarshal(data, &rawFields); err != nil {
		return CloudEventHeader{}, err
	}

	// Reassemble header keys into a single JSON object and unmarshal once (no per-field routing).
	headerRaw := make(map[string]json.RawMessage, len(rawFields))
	for key, raw := range rawFields {
		if key == "data" || key == "data_base64" {
			continue
		}
		if _, defined := definedCloudeEventHdrFields[key]; defined {
			headerRaw[key] = raw
		}
	}
	var c CloudEventHeader
	if len(headerRaw) > 0 {
		headerBytes, err := json.Marshal(headerRaw)
		if err != nil {
			return CloudEventHeader{}, err
		}
		var aux cloudEventHeader
		if err := json.Unmarshal(headerBytes, &aux); err != nil {
			return CloudEventHeader{}, err
		}
		c = CloudEventHeader(aux)
	}
	c.SpecVersion = SpecVersion

	// Unknown keys → Extras
	for key, raw := range rawFields {
		if key == "data" || key == "data_base64" {
			continue
		}
		if _, defined := definedCloudeEventHdrFields[key]; defined {
			continue
		}
		if c.Extras == nil {
			c.Extras = make(map[string]any)
		}
		var value any
		if err := json.Unmarshal(raw, &value); err != nil {
			return c, err
		}
		c.Extras[key] = value
	}

	// Payload
	var dataRaw json.RawMessage
	var dataBase64 string
	if raw, ok := rawFields["data_base64"]; ok && len(raw) > 0 {
		if err := json.Unmarshal(raw, &dataBase64); err != nil {
			return c, err
		}
	}
	if raw, ok := rawFields["data"]; ok {
		dataRaw = raw
	}
	if dataRaw != nil || dataBase64 != "" {
		if err := payloadFunc(dataRaw, dataBase64); err != nil {
			return c, err
		}
	}
	return c, nil
}

// ignoreDataField is a function that ignores the data field.
// It is used when unmarshalling the CloudEventHeader so that the data field is not added to the Extras map.
func ignoreDataField(json.RawMessage) error { return nil }

// DecodeHeader parses only the CloudEvent header fields from JSON, skipping the data payload.
// More efficient than unmarshaling a full event when only metadata is needed (e.g. for scaling).
//
// Equivalent to json.Unmarshal(data, &header) since CloudEventHeader.UnmarshalJSON
// already ignores the data field, but provided as an explicit API for discoverability.
func DecodeHeader(data []byte) (CloudEventHeader, error) {
	var hdr CloudEventHeader
	if err := json.Unmarshal(data, &hdr); err != nil {
		return CloudEventHeader{}, err
	}
	return hdr, nil
}
