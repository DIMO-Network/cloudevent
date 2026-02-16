package cloudevent

import (
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
	if dataBase64 != "" {
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
func (c CloudEvent[A]) MarshalJSON() ([]byte, error) {
	data, err := json.Marshal(c.CloudEventHeader)
	if err != nil {
		return nil, err
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

// getJSONFieldNames returns a map of the JSON field names for the given type.
// It is used to determine which fields are defined in the CloudEventHeader and which should be added to the Extras map.
func getJSONFieldNames(t reflect.Type) map[string]struct{} {
	fields := map[string]struct{}{}

	for i := range t.NumField() {
		field := t.Field(i)

		tag := field.Tag.Get("json")
		if tag == "" {
			continue
		}

		name := tag
		if comma := strings.Index(tag, ","); comma != -1 {
			name = tag[:comma]
		}

		if name == "-" {
			continue
		}

		fields[name] = struct{}{}
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
// "data" and "data_base64" for RawEvent.
func unmarshalCloudEventWithPayload(data []byte, payloadFunc func(dataRaw json.RawMessage, dataBase64 string) error) (CloudEventHeader, error) {
	// Unmarshal known header fields via the type alias (no custom UnmarshalJSON).
	var c CloudEventHeader
	if err := json.Unmarshal(data, (*cloudEventHeader)(&c)); err != nil {
		return c, err
	}
	c.SpecVersion = SpecVersion

	// Second pass into raw map to extract data, data_base64, and extras.
	rawFields := make(map[string]json.RawMessage)
	if err := json.Unmarshal(data, &rawFields); err != nil {
		return c, err
	}

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

	for key, rawValue := range rawFields {
		if _, ok := definedCloudeEventHdrFields[key]; ok {
			continue
		}
		if key == "data" || key == "data_base64" {
			continue
		}
		if c.Extras == nil {
			c.Extras = make(map[string]any)
		}
		var value any
		if err := json.Unmarshal(rawValue, &value); err != nil {
			return c, err
		}
		c.Extras[key] = value
	}
	return c, nil
}

// ignoreDataField is a function that ignores the data field.
// It is used when unmarshalling the CloudEventHeader so that the data field is not added to the Extras map.
func ignoreDataField(json.RawMessage) error { return nil }
