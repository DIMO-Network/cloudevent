package cloudevent

import (
	"encoding/json"
	"reflect"
	"strings"

	"github.com/tidwall/sjson"
)

var definedCloudeEventHdrFields = getJSONFieldNames(reflect.TypeFor[CloudEventHeader]())

type cloudEventHeader CloudEventHeader

// UnmarshalJSON implements custom JSON unmarshaling for CloudEvent.
func (c *CloudEvent[A]) UnmarshalJSON(data []byte) error {
	var err error
	c.CloudEventHeader, err = unmarshalCloudEvent(data, c.setDataField)
	return err
}

// MarshalJSON implements custom JSON marshaling for CloudEvent[A].
func (c CloudEvent[A]) MarshalJSON() ([]byte, error) {
	data, err := json.Marshal(c.CloudEventHeader)
	if err != nil {
		return nil, err
	}
	data, err = sjson.SetBytes(data, "data", c.Data)
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
// Single-pass: decode only into map[string]json.RawMessage, then fill header from raw fields.
func unmarshalCloudEventWithPayload(data []byte, payloadFunc func(dataRaw json.RawMessage, dataBase64 string) error) (CloudEventHeader, error) {
	c := CloudEventHeader{}
	rawFields := make(map[string]json.RawMessage)
	if err := json.Unmarshal(data, &rawFields); err != nil {
		return c, err
	}

	// Populate known header fields from raw values (one small unmarshal per field).
	if raw, ok := rawFields["id"]; ok && len(raw) > 0 {
		if err := json.Unmarshal(raw, &c.ID); err != nil {
			return c, err
		}
	}
	if raw, ok := rawFields["source"]; ok && len(raw) > 0 {
		if err := json.Unmarshal(raw, &c.Source); err != nil {
			return c, err
		}
	}
	if raw, ok := rawFields["producer"]; ok && len(raw) > 0 {
		if err := json.Unmarshal(raw, &c.Producer); err != nil {
			return c, err
		}
	}
	c.SpecVersion = SpecVersion
	if raw, ok := rawFields["subject"]; ok && len(raw) > 0 {
		if err := json.Unmarshal(raw, &c.Subject); err != nil {
			return c, err
		}
	}
	if raw, ok := rawFields["time"]; ok && len(raw) > 0 {
		if err := json.Unmarshal(raw, &c.Time); err != nil {
			return c, err
		}
	}
	if raw, ok := rawFields["type"]; ok && len(raw) > 0 {
		if err := json.Unmarshal(raw, &c.Type); err != nil {
			return c, err
		}
	}
	if raw, ok := rawFields["datacontenttype"]; ok && len(raw) > 0 {
		if err := json.Unmarshal(raw, &c.DataContentType); err != nil {
			return c, err
		}
	}
	if raw, ok := rawFields["dataschema"]; ok && len(raw) > 0 {
		if err := json.Unmarshal(raw, &c.DataSchema); err != nil {
			return c, err
		}
	}
	if raw, ok := rawFields["dataversion"]; ok && len(raw) > 0 {
		if err := json.Unmarshal(raw, &c.DataVersion); err != nil {
			return c, err
		}
	}
	if raw, ok := rawFields["signature"]; ok && len(raw) > 0 {
		if err := json.Unmarshal(raw, &c.Signature); err != nil {
			return c, err
		}
	}
	if raw, ok := rawFields["tags"]; ok && len(raw) > 0 {
		if err := json.Unmarshal(raw, &c.Tags); err != nil {
			return c, err
		}
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

// setDataField is a function that sets the data field.
// It is used to unmarshal the data field into the CloudEvent[A].Data field.
func (c *CloudEvent[A]) setDataField(data json.RawMessage) error {
	return json.Unmarshal(data, &c.Data)
}
