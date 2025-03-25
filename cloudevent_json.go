package cloudevent

import (
	"encoding/json"
	"reflect"
	"strings"

	"github.com/tidwall/sjson"
)

var definedCloudeEventHdrFields = getJSONFieldNames(reflect.TypeOf(CloudEventHeader{}))

type cloudEventHeader CloudEventHeader

// UnmarshalJSON implements custom JSON unmarshaling for CloudEvent.
func (c *CloudEvent[A]) UnmarshalJSON(data []byte) error {
	var err error
	c.CloudEventHeader, err = unmarshalCloudEvent(data, c.setDataField)
	return err
}

// MarshalJSON implements custom JSON marshaling for CloudEventHeader.
func (c CloudEvent[A]) MarshalJSON() ([]byte, error) {
	// Marshal the base struct
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
	// Marshal the base struct
	aux := (cloudEventHeader)(c)
	aux.SpecVersion = SpecVersion
	data, err := json.Marshal(aux)
	if err != nil {
		return nil, err
	}
	// Add all extras using sjson]
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
	c := CloudEventHeader{}
	aux := cloudEventHeader{}
	// Unmarshal known fields directly into the struct
	if err := json.Unmarshal(data, &aux); err != nil {
		return c, err
	}
	aux.SpecVersion = SpecVersion
	c = (CloudEventHeader)(aux)
	// Create a map to hold all JSON fields
	rawFields := make(map[string]json.RawMessage)
	if err := json.Unmarshal(data, &rawFields); err != nil {
		return c, err
	}

	// Separate known and unknown fields
	for key, rawValue := range rawFields {
		if _, ok := definedCloudeEventHdrFields[key]; ok {
			// Skip defined fields
			continue
		}
		if key == "data" {
			if err := dataFunc(rawValue); err != nil {
				return c, err
			}
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
