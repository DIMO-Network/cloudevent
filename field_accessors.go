package cloudevent

// RestoreNonColumnFields restores non-column fields from Extras.
func RestoreNonColumnFields(event *CloudEventHeader) {
	event.SpecVersion = SpecVersion
	if event.Extras != nil {
		delete(event.Extras, "specversion")
	}
	if len(event.Extras) == 0 {
		return
	}
	if val, ok := event.Extras["dataschema"]; ok {
		if typedVal, ok := val.(string); ok {
			event.DataSchema = typedVal
		}
		delete(event.Extras, "dataschema")
	}
	if val, ok := event.Extras["signature"]; ok {
		if typedVal, ok := val.(string); ok {
			event.Signature = typedVal
		}
		delete(event.Extras, "signature")
	}
	if val, ok := event.Extras["raweventid"]; ok {
		if typedVal, ok := val.(string); ok {
			event.RawEventID = typedVal
		}
		delete(event.Extras, "raweventid")
	}
	if val, ok := event.Extras["tags"]; ok {
		if anySlice, ok := val.([]any); ok {
			// Skip non-string elements rather than panic on the unchecked assertion:
			// stored extras come from arbitrary producer JSON (e.g. a backfilled bundle
			// with {"tags":[42]} or a null element), and the parquet Decode path has no
			// recover — an unguarded v.(string) would crash the reader/backfill.
			typedSlice := make([]string, 0, len(anySlice))
			for _, v := range anySlice {
				if s, ok := v.(string); ok {
					typedSlice = append(typedSlice, s)
				}
			}
			event.Tags = typedSlice
		}
		delete(event.Extras, "tags")
	}
}

// AddNonColumnFieldsToExtras adds fields without dedicated columns to Extras.
// Returns nil when there are no extras and no non-column fields to add.
func AddNonColumnFieldsToExtras(event *CloudEventHeader) map[string]any {
	hasNonColumn := event.DataSchema != "" || event.Signature != "" || event.RawEventID != "" || len(event.Tags) > 0
	if !hasNonColumn && len(event.Extras) == 0 {
		return nil
	}

	extras := make(map[string]any, len(event.Extras))
	for k, v := range event.Extras {
		extras[k] = v
	}

	if event.DataSchema != "" {
		extras["dataschema"] = event.DataSchema
	}
	if event.Signature != "" {
		extras["signature"] = event.Signature
	}
	if event.RawEventID != "" {
		extras["raweventid"] = event.RawEventID
	}
	if len(event.Tags) > 0 {
		extras["tags"] = event.Tags
	}
	return extras
}
