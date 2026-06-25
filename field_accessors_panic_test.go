package cloudevent

import "testing"

// TestRestoreNonColumnFields_MalformedTags: a stored tags array with non-string
// elements (arbitrary producer JSON / a backfilled bundle) must not panic. Non-string
// elements are skipped; strings are kept in order. Before the fix the unchecked
// v.(string) panicked on the parquet Decode path, which has no recover.
func TestRestoreNonColumnFields_MalformedTags(t *testing.T) {
	hdr := &CloudEventHeader{
		Extras: map[string]any{"tags": []any{"ok", float64(42), nil, "two", true}},
	}
	RestoreNonColumnFields(hdr) // must not panic
	if len(hdr.Tags) != 2 || hdr.Tags[0] != "ok" || hdr.Tags[1] != "two" {
		t.Fatalf("expected [ok two], got %v", hdr.Tags)
	}
	if _, stillThere := hdr.Extras["tags"]; stillThere {
		t.Error("tags should be deleted from Extras after restore")
	}
}
