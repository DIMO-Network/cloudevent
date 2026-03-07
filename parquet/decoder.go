package parquet

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/DIMO-Network/cloudevent/ce"
	pq "github.com/parquet-go/parquet-go"
)

// Reader wraps an opened parquet file so that multiple row seeks reuse the
// parsed file metadata (footer) instead of re-reading it from the underlying
// io.ReaderAt on every call.
type Reader struct {
	reader *pq.GenericReader[ParquetRow]
}

// OpenReader opens a parquet file and returns a Reader for repeated row access.
// Call Close when done to release resources.
func OpenReader(r io.ReaderAt, size int64) (*Reader, error) {
	f, err := pq.OpenFile(r, size)
	if err != nil {
		return nil, fmt.Errorf("opening parquet file: %w", err)
	}
	return &Reader{reader: pq.NewGenericReader[ParquetRow](f)}, nil
}

// NumRows returns the total number of rows in the parquet file.
func (pr *Reader) NumRows() int64 {
	return pr.reader.NumRows()
}

// SeekToRow retrieves a single row by index.
// The rowIndex must be in the range [0, NumRows()).
func (pr *Reader) SeekToRow(rowIndex int64) (cloudevent.RawEvent, error) {
	if rowIndex < 0 || rowIndex >= pr.reader.NumRows() {
		return cloudevent.RawEvent{}, fmt.Errorf("row index %d out of range [0, %d)", rowIndex, pr.reader.NumRows())
	}

	pr.reader.SeekToRow(rowIndex)

	var rows [1]ParquetRow
	_, err := pr.reader.Read(rows[:])
	if err != nil && err != io.EOF {
		return cloudevent.RawEvent{}, fmt.Errorf("reading parquet row: %w", err)
	}

	return convertRow(&rows[0])
}

// Close releases the underlying parquet reader resources.
func (pr *Reader) Close() error {
	return pr.reader.Close()
}

// Decode reads a parquet file from r and returns the decoded CloudEvents.
// The size parameter must be the total size of the parquet data in bytes.
func Decode(r io.ReaderAt, size int64) ([]cloudevent.RawEvent, error) {
	pr, err := OpenReader(r, size)
	if err != nil {
		return nil, err
	}
	defer pr.Close()

	numRows := pr.NumRows()
	if numRows == 0 {
		return nil, nil
	}

	rows := make([]ParquetRow, numRows)
	_, err = pr.reader.Read(rows)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("reading parquet rows: %w", err)
	}

	events := make([]cloudevent.RawEvent, len(rows))
	for i := range rows {
		event, err := convertRow(&rows[i])
		if err != nil {
			return nil, fmt.Errorf("converting row at index %d: %w", i, err)
		}
		events[i] = event
	}

	return events, nil
}

// SeekToRow retrieves a single row by index without reading the entire file.
// The rowIndex must be in the range [0, numRows).
// For multiple seeks on the same file, use OpenReader instead to avoid
// re-parsing the parquet footer on each call.
func SeekToRow(r io.ReaderAt, size int64, rowIndex int64) (cloudevent.RawEvent, error) {
	pr, err := OpenReader(r, size)
	if err != nil {
		return cloudevent.RawEvent{}, err
	}
	defer pr.Close()

	return pr.SeekToRow(rowIndex)
}

// convertRow transforms a single ParquetRow back into a RawEvent.
// This is the inverse of convertEvent in encoder.go.
func convertRow(row *ParquetRow) (cloudevent.RawEvent, error) {
	var extras map[string]any
	if row.Extras != "" && row.Extras != "{}" {
		if err := json.Unmarshal([]byte(row.Extras), &extras); err != nil {
			return cloudevent.RawEvent{}, fmt.Errorf("unmarshaling extras: %w", err)
		}
	}

	header := cloudevent.CloudEventHeader{
		Type:            row.Type,
		Source:          row.Source,
		Subject:         row.Subject,
		ID:              row.ID,
		Time:            row.Time,
		DataContentType: row.DataContentType,
		DataVersion:     row.DataVersion,
		Producer:        row.Producer,
		Extras:          extras,
	}

	cloudevent.RestoreNonColumnFields(&header)

	event := cloudevent.RawEvent{
		CloudEventHeader: header,
	}

	if len(row.DataBase64) > 0 {
		event.DataBase64 = string(row.DataBase64)
	} else if row.Data != nil {
		event.Data = json.RawMessage(*row.Data)
	}

	return event, nil
}

// IsParquetRef returns true if the index key references a row within a parquet
// file, indicated by the presence of a '#' separator.
func IsParquetRef(indexKey string) bool {
	return strings.Contains(indexKey, "#")
}

// ParseIndexKey splits a parquet index key in the format "objectKey#rowOffset"
// into its component parts.
func ParseIndexKey(indexKey string) (objectKey string, rowOffset int64, err error) {
	idx := strings.LastIndex(indexKey, "#")
	if idx < 0 {
		return "", 0, fmt.Errorf("index key %q does not contain '#'", indexKey)
	}

	objectKey = indexKey[:idx]
	rowOffset, err = strconv.ParseInt(indexKey[idx+1:], 10, 64)
	if err != nil {
		return "", 0, fmt.Errorf("parsing row offset from index key %q: %w", indexKey, err)
	}

	return objectKey, rowOffset, nil
}
