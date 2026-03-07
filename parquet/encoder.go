package parquet

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"

	"github.com/DIMO-Network/cloudevent/ce"
	pq "github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/snappy"
)

// EncoderConfig holds tunable parameters for the parquet encoder.
type EncoderConfig struct {
	// MaxRowsPerRowGroup controls how many rows are written per row group.
	// Zero means use the parquet-go default.
	MaxRowsPerRowGroup int64

	// PageBufferSize controls the page buffer size in bytes.
	// Zero means use the parquet-go default.
	PageBufferSize int

	// WriteBufferSize controls the write buffer size in bytes.
	// Zero means use the parquet-go default.
	WriteBufferSize int
}

// Option is a functional option for configuring the parquet encoder.
type Option func(*EncoderConfig)

// WithMaxRowsPerRowGroup sets the maximum number of rows per row group.
func WithMaxRowsPerRowGroup(n int64) Option {
	return func(c *EncoderConfig) {
		c.MaxRowsPerRowGroup = n
	}
}

// WithPageBufferSize sets the page buffer size in bytes.
func WithPageBufferSize(n int) Option {
	return func(c *EncoderConfig) {
		c.PageBufferSize = n
	}
}

// WithWriteBufferSize sets the write buffer size in bytes.
func WithWriteBufferSize(n int) Option {
	return func(c *EncoderConfig) {
		c.WriteBufferSize = n
	}
}

// Encode writes events as Snappy-compressed Parquet to w. Each event is assigned
// an index key in the format "objectKey#rowOffset". The returned map contains
// the event index to index key mapping.
func Encode(w io.Writer, events []cloudevent.RawEvent, objectKey string, opts ...Option) (map[int]string, error) {
	cfg := EncoderConfig{
		MaxRowsPerRowGroup: 10000,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	writerOpts := []pq.WriterOption{
		pq.Compression(&snappy.Codec{}),
	}
	if cfg.MaxRowsPerRowGroup > 0 {
		writerOpts = append(writerOpts, pq.MaxRowsPerRowGroup(cfg.MaxRowsPerRowGroup))
	}
	if cfg.PageBufferSize > 0 {
		writerOpts = append(writerOpts, pq.PageBufferSize(cfg.PageBufferSize))
	}
	if cfg.WriteBufferSize > 0 {
		writerOpts = append(writerOpts, pq.WriteBufferSize(cfg.WriteBufferSize))
	}

	writer := pq.NewGenericWriter[ParquetRow](w, writerOpts...)

	rows := make([]ParquetRow, 0, len(events))
	indexKeys := make(map[int]string, len(events))

	for i := range events {
		indexKey := objectKey + "#" + strconv.Itoa(i)
		indexKeys[i] = indexKey

		row, err := convertEvent(&events[i])
		if err != nil {
			return nil, fmt.Errorf("converting event at index %d: %w", i, err)
		}
		rows = append(rows, row)
	}

	if _, err := writer.Write(rows); err != nil {
		return nil, fmt.Errorf("writing parquet rows: %w", err)
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("closing parquet writer: %w", err)
	}

	return indexKeys, nil
}

// convertEvent transforms a single CloudEvent into a ParquetRow.
func convertEvent(event *cloudevent.RawEvent) (ParquetRow, error) {
	extras := cloudevent.AddNonColumnFieldsToExtras(&event.CloudEventHeader)

	var extrasJSON []byte
	if extras == nil {
		extrasJSON = []byte("{}")
	} else {
		var err error
		extrasJSON, err = json.Marshal(extras)
		if err != nil {
			return ParquetRow{}, fmt.Errorf("marshaling extras: %w", err)
		}
	}

	row := ParquetRow{
		Type:            event.Type,
		Source:          event.Source,
		Subject:         event.Subject,
		ID:              event.ID,
		Time:            event.Time,
		DataContentType: event.DataContentType,
		DataVersion:     event.DataVersion,
		Producer:        event.Producer,
		Extras:          string(extrasJSON),
	}

	if event.DataBase64 != "" {
		row.DataBase64 = []byte(event.DataBase64)
	} else if len(event.Data) > 0 {
		s := string(event.Data)
		row.Data = &s
	}

	return row, nil
}
