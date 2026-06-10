package parquet

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"

	"github.com/DIMO-Network/cloudevent"
	pq "github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/snappy"
	"github.com/parquet-go/parquet-go/compress/zstd"
)

// subjectBloomFilterBitsPerValue sizes the subject bloom filter at ~1% false
// positives, which is what query engines need for row-group skipping.
const subjectBloomFilterBitsPerValue = 10

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

	// SortRows orders rows by (subject, time) before writing and declares the
	// sorting columns in the file metadata, enabling row-group pruning on
	// subject for engines that read column statistics.
	SortRows bool

	// ZstdCompression replaces the default Snappy codec with Zstd.
	ZstdCompression bool

	// SubjectBloomFilter writes a split-block bloom filter for the subject
	// column so point lookups can skip row groups without reading pages.
	SubjectBloomFilter bool
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

// WithSortedRows sorts rows by (subject, time) before writing. The returned
// index keys still map each input event index to its (post-sort) row offset.
func WithSortedRows() Option {
	return func(c *EncoderConfig) {
		c.SortRows = true
	}
}

// WithZstdCompression compresses pages with Zstd instead of Snappy.
func WithZstdCompression() Option {
	return func(c *EncoderConfig) {
		c.ZstdCompression = true
	}
}

// WithSubjectBloomFilter adds a bloom filter on the subject column.
func WithSubjectBloomFilter() Option {
	return func(c *EncoderConfig) {
		c.SubjectBloomFilter = true
	}
}

// Encode writes events as compressed Parquet to w (Snappy by default, Zstd
// with WithZstdCompression). Each event is assigned an index key in the
// format "objectKey#rowOffset". The returned map contains the event index to
// index key mapping; with WithSortedRows the offset reflects the row's
// position after sorting.
//
// Each StoredEvent's DataIndexKey is written into the row so that a reader
// holding a bundle alone can locate any externally-stored payload without
// consulting ClickHouse. Leave DataIndexKey empty when the payload is inline
// in Data / DataBase64.
func Encode(w io.Writer, events []cloudevent.StoredEvent, objectKey string, opts ...Option) (map[int]string, error) {
	cfg := EncoderConfig{
		MaxRowsPerRowGroup: 10000,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	writerOpts := []pq.WriterOption{}
	if cfg.ZstdCompression {
		writerOpts = append(writerOpts, pq.Compression(&zstd.Codec{}))
	} else {
		writerOpts = append(writerOpts, pq.Compression(&snappy.Codec{}))
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
	if cfg.SortRows {
		// SortingWriterConfig only stamps the sorting-columns metadata into
		// row groups — GenericWriter never reorders rows. The actual sort is
		// the sort.SliceStable over `order` below; both are required.
		writerOpts = append(writerOpts, pq.SortingWriterConfig(
			pq.SortingColumns(pq.Ascending("subject"), pq.Ascending("time")),
		))
	}
	if cfg.SubjectBloomFilter {
		writerOpts = append(writerOpts, pq.BloomFilters(
			pq.SplitBlockFilter(subjectBloomFilterBitsPerValue, "subject"),
		))
	}

	writer := pq.NewGenericWriter[ParquetRow](w, writerOpts...)

	// order[rowOffset] = original event index. Identity unless sorting.
	order := make([]int, len(events))
	for i := range order {
		order[i] = i
	}
	if cfg.SortRows {
		sort.SliceStable(order, func(a, b int) bool {
			ea, eb := &events[order[a]], &events[order[b]]
			if ea.Subject != eb.Subject {
				return ea.Subject < eb.Subject
			}
			return ea.Time.Before(eb.Time)
		})
	}

	rows := make([]ParquetRow, 0, len(events))
	indexKeys := make(map[int]string, len(events))

	for offset, eventIdx := range order {
		indexKeys[eventIdx] = objectKey + "#" + strconv.Itoa(offset)

		row, err := convertEvent(&events[eventIdx])
		if err != nil {
			return nil, fmt.Errorf("converting event at index %d: %w", eventIdx, err)
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

// convertEvent transforms a single StoredEvent into a ParquetRow.
func convertEvent(event *cloudevent.StoredEvent) (ParquetRow, error) {
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
		DataIndexKey:    event.DataIndexKey,
	}

	if event.DataBase64 != "" {
		row.DataBase64 = []byte(event.DataBase64)
	} else if len(event.Data) > 0 {
		s := string(event.Data)
		row.Data = &s
	}

	return row, nil
}
