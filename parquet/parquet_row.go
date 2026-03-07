package parquet

import "time"

// ParquetRow is the row layout for storing CloudEvents in Parquet files.
// Field order matches the JSON Event Format: specversion, type, source, subject, id, time,
// datacontenttype, then extension attributes, then data/data_base64.
// See https://github.com/cloudevents/spec/blob/main/cloudevents/formats/json-format.md
type ParquetRow struct {
	SpecVersion     string    `parquet:"specversion"`
	Type            string    `parquet:"type"`
	Source          string    `parquet:"source"`
	Subject         string    `parquet:"subject"`
	ID              string    `parquet:"id"`
	Time            time.Time `parquet:"time,timestamp(millisecond)"`
	DataContentType string    `parquet:"data_content_type"`
	DataVersion     string    `parquet:"data_version"`
	Producer        string    `parquet:"producer"`
	Extras          string    `parquet:"extras"`
	Data            *string   `parquet:"data,optional"`
	DataBase64      []byte    `parquet:"data_base64,optional"`
}
