package parquet

import "time"

type ParquetRow struct {
	Subject         string    `parquet:"subject"`
	Time            time.Time `parquet:"time,timestamp(millisecond)"`
	Type            string    `parquet:"type"`
	ID              string    `parquet:"id"`
	Source          string    `parquet:"source"`
	Producer        string    `parquet:"producer"`
	DataContentType string    `parquet:"data_content_type"`
	DataVersion     string    `parquet:"data_version"`
	Extras          string    `parquet:"extras"`
	Data            *string   `parquet:"data,optional"`
	DataBase64      []byte    `parquet:"data_base64,optional"`
}
