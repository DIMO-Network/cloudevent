-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS name_index
(
    timestamp DateTime('UTC') COMMENT 'Combined date and time in UTC with millisecond precision.',
    primary_filler FixedString(2) COMMENT 'Primary filler, a constant string of length 2.',
    data_type FixedString(10) COMMENT 'Data type left-padded with zeros or truncated to 10 characters.',
    subject FixedString(40) COMMENT 'Hexadecimal representation of the devices address or tokenId 40 characters.',
    secondary_filler FixedString(2) COMMENT 'Secondary filler, a constant string of length 2.',
	file_name String COMMENT 'Name of the file that the data was collected from.',
)
ENGINE = MergeTree()
ORDER BY (file_name);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE name_index;
-- +goose StatementEnd


