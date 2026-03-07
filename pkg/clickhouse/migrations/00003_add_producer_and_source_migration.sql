-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS name_index_tmp
(
    subject FixedString(64) COMMENT 'represents the DID of the subject of the event ChainId+ContractAddress+TokenId.',
    timestamp DateTime('UTC') COMMENT 'Combined date and time in UTC with millisecond precision.',
    primary_filler FixedString(2) COMMENT 'Primary filler, a constant string of length 2.',
	source FixedString(40) COMMENT 'represents the address of the source of the event.',
    data_type FixedString(20) COMMENT 'Data type left-padded with zeros or truncated to 20 characters.',
    secondary_filler FixedString(2) COMMENT 'Secondary filler, a constant string of length 2.',
	producer FixedString(64) COMMENT 'represents the DID of the producer of the event ChainId+ContractAddress+TokenId.',
	file_name String COMMENT 'Name of the file that the data was collected from.',
)
ENGINE = MergeTree()
ORDER BY (subject, timestamp, primary_filler, source, data_type, secondary_filler, producer);
-- +goose StatementEnd

-- +goose StatementBegin
INSERT INTO name_index_tmp (subject, timestamp, primary_filler, source, data_type, secondary_filler, producer, file_name)
SELECT 
    subject, 
    timestamp,
    primary_filler,
    '' AS source, 
    data_type,
    secondary_filler,
    '' AS producer,
    file_name
FROM name_index;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS name_index;
-- +goose StatementEnd

-- +goose StatementBegin
RENAME TABLE name_index_tmp TO name_index;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS name_index_tmp
(
    timestamp DateTime('UTC') COMMENT 'Combined date and time in UTC with millisecond precision.',
    primary_filler FixedString(2) COMMENT 'Primary filler, a constant string of length 2.',
    data_type FixedString(10) COMMENT 'Data type left-padded with zeros or truncated to 10 characters.',
    subject FixedString(40) COMMENT 'Hexadecimal representation of the devices address or tokenId 40 characters.',
    secondary_filler FixedString(2) COMMENT 'Secondary filler, a constant string of length 2.',
	file_name String COMMENT 'Name of the file that the data was collected from.',
)
ENGINE = MergeTree()
ORDER BY (timestamp, primary_filler, data_type, subject, secondary_filler);
-- +goose StatementEnd

-- +goose StatementBegin
INSERT INTO name_index_tmp (timestamp, primary_filler, data_type, subject, secondary_filler, file_name)
SELECT 
    timestamp,
    left(primary_filler, 2),
    left(data_type, 10),
    left(subject, 40),
    left(secondary_filler, 2),
    file_name
FROM name_index;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS name_index;
-- +goose StatementEnd

-- +goose StatementBegin
RENAME TABLE name_index_tmp TO name_index;
-- +goose StatementEnd
