-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS cloud_event (
    subject String COMMENT 'identifying the subject of the event within the context of the event producer',
    event_time DateTime64(3, 'UTC') COMMENT 'Time at which the event occurred.',
    event_type String COMMENT 'event type for this object',
    id String COMMENT 'Identifier for the event.',
    source String COMMENT 'Entity that is responsible for providing this cloud event',
    producer String COMMENT 'specific instance, process or device that creates the data structure describing the cloud event.',
    data_content_type String COMMENT 'Type of data of this object.',
    data_version String COMMENT 'Version of the data stored for this cloud event.',
	extras String COMMENT 'Extra metadata for the cloud event',
    index_key String COMMENT 'Key of the backing object for this cloud event'
) ENGINE = ReplacingMergeTree()
ORDER BY
    (subject, event_time, event_type, source, id) SETTINGS index_granularity = 8192;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS cloud_event;
-- +goose StatementEnd