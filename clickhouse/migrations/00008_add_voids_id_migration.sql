-- +goose Up
-- +goose StatementBegin
ALTER TABLE cloud_event ADD COLUMN voids_id String DEFAULT '' COMMENT 'For dimo.tombstone events, the id of the attestation being tombstoned. Empty for all other event types.' AFTER data_index_key;
-- +goose StatementEnd
-- +goose StatementBegin
ALTER TABLE cloud_event ADD INDEX idx_event_type event_type TYPE set(0) GRANULARITY 1;
-- +goose StatementEnd
-- +goose StatementBegin
ALTER TABLE cloud_event MATERIALIZE INDEX idx_event_type;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE cloud_event DROP INDEX idx_event_type;
-- +goose StatementEnd
-- +goose StatementBegin
ALTER TABLE cloud_event DROP COLUMN voids_id;
-- +goose StatementEnd
