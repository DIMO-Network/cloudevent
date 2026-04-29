-- +goose Up
-- +goose StatementBegin
ALTER TABLE cloud_event ADD COLUMN IF NOT EXISTS voids_id String DEFAULT '' COMMENT 'For dimo.tombstone events, the id of the event being tombstoned. Empty for all other event types.' AFTER data_index_key;
-- +goose StatementEnd
-- +goose StatementBegin
-- Make it cheap to find these events. This may also help with some attestation filtering.
-- Need to materialize this on your own.
ALTER TABLE cloud_event ADD INDEX IF NOT EXISTS idx_event_type event_type TYPE set(0) GRANULARITY 1;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE cloud_event DROP INDEX IF EXISTS idx_event_type;
-- +goose StatementEnd
-- +goose StatementBegin
ALTER TABLE cloud_event DROP COLUMN IF EXISTS voids_id;
-- +goose StatementEnd
