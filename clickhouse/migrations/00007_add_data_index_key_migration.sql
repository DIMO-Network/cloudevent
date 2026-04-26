-- +goose Up
-- +goose StatementBegin
ALTER TABLE cloud_event ADD COLUMN data_index_key String COMMENT 'Key of the external object holding the data payload, when stored out-of-band; empty when the data is inline.' AFTER index_key;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE cloud_event DROP COLUMN data_index_key;
-- +goose StatementEnd
