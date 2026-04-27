-- +goose Up
-- +goose StatementBegin
ALTER TABLE cloud_event ADD COLUMN data_index_key String COMMENT 'Key of the object holding the data payload. Empty for small events that are entirely stored in the object referenced by index_key.' AFTER index_key;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE cloud_event DROP COLUMN data_index_key;
-- +goose StatementEnd
