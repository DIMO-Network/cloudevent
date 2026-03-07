-- +goose Up
-- +goose StatementBegin
ALTER TABLE name_index ADD COLUMN IF NOT EXISTS optional String COMMENT 'Optional metadata';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE name_index DROP COLUMN IF EXISTS optional;
-- +goose StatementEnd
