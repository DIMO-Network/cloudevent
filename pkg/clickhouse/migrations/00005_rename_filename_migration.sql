-- +goose Up
-- +goose StatementBegin
ALTER TABLE name_index RENAME COLUMN file_name TO index_key;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE name_index MODIFY COLUMN index_key String AFTER optional;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE name_index RENAME COLUMN index_key TO file_name;
-- +goose StatementEnd

-- +goose StatementBegin
ALTER TABLE name_index MODIFY COLUMN file_name String BEFORE optional;
-- +goose StatementEnd
