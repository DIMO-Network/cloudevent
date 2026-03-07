-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS name_index_tmp AS name_index ENGINE = MergeTree()
ORDER BY (timestamp, primary_filler, data_type, subject, secondary_filler);
-- +goose StatementEnd

-- +goose StatementBegin
INSERT INTO name_index_tmp SELECT * FROM name_index;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS name_index;
-- +goose StatementEnd

-- +goose StatementBegin
RENAME TABLE name_index_tmp TO name_index;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS name_index_tmp AS name_index ENGINE = MergeTree()
ORDER BY file_name;
-- +goose StatementEnd

-- +goose StatementBegin
INSERT INTO name_index_tmp SELECT * FROM name_index;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS name_index;
-- +goose StatementEnd

-- +goose StatementBegin
RENAME TABLE name_index_tmp TO name_index;
-- +goose StatementEnd
