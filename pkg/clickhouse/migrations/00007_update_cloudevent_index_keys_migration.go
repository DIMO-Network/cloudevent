package migrations

import (
	"context"
	"database/sql"
	"runtime"

	"github.com/pressly/goose/v3"
)

func init() {
	_, filename, _, _ := runtime.Caller(0)
	registerFunc := func() {
		goose.AddNamedMigrationContext(filename, upUpdateCloudeventIndexKeys, downUpdateCloudeventIndexKeys)
	}
	registerFuncs = append(registerFuncs, registerFunc)
}

func upUpdateCloudeventIndexKeys(ctx context.Context, tx *sql.Tx) error {
	// This code is executed when the migration is applied.
	upStatements := []string{
		`CREATE TABLE file_index.cloud_event_new AS file_index.cloud_event
		ENGINE = ReplacingMergeTree()
		ORDER BY (subject, event_time, event_type, source, id)
		SETTINGS index_granularity = 8192`,
		`RENAME TABLE file_index.cloud_event TO file_index.cloud_event_backup`,
		`RENAME TABLE file_index.cloud_event_new TO file_index.cloud_event`,
	}
	for _, upStatement := range upStatements {
		_, err := tx.ExecContext(ctx, upStatement)
		if err != nil {
			return err
		}
	}
	return nil
}

func downUpdateCloudeventIndexKeys(ctx context.Context, tx *sql.Tx) error {
	// This code is executed when the migration is rolled back.
	downStatements := []string{
		`CREATE TABLE file_index.cloud_event_original AS file_index.cloud_event
		ENGINE = MergeTree()
		ORDER BY (subject, event_time, event_type)
		SETTINGS index_granularity = 8192`,
		`RENAME TABLE file_index.cloud_event TO file_index.cloud_event_temp`,
		`RENAME TABLE file_index.cloud_event_original TO file_index.cloud_event`,
		`DROP TABLE IF EXISTS file_index.cloud_event_temp`,
		`DROP TABLE IF EXISTS file_index.cloud_event_backup`,
	}
	for _, downStatement := range downStatements {
		_, err := tx.ExecContext(ctx, downStatement)
		if err != nil {
			return err
		}
	}
	return nil
}
