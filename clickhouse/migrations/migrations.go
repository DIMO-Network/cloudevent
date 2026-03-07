package migrations

import (
	"context"
	"database/sql"
	"embed"

	"github.com/DIMO-Network/clickhouse-infra/pkg/migrate"
)

// BaseFS is the embed.FS for the migrations.
//
//go:embed *.sql
var BaseFS embed.FS

// RunGoose runs the goose command with the provided arguments.
// args should be the command and the arguments to pass to goose.
// eg RunGoose(ctx, []string{"up", "-v"}, db).
func RunGoose(ctx context.Context, gooseArgs []string, db *sql.DB) error {
	return migrate.RunGoose(ctx, gooseArgs, BaseFS, db)
}
